#!/bin/bash
set -euo pipefail
###############################################################################
# Create consumer projects — one per IdC group, with group members added.
#
# Usage: ./06-create-consumer-projects.sh
#
# For each group in IDC_GROUPS + IDC_GROUP_GRANTS, this script:
#   1. Creates a DataZone project named "<group>-consumer"
#   2. Waits for environments to provision
#   3. Creates a per-group Redshift user with scoped table grants
#   4. Grants the consumer's DZ user role access to the admin secret
#   5. Adds each group member as PROJECT_CONTRIBUTOR
#
# NOTE: No permanent Redshift connection is created in consumer projects.
# The grant automation Lambda (05-deploy-grant-automation.sh) creates
# temporary readonly connections on-demand when subscriptions are approved,
# and cleans them up after the grant completes.
#
# Required: CLUSTER_ID, DOMAIN_ID, REDSHIFT_HOST, REDSHIFT_DB, REDSHIFT_PORT,
#           REDSHIFT_USER (admin), IDC_GROUPS, IDC_GROUP_GRANTS (from .env)
###############################################################################

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  echo "Usage: $0"
  echo ""
  echo "Creates one consumer project per IdC group with scoped Redshift connections."
  echo "Group members are added as PROJECT_CONTRIBUTOR."
  echo ""
  echo "Configure in .env:"
  echo "  IDC_GROUPS='DataAnalysts:user1@example.com,DataEngineers:user2@example.com'"
  echo "  IDC_GROUP_GRANTS='DataAnalysts:SELECT:employees,DataEngineers:ALL:departments'"
  exit 0
fi

source "$(dirname "$0")/config.sh"

: "${IDC_GROUPS:?'IDC_GROUPS is required'}"
: "${IDC_GROUP_GRANTS:?'IDC_GROUP_GRANTS is required for per-group Redshift users'}"

# Discover IdC instance
IDC_INSTANCE_ARN=$(aws sso-admin list-instances --query 'Instances[0].InstanceArn' --output text --region "$REGION" 2>/dev/null || echo "")
IDENTITY_STORE_ID=$(aws sso-admin list-instances --query 'Instances[0].IdentityStoreId' --output text --region "$REGION" 2>/dev/null || echo "")

# Find the SQL analytics profile for project creation
PROFILE_ID=$(aws datazone list-project-profiles \
  --domain-identifier "$DOMAIN_ID" --region "$REGION" --output json 2>/dev/null | python3 -c "
import json, sys
for p in json.load(sys.stdin).get('items', []):
    if p['name'] == 'SQL analytics':
        print(p['id']); break
" 2>/dev/null || echo "")

if [ -z "$PROFILE_ID" ]; then
  echo "❌ Could not find 'SQL analytics' project profile"
  exit 1
fi

###############################################################################
# Helper: execute Redshift SQL and wait for completion
###############################################################################
exec_sql() {
  local sql="$1"
  local stmt_id
  stmt_id=$(aws redshift-data execute-statement \
    --cluster-identifier "$CLUSTER_ID" --database "$REDSHIFT_DB" --db-user "$REDSHIFT_USER" \
    --sql "$sql" --region "$REGION" --query 'Id' --output text 2>/dev/null || echo "")
  if [ -z "$stmt_id" ]; then
    echo "    ⚠️  Failed to submit SQL"
    return 1
  fi
  for _i in $(seq 1 15); do
    local qstatus
    qstatus=$(aws redshift-data describe-statement --id "$stmt_id" --region "$REGION" \
      --query 'Status' --output text 2>/dev/null || echo "")
    [ "$qstatus" = "FINISHED" ] && return 0
    if [ "$qstatus" = "FAILED" ]; then
      local err
      err=$(aws redshift-data describe-statement --id "$stmt_id" --region "$REGION" \
        --query 'Error' --output text 2>/dev/null || echo "unknown")
      if echo "$err" | grep -qi "already exists"; then
        echo "    (already exists)"
        return 0
      fi
      echo "    ⚠️  $err"
      return 1
    fi
    sleep 2
  done
  echo "    ⚠️  Timed out"
  return 1
}

###############################################################################
# Parse IDC_GROUP_GRANTS into parallel arrays (bash 3.x compat)
# Format: "Group:LEVEL:table1;table2,Group2:LEVEL:table3"
###############################################################################
GRANT_GROUP_NAMES=()
GRANT_LEVELS=()
GRANT_TABLES_LIST=()

IFS=',' read -ra _GRANT_ENTRIES <<< "$IDC_GROUP_GRANTS"
for _ge in "${_GRANT_ENTRIES[@]}"; do
  _gname=$(echo "$_ge" | cut -d: -f1)
  _glevel=$(echo "$_ge" | cut -d: -f2)
  _gtables=$(echo "$_ge" | cut -d: -f3)
  GRANT_GROUP_NAMES+=("$_gname")
  GRANT_LEVELS+=("$_glevel")
  GRANT_TABLES_LIST+=("$_gtables")
done

# Generate a random password for per-group users
generate_password() {
  python3 -c "
import secrets, string
chars = string.ascii_letters + string.digits + '!@#%^&*'
pwd = secrets.choice(string.ascii_uppercase)
pwd += secrets.choice(string.ascii_lowercase)
pwd += secrets.choice(string.digits)
pwd += secrets.choice('!@#%^&*')
pwd += ''.join(secrets.choice(chars) for _ in range(12))
print(pwd)
"
}

###############################################################################
# Loop over each group in IDC_GROUPS
###############################################################################
IFS=',' read -ra GROUP_ENTRIES <<< "$IDC_GROUPS"
for entry in "${GROUP_ENTRIES[@]}"; do
  GROUP_NAME="${entry%%:*}"
  USERS_PART="${entry#*:}"
  PROJECT_NAME="${GROUP_NAME}-consumer"
  GROUP_LOWER=$(echo "$GROUP_NAME" | tr '[:upper:]' '[:lower:]')
  DB_USER="${GROUP_LOWER}_user"

  log "Setting up consumer project: ${PROJECT_NAME}"

  ###########################################################################
  # Find grants for this group
  ###########################################################################
  GRANT_LEVEL=""
  GRANT_TABLES=""
  for _gi in "${!GRANT_GROUP_NAMES[@]}"; do
    if [ "${GRANT_GROUP_NAMES[$_gi]}" = "$GROUP_NAME" ]; then
      GRANT_LEVEL="${GRANT_LEVELS[$_gi]}"
      GRANT_TABLES="${GRANT_TABLES_LIST[$_gi]}"
      break
    fi
  done

  if [ -z "$GRANT_LEVEL" ]; then
    echo "  ⚠️  No grants found for $GROUP_NAME in IDC_GROUP_GRANTS — skipping"
    continue
  fi

  echo "  Grants: ${GRANT_LEVEL} on ${GRANT_TABLES}"

  ###########################################################################
  # Create per-group Redshift user with scoped grants
  ###########################################################################
  DB_PASSWORD=$(generate_password)

  echo "  Creating Redshift user: $DB_USER"
  exec_sql "CREATE USER ${DB_USER} PASSWORD '${DB_PASSWORD}'"
  echo "  Syncing password for $DB_USER"
  exec_sql "ALTER USER ${DB_USER} PASSWORD '${DB_PASSWORD}'"

  echo "  Granting USAGE on schema public"
  exec_sql "GRANT USAGE ON SCHEMA public TO ${DB_USER}"

  IFS=';' read -ra _TABLES <<< "$GRANT_TABLES"
  for _tbl in "${_TABLES[@]}"; do
    if [ "$_tbl" = "*" ]; then
      echo "  Granting ${GRANT_LEVEL} on ALL TABLES"
      exec_sql "GRANT ${GRANT_LEVEL} ON ALL TABLES IN SCHEMA public TO ${DB_USER}"
    else
      echo "  Granting ${GRANT_LEVEL} on ${_tbl}"
      exec_sql "GRANT ${GRANT_LEVEL} ON TABLE public.${_tbl} TO ${DB_USER}"
    fi
  done

  echo "  ✅ Redshift user $DB_USER created with scoped grants"

  ###########################################################################
  # Create project (or find existing)
  ###########################################################################
  EXISTING=$(aws datazone list-projects --domain-identifier "$DOMAIN_ID" \
    --region "$REGION" --output json 2>/dev/null | python3 -c "
import json, sys
for p in json.load(sys.stdin).get('items', []):
    if p['name'] == '${PROJECT_NAME}':
        print(p['id']); break
" 2>/dev/null || echo "")

  if [ -n "$EXISTING" ]; then
    PROJECT_ID="$EXISTING"
    echo "  Project already exists: $PROJECT_ID"
  else
    PROJECT_ID=$(aws datazone create-project \
      --domain-identifier "$DOMAIN_ID" \
      --name "$PROJECT_NAME" \
      --description "Consumer project for ${GROUP_NAME} group" \
      --project-profile-id "$PROFILE_ID" \
      --region "$REGION" --query 'id' --output text)
    echo "  Created project: $PROJECT_ID"
  fi

  ###########################################################################
  # Wait for environments
  ###########################################################################
  echo "  Waiting for environments..."
  TOOLING_ENV_ID=""
  for attempt in $(seq 1 60); do
    ENVS_JSON=$(aws datazone list-environments \
      --domain-identifier "$DOMAIN_ID" --project-identifier "$PROJECT_ID" \
      --region "$REGION" --output json 2>/dev/null || echo '{"items":[]}')

    ENV_COUNT=$(echo "$ENVS_JSON" | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('items',[])))")
    ACTIVE_COUNT=$(echo "$ENVS_JSON" | python3 -c "import json,sys; print(sum(1 for e in json.load(sys.stdin).get('items',[]) if e.get('status')=='ACTIVE'))")

    if [ "$ENV_COUNT" -ge 2 ] && [ "$ACTIVE_COUNT" -ge 2 ]; then
      echo "  ✅ $ACTIVE_COUNT environments active"
      break
    fi
    echo "  ⏳ $ACTIVE_COUNT/$ENV_COUNT active (attempt $attempt/60)..."
    sleep 10
  done

  TOOLING_ENV_ID=$(echo "$ENVS_JSON" | python3 -c "
import json, sys
for e in json.load(sys.stdin).get('items', []):
    if 'Tooling' in e.get('name', '') or 'tooling' in e.get('name', '').lower():
        print(e['id']); break
" 2>/dev/null || echo "")

  echo "  Tooling env: $TOOLING_ENV_ID"

  ###########################################################################
  # Grant consumer's DZ user role access to admin secret
  # (needed for Lambda to create subscription target with admin credentials)
  ###########################################################################
  CONSUMER_DZ_ROLE="datazone_usr_role_${PROJECT_ID}_${TOOLING_ENV_ID}"
  ADMIN_SECRET_NAME=$(aws secretsmanager list-secrets \
    --filters "Key=tag-key,Values=AmazonDataZoneProject" "Key=tag-value,Values=${ADMIN_PROJECT_ID}" \
    --region "$REGION" --output json 2>/dev/null | python3 -c "
import json, sys
for s in json.load(sys.stdin).get('SecretList', []):
    tags = {t['Key']: t['Value'] for t in s.get('Tags', [])}
    if tags.get('AmazonDataZoneProject') == '${ADMIN_PROJECT_ID}' and 'smus-redshift' in s.get('Name', ''):
        print(s['Name']); break
" 2>/dev/null || echo "")

  if [ -n "$ADMIN_SECRET_NAME" ]; then
    CURRENT_POLICY=$(aws secretsmanager get-resource-policy --secret-id "$ADMIN_SECRET_NAME" \
      --region "$REGION" --query 'ResourcePolicy' --output text 2>/dev/null || echo "")
    if [ -n "$CURRENT_POLICY" ] && [ "$CURRENT_POLICY" != "None" ]; then
      UPDATED_POLICY=$(echo "$CURRENT_POLICY" | python3 -c "
import json, sys
policy = json.loads(sys.stdin.read())
role_arn = 'arn:aws:iam::${ACCOUNT_ID}:role/${CONSUMER_DZ_ROLE}'
principals = policy['Statement'][0]['Principal']['AWS']
if isinstance(principals, str):
    principals = [principals]
if role_arn not in principals:
    principals.append(role_arn)
policy['Statement'][0]['Principal']['AWS'] = principals
print(json.dumps(policy))
")
      aws secretsmanager put-resource-policy --secret-id "$ADMIN_SECRET_NAME" \
        --resource-policy "$UPDATED_POLICY" --region "$REGION" > /dev/null
      echo "  ✅ Added $CONSUMER_DZ_ROLE to admin secret policy"
    fi
  fi

  echo "  ℹ️  No permanent Redshift connection created — Lambda creates"
  echo "     temporary readonly connections on subscription acceptance"

  ###########################################################################
  # Add group members as PROJECT_CONTRIBUTOR
  ###########################################################################
  echo "  Adding members..."
  IFS=';' read -ra USERS <<< "$USERS_PART"
  for user_email in "${USERS[@]}"; do
    DZ_USER_PROFILE=$(aws datazone search-user-profiles \
      --domain-identifier "$DOMAIN_ID" --user-type SSO_USER \
      --search-text "$user_email" --region "$REGION" --output json 2>/dev/null | python3 -c "
import json, sys
for p in json.load(sys.stdin).get('items', []):
    if p.get('status') == 'ASSIGNED' or p.get('status') == 'ACTIVATED':
        print(p['id']); break
" 2>/dev/null || echo "")

    if [ -n "$DZ_USER_PROFILE" ]; then
      aws datazone create-project-membership \
        --domain-identifier "$DOMAIN_ID" --project-identifier "$PROJECT_ID" \
        --member "{\"userIdentifier\": \"$DZ_USER_PROFILE\"}" \
        --designation PROJECT_CONTRIBUTOR \
        --region "$REGION" 2>/dev/null || true
      echo "  ✅ Added $user_email"
    else
      echo "  ⚠️  No profile for $user_email — they need to log in to SMUS first"
    fi
  done

  echo "  ✅ ${PROJECT_NAME} ready"
  echo ""
done

log "🎉 ALL CONSUMER PROJECTS CREATED"
