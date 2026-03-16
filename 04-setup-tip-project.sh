#!/bin/bash
set -euo pipefail
###############################################################################
# Set up a Trusted Identity Propagation (TIP) project in SMUS with per-user
# Redshift table isolation.
#
# This script automates the full TIP setup:
#   1. Enables TIP on a project profile (SQL analytics by default)
#   2. Creates a new project using the TIP-enabled profile
#   3. Waits for environments to provision
#   4. Creates a Redshift IAM connection (no credentials — uses IdC identity)
#   5. Configures Lake Formation IdC integration
#   6. Assigns IdC groups/users to the Lake Formation IdC application
#   7. Adds IdC users as project members
#   8. Grants IAM permissions to the project's user role
#   9. Creates Redshift users for each IdC user (TIP identity pattern)
#  10. Grants per-user table-level access in Redshift
#
# KEY LEARNINGS:
#   - TIP only works for projects created AFTER enabling it in the profile.
#     Existing projects won't get TIP even if the profile is updated later.
#   - The project profile's Tooling blueprint must have
#     enableTrustedIdentityPropagationPermissions = true.
#   - TIP Redshift connections use IAM auth (no credentials/secrets needed).
#     Create with redshiftProperties but omit the credentials field entirely.
#   - When a user connects via TIP, their Redshift identity is:
#       IAMR:user-<idcUserId>@<projectId>
#     NOT the AWSIDC:<GroupName> pattern from direct IdC integration.
#   - The AWSIDC: roles (from 01-setup-idc-redshift.sh) are NOT inherited
#     by TIP users. You must grant directly to the IAMR:user-* identities.
#   - The Lakehouse data explorer tree uses the project IAM role for metadata
#     browsing, so ALL users see the same tables in the tree. Per-user
#     isolation only applies to the direct Redshift connection path
#     (dev → public), not the federated catalog path (dev@cluster → public).
#   - Lake Formation must have IdC configured (create-lake-formation-
#     identity-center-configuration) or you get "LakeFormation Identity
#     Center Configuration not configured for requested catalog".
#   - The project's user role needs broad Redshift + SSO permissions:
#     redshift:*, redshift-data:*, redshift-serverless:*, sso:*, sso-oauth:*,
#     sts:*. Missing redshift:GetClusterCredentialsWithIAM causes
#     "CredentialsProviderError".
#   - The project's user role should be added as a Lake Formation admin
#     so it can browse catalogs without permission errors.
#   - IdC groups/users must be assigned to the Lake Formation IdC app
#     (in addition to the Redshift, QEV2, and Console TIP apps).
#   - Querying via the federated catalog path (dev@redshift-cluster-1)
#     may fail with "Failed to set ClientInfo property: ApplicationName".
#     Use the direct dev database path instead.
#   - TIP does NOT support subscription/publish of data products yet.
#   - TIP requires the cluster and SMUS domain to be in the same account
#     and region.
#   - Pre-create Redshift users with PASSWORD DISABLE for IdC users who
#     haven't connected yet, so grants can be applied before first login.
#
# Usage: ./04-setup-tip-project.sh [--help]
#
# Required env vars (via .env or environment):
#   CLUSTER_ID, REDSHIFT_USER, REDSHIFT_DB, REGION, DOMAIN_ID
#
# Optional env vars:
#   TIP_PROJECT_NAME     — name for the new project (default: tip-redshift-project)
#   TIP_PROFILE_NAME     — project profile to enable TIP on (default: SQL analytics)
#   TIP_USER_GRANTS      — per-user grants: "idcUserId:LEVEL:table1;table2,..."
#   IDC_GROUPS           — reused from 06 script for group/user discovery
###############################################################################

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  echo "Usage: $0"
  echo ""
  echo "Creates a TIP-enabled SMUS project with per-user Redshift table isolation."
  echo ""
  echo "Configure via .env file or environment variables. See config.sh."
  echo ""
  echo "TIP-specific variables:"
  echo "  TIP_PROJECT_NAME   Project name (default: tip-redshift-project)"
  echo "  TIP_PROFILE_NAME   Profile to enable TIP on (default: SQL analytics)"
  echo "  TIP_USER_GRANTS    Per-user grants: 'idcUserId:LEVEL:table1;table2,...'"
  echo "                     e.g. 'd418f428-...:SELECT:departments,84f83498-...:SELECT:employees'"
  echo "  IDC_GROUPS         Group-to-user mappings (reused from 06 script)"
  exit 0
fi

source "$(dirname "$0")/config.sh"

TIP_PROJECT_NAME="${TIP_PROJECT_NAME:-tip-redshift-project}"
TIP_PROFILE_NAME="${TIP_PROFILE_NAME:-SQL analytics}"
TIP_USER_GRANTS="${TIP_USER_GRANTS:-}"
IDC_GROUPS="${IDC_GROUPS:-}"

REDSHIFT_HOST="${REDSHIFT_HOST:-${CLUSTER_ID}.UNKNOWN.${REGION}.redshift.amazonaws.com}"

###############################################################################
# Discover IdC instance
###############################################################################
log "Discovering IAM Identity Center instance"

IDC_INSTANCE_ARN=$(aws sso-admin list-instances --query 'Instances[0].InstanceArn' --output text --region "$REGION" 2>/dev/null || echo "")
IDENTITY_STORE_ID=$(aws sso-admin list-instances --query 'Instances[0].IdentityStoreId' --output text --region "$REGION" 2>/dev/null || echo "")

if [ -z "$IDC_INSTANCE_ARN" ] || [ "$IDC_INSTANCE_ARN" = "None" ]; then
  echo "  ❌ No IAM Identity Center instance found. Enable IdC first."
  exit 1
fi

echo "  Instance ARN:    $IDC_INSTANCE_ARN"
echo "  Identity Store:  $IDENTITY_STORE_ID"

###############################################################################
# Find and update project profile to enable TIP
###############################################################################
log "Enabling TIP on project profile '${TIP_PROFILE_NAME}'"

PROFILE_INFO=$(aws datazone list-project-profiles \
  --domain-identifier "$DOMAIN_ID" \
  --region "$REGION" --output json 2>/dev/null)

PROFILE_ID=$(echo "$PROFILE_INFO" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for p in data.get('items', []):
    if p['name'] == '${TIP_PROFILE_NAME}':
        print(p['id']); break
" 2>/dev/null || echo "")

if [ -z "$PROFILE_ID" ]; then
  echo "  ❌ Profile '${TIP_PROFILE_NAME}' not found"
  exit 1
fi

echo "  Profile ID: $PROFILE_ID"

# Get current profile config and update TIP parameter
PROFILE_JSON=$(aws datazone get-project-profile \
  --domain-identifier "$DOMAIN_ID" --identifier "$PROFILE_ID" \
  --region "$REGION" --output json)

# Check if TIP is already enabled
TIP_CURRENT=$(echo "$PROFILE_JSON" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for ec in data.get('environmentConfigurations', []):
    if ec.get('name') == 'Tooling':
        for p in ec.get('configurationParameters', {}).get('resolvedParameters', []):
            if p.get('name') == 'enableTrustedIdentityPropagationPermissions':
                print(p.get('value', 'false')); sys.exit(0)
print('false')
" 2>/dev/null)

if [ "$TIP_CURRENT" = "true" ]; then
  echo "  TIP already enabled"
else
  # Build updated environment configurations JSON
  UPDATED_CONFIGS=$(echo "$PROFILE_JSON" | python3 -c "
import json, sys
data = json.load(sys.stdin)
configs = data.get('environmentConfigurations', [])
for ec in configs:
    if ec.get('name') == 'Tooling':
        overrides = ec.get('configurationParameters', {}).get('parameterOverrides', [])
        # Add or update TIP parameter
        found = False
        for p in overrides:
            if p.get('name') == 'enableTrustedIdentityPropagationPermissions':
                p['value'] = 'true'
                p['isEditable'] = True
                found = True
        if not found:
            overrides.append({'name': 'enableTrustedIdentityPropagationPermissions', 'value': 'true', 'isEditable': True})
    # Clean up: only keep fields the API accepts
    ec.pop('scope', None)
    params = ec.get('configurationParameters', {})
    params.pop('resolvedParameters', None)
print(json.dumps(configs))
")

  # Write to temp file to avoid shell escaping issues
  echo "$UPDATED_CONFIGS" > /tmp/tip-profile-update.json

  aws datazone update-project-profile \
    --domain-identifier "$DOMAIN_ID" \
    --identifier "$PROFILE_ID" \
    --environment-configurations file:///tmp/tip-profile-update.json \
    --region "$REGION" --output text --query 'id' > /dev/null

  rm -f /tmp/tip-profile-update.json
  echo "  ✅ TIP enabled on profile"
fi

###############################################################################
# Create TIP-enabled project
###############################################################################
log "Creating TIP project '${TIP_PROJECT_NAME}'"

EXISTING_PROJECT=$(aws datazone list-projects \
  --domain-identifier "$DOMAIN_ID" \
  --region "$REGION" --output json 2>/dev/null | python3 -c "
import json, sys
data = json.load(sys.stdin)
for p in data.get('items', []):
    if p['name'] == '${TIP_PROJECT_NAME}':
        print(p['id']); break
" 2>/dev/null || echo "")

if [ -n "$EXISTING_PROJECT" ]; then
  TIP_PROJECT_ID="$EXISTING_PROJECT"
  echo "  Project already exists: $TIP_PROJECT_ID"
else
  TIP_PROJECT_ID=$(aws datazone create-project \
    --domain-identifier "$DOMAIN_ID" \
    --name "$TIP_PROJECT_NAME" \
    --description "TIP-enabled project with per-user Redshift table isolation" \
    --project-profile-id "$PROFILE_ID" \
    --region "$REGION" --query 'id' --output text)
  echo "  Created project: $TIP_PROJECT_ID"
fi

###############################################################################
# Wait for environments to provision
###############################################################################
log "Waiting for project environments to provision"

for attempt in $(seq 1 60); do
  ENVS_JSON=$(aws datazone list-environments \
    --domain-identifier "$DOMAIN_ID" \
    --project-identifier "$TIP_PROJECT_ID" \
    --region "$REGION" --output json 2>/dev/null || echo '{"items":[]}')

  ENV_COUNT=$(echo "$ENVS_JSON" | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('items',[])))")
  ACTIVE_COUNT=$(echo "$ENVS_JSON" | python3 -c "import json,sys; print(sum(1 for e in json.load(sys.stdin).get('items',[]) if e.get('status')=='ACTIVE'))")

  if [ "$ENV_COUNT" -ge 2 ] && [ "$ACTIVE_COUNT" -ge 2 ]; then
    echo "  ✅ $ACTIVE_COUNT environments active"
    break
  fi
  echo "  ⏳ $ACTIVE_COUNT/$ENV_COUNT environments active (attempt $attempt/60)..."
  sleep 10
done

# Discover environment IDs
TOOLING_ENV_ID=$(echo "$ENVS_JSON" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for e in data.get('items', []):
    if 'Tooling' in e.get('name', '') or 'tooling' in e.get('name', '').lower():
        print(e['id']); break
" 2>/dev/null || echo "")

LAKEHOUSE_ENV_ID=$(echo "$ENVS_JSON" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for e in data.get('items', []):
    if 'Lakehouse' in e.get('name', '') or 'lakehouse' in e.get('name', '').lower() or 'Database' in e.get('name', ''):
        print(e['id']); break
" 2>/dev/null || echo "")

echo "  Tooling env:   $TOOLING_ENV_ID"
echo "  Lakehouse env: $LAKEHOUSE_ENV_ID"

# Derive the project user role
TIP_USER_ROLE="datazone_usr_role_${TIP_PROJECT_ID}_${TOOLING_ENV_ID}"
echo "  User role:     $TIP_USER_ROLE"

###############################################################################
# Create Redshift IAM connection (no credentials — TIP uses IdC identity)
###############################################################################
log "Creating Redshift IAM connection (no credentials)"

TIP_CONN_NAME="${CLUSTER_ID}-idc"

EXISTING_CONN=$(aws datazone list-connections \
  --domain-identifier "$DOMAIN_ID" \
  --environment-identifier "$TOOLING_ENV_ID" \
  --project-identifier "$TIP_PROJECT_ID" \
  --region "$REGION" --output json 2>/dev/null | python3 -c "
import json, sys
data = json.load(sys.stdin)
for c in data.get('items', []):
    if c.get('name') == '${TIP_CONN_NAME}':
        print(c['connectionId']); break
" 2>/dev/null || echo "")

if [ -n "$EXISTING_CONN" ]; then
  TIP_CONN_ID="$EXISTING_CONN"
  echo "  Connection already exists: $TIP_CONN_ID"
else
  # TIP connections omit the credentials field entirely — IAM auth via IdC
  TIP_CONN_ID=$(aws datazone create-connection \
    --domain-identifier "$DOMAIN_ID" \
    --environment-identifier "$TOOLING_ENV_ID" \
    --name "$TIP_CONN_NAME" \
    --description "Redshift IAM connection for TIP (no credentials)" \
    --props "{
      \"redshiftProperties\": {
        \"databaseName\": \"${REDSHIFT_DB}\",
        \"host\": \"${REDSHIFT_HOST}\",
        \"port\": ${REDSHIFT_PORT},
        \"storage\": {\"clusterName\": \"${CLUSTER_ID}\"}
      }
    }" --region "$REGION" --query 'connectionId' --output text)
  echo "  Created connection: $TIP_CONN_ID"
fi

###############################################################################
# Configure Lake Formation IdC integration
###############################################################################
log "Configuring Lake Formation IdC integration"

LF_IDC_CONFIG=$(aws lakeformation describe-lake-formation-identity-center-configuration \
  --region "$REGION" --output json 2>/dev/null || echo "")

if echo "$LF_IDC_CONFIG" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('CatalogId',''))" 2>/dev/null | grep -q "$ACCOUNT_ID"; then
  echo "  Lake Formation IdC already configured"
  LF_APP_ARN=$(echo "$LF_IDC_CONFIG" | python3 -c "import json,sys; print(json.load(sys.stdin).get('ApplicationArn',''))")
else
  LF_RESULT=$(aws lakeformation create-lake-formation-identity-center-configuration \
    --instance-arn "$IDC_INSTANCE_ARN" \
    --region "$REGION" --output json 2>/dev/null || echo "")

  if [ -n "$LF_RESULT" ]; then
    LF_APP_ARN=$(echo "$LF_RESULT" | python3 -c "import json,sys; print(json.load(sys.stdin).get('ApplicationArn',''))")
    echo "  ✅ Lake Formation IdC configured"
  else
    echo "  ⚠️  Could not create LF IdC config (may already exist)"
    LF_APP_ARN=$(aws lakeformation describe-lake-formation-identity-center-configuration \
      --region "$REGION" --query 'ApplicationArn' --output text 2>/dev/null || echo "")
  fi
fi

echo "  LF App ARN: $LF_APP_ARN"

###############################################################################
# Assign IdC groups/users to Lake Formation IdC application
###############################################################################
log "Assigning IdC groups/users to Lake Formation IdC app"

if [ -n "$LF_APP_ARN" ]; then
  # Extract the application ID from the ARN
  LF_APP_ID=$(echo "$LF_APP_ARN" | grep -o 'apl-[a-z0-9]*')

  # Assign IdC groups
  IFS=',' read -ra GROUP_ENTRIES <<< "$IDC_GROUPS"
  for entry in "${GROUP_ENTRIES[@]}"; do
    GROUP_NAME="${entry%%:*}"
    # Look up group ID
    GROUP_ID=$(aws identitystore list-groups \
      --identity-store-id "$IDENTITY_STORE_ID" \
      --filters "[{\"AttributePath\":\"DisplayName\",\"AttributeValue\":\"${GROUP_NAME}\"}]" \
      --region "$REGION" --query 'Groups[0].GroupId' --output text 2>/dev/null || echo "None")

    if [ "$GROUP_ID" != "None" ] && [ -n "$GROUP_ID" ]; then
      aws sso-admin create-application-assignment \
        --application-arn "$LF_APP_ARN" \
        --principal-id "$GROUP_ID" \
        --principal-type GROUP \
        --region "$REGION" 2>/dev/null || true
      echo "  ✅ Assigned group $GROUP_NAME ($GROUP_ID)"
    fi

    # Also assign individual users from the group
    USERS_PART="${entry#*:}"
    IFS=';' read -ra USERS <<< "$USERS_PART"
    for user_email in "${USERS[@]}"; do
      USER_ID=$(aws identitystore list-users \
        --identity-store-id "$IDENTITY_STORE_ID" \
        --filters "[{\"AttributePath\":\"UserName\",\"AttributeValue\":\"${user_email}\"}]" \
        --region "$REGION" --query 'Users[0].UserId' --output text 2>/dev/null || echo "None")

      if [ "$USER_ID" != "None" ] && [ -n "$USER_ID" ]; then
        aws sso-admin create-application-assignment \
          --application-arn "$LF_APP_ARN" \
          --principal-id "$USER_ID" \
          --principal-type USER \
          --region "$REGION" 2>/dev/null || true
        echo "  ✅ Assigned user $user_email ($USER_ID)"
      fi
    done
  done
else
  echo "  ⚠️  No LF App ARN — skipping assignments"
fi

###############################################################################
# Add IdC users as project members
###############################################################################
log "Adding IdC users as project members"

IFS=',' read -ra GROUP_ENTRIES <<< "$IDC_GROUPS"
for entry in "${GROUP_ENTRIES[@]}"; do
  USERS_PART="${entry#*:}"
  IFS=';' read -ra USERS <<< "$USERS_PART"
  for user_email in "${USERS[@]}"; do
    USER_ID=$(aws identitystore list-users \
      --identity-store-id "$IDENTITY_STORE_ID" \
      --filters "[{\"AttributePath\":\"UserName\",\"AttributeValue\":\"${user_email}\"}]" \
      --region "$REGION" --query 'Users[0].UserId' --output text 2>/dev/null || echo "None")

    if [ "$USER_ID" != "None" ] && [ -n "$USER_ID" ]; then
      # Look up the SMUS user profile for this IdC user
      DZ_USER_PROFILE=$(aws datazone search-user-profiles \
        --domain-identifier "$DOMAIN_ID" \
        --user-type SSO_USER \
        --search-text "$user_email" \
        --region "$REGION" --output json 2>/dev/null | python3 -c "
import json, sys
data = json.load(sys.stdin)
for p in data.get('items', []):
    s = p.get('status', '')
    if s in ('ASSIGNED', 'ACTIVATED', 'NOT_ACTIVATED', ''):
        print(p['id']); break
" 2>/dev/null || echo "")

      if [ -n "$DZ_USER_PROFILE" ]; then
        aws datazone create-project-membership \
          --domain-identifier "$DOMAIN_ID" \
          --project-identifier "$TIP_PROJECT_ID" \
          --member "{\"userIdentifier\": \"$DZ_USER_PROFILE\"}" \
          --designation PROJECT_CONTRIBUTOR \
          --region "$REGION" 2>/dev/null || true
        echo "  ✅ Added $user_email as PROJECT_CONTRIBUTOR"
      else
        # Profile doesn't exist yet — create it, then add to project
        echo "  Creating domain profile for $user_email..."
        DZ_USER_PROFILE=$(aws datazone create-user-profile \
          --domain-identifier "$DOMAIN_ID" \
          --user-identifier "$USER_ID" \
          --user-type SSO_USER \
          --region "$REGION" --query 'id' --output text 2>/dev/null || echo "")
        if [ -n "$DZ_USER_PROFILE" ] && [ "$DZ_USER_PROFILE" != "None" ]; then
          aws datazone create-project-membership \
            --domain-identifier "$DOMAIN_ID" \
            --project-identifier "$TIP_PROJECT_ID" \
            --member "{\"userIdentifier\": \"$DZ_USER_PROFILE\"}" \
            --designation PROJECT_CONTRIBUTOR \
            --region "$REGION" 2>/dev/null || true
          echo "  ✅ Created profile and added $user_email as PROJECT_CONTRIBUTOR"
        else
          echo "  ⚠️  Could not create profile for $user_email — add them to the domain manually"
        fi
      fi
    fi
  done
done


###############################################################################
# Grant IAM permissions to project user role
###############################################################################
log "Granting IAM permissions to project user role"

# TIP requires broad Redshift + SSO + STS permissions on the project role.
# Missing redshift:GetClusterCredentialsWithIAM causes CredentialsProviderError.
aws iam put-role-policy --role-name "$TIP_USER_ROLE" \
  --policy-name "TIPRedshiftAccess" \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "redshift:*",
          "redshift-data:*",
          "redshift-serverless:*"
        ],
        "Resource": "*"
      },
      {
        "Effect": "Allow",
        "Action": [
          "sso:*",
          "sso-oauth:*"
        ],
        "Resource": "*"
      },
      {
        "Effect": "Allow",
        "Action": "sts:*",
        "Resource": "*"
      }
    ]
  }' 2>/dev/null || echo "  ⚠️  Could not attach policy (role may not exist yet)"

echo "  ✅ IAM permissions granted to $TIP_USER_ROLE"

###############################################################################
# Add project role as Lake Formation admin
###############################################################################
log "Adding project role as Lake Formation admin"

TIP_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${TIP_USER_ROLE}"

# Get current LF admins and add the TIP role
CURRENT_ADMINS=$(aws lakeformation get-data-lake-settings \
  --region "$REGION" --output json 2>/dev/null)

UPDATED_SETTINGS=$(echo "$CURRENT_ADMINS" | python3 -c "
import json, sys
data = json.load(sys.stdin)
settings = data.get('DataLakeSettings', {})
admins = settings.get('DataLakeAdmins', [])
tip_arn = '${TIP_ROLE_ARN}'
if not any(a.get('DataLakePrincipalIdentifier') == tip_arn for a in admins):
    admins.append({'DataLakePrincipalIdentifier': tip_arn})
settings['DataLakeAdmins'] = admins
# Remove read-only fields
settings.pop('CreateDatabaseDefaultPermissions', None)
settings.pop('CreateTableDefaultPermissions', None)
print(json.dumps(settings))
")

aws lakeformation put-data-lake-settings \
  --data-lake-settings "$UPDATED_SETTINGS" \
  --region "$REGION" 2>/dev/null || echo "  ⚠️  Could not update LF settings"

echo "  ✅ $TIP_USER_ROLE added as Lake Formation admin"

###############################################################################
# Resolve IdC user IDs and create Redshift users with TIP identity pattern
###############################################################################
log "Creating Redshift users for TIP identities"

# Collect all unique IdC user IDs from IDC_GROUPS
# Using parallel arrays (bash 3.x compat)
USER_EMAILS_LIST=()
USER_IDS_LIST=()

IFS=',' read -ra GROUP_ENTRIES <<< "$IDC_GROUPS"
for entry in "${GROUP_ENTRIES[@]}"; do
  USERS_PART="${entry#*:}"
  IFS=';' read -ra USERS <<< "$USERS_PART"
  for user_email in "${USERS[@]}"; do
    USER_ID=$(aws identitystore list-users \
      --identity-store-id "$IDENTITY_STORE_ID" \
      --filters "[{\"AttributePath\":\"UserName\",\"AttributeValue\":\"${user_email}\"}]" \
      --region "$REGION" --query 'Users[0].UserId' --output text 2>/dev/null || echo "None")

    if [ "$USER_ID" != "None" ] && [ -n "$USER_ID" ]; then
      USER_EMAILS_LIST+=("$user_email")
      USER_IDS_LIST+=("$USER_ID")
      echo "  Resolved $user_email → $USER_ID"
    else
      echo "  ⚠️  Could not resolve $user_email"
    fi
  done
done

# Create Redshift users for each TIP identity
# Pattern: IAMR:user-<idcUserId>@<projectId>
for i in "${!USER_IDS_LIST[@]}"; do
  USER_ID="${USER_IDS_LIST[$i]}"
  RS_USER="IAMR:user-${USER_ID}@${TIP_PROJECT_ID}"

  echo "  Creating Redshift user: $RS_USER"

  STMT_ID=$(aws redshift-data execute-statement \
    --cluster-identifier "$CLUSTER_ID" --database "$REDSHIFT_DB" --db-user "$REDSHIFT_USER" \
    --sql "CREATE USER \"${RS_USER}\" PASSWORD DISABLE" \
    --region "$REGION" --output text --query 'Id' 2>/dev/null || echo "")

  if [ -n "$STMT_ID" ]; then
    for i in $(seq 1 15); do
      QSTATUS=$(aws redshift-data describe-statement --id "$STMT_ID" --region "$REGION" --output text --query 'Status' 2>/dev/null || echo "")
      [ "$QSTATUS" = "FINISHED" ] && break
      [ "$QSTATUS" = "FAILED" ] && break
      sleep 2
    done
    if [ "$QSTATUS" = "FINISHED" ]; then
      echo "    ✅ User created"
    elif [ "$QSTATUS" = "FAILED" ]; then
      ERR=$(aws redshift-data describe-statement --id "$STMT_ID" --region "$REGION" --query 'Error' --output text 2>/dev/null || echo "unknown")
      if echo "$ERR" | grep -qi "already exists"; then
        echo "    ✅ User already exists"
      else
        echo "    ⚠️  $ERR"
      fi
    fi
  fi
done

###############################################################################
# Grant per-user table-level access based on TIP_USER_GRANTS
###############################################################################
log "Granting per-user table-level access"

if [ -z "$TIP_USER_GRANTS" ]; then
  echo "  ℹ️  TIP_USER_GRANTS not set — skipping per-user grants"
  echo "  Format: 'idcUserId:LEVEL:table1;table2,...'"
  echo "  Example: 'd418f428-...:SELECT:departments,84f83498-...:SELECT:employees'"
else
  IFS=',' read -ra GRANT_ENTRIES <<< "$TIP_USER_GRANTS"
  for grant_entry in "${GRANT_ENTRIES[@]}"; do
    IFS=':' read -r GRANT_USER_ID GRANT_LEVEL GRANT_TABLES <<< "$grant_entry"

    RS_USER="IAMR:user-${GRANT_USER_ID}@${TIP_PROJECT_ID}"
    echo "  Granting ${GRANT_LEVEL} to ${RS_USER}"

    # Ensure user exists first
    STMT_ID=$(aws redshift-data execute-statement \
      --cluster-identifier "$CLUSTER_ID" --database "$REDSHIFT_DB" --db-user "$REDSHIFT_USER" \
      --sql "CREATE USER \"${RS_USER}\" PASSWORD DISABLE" \
      --region "$REGION" --output text --query 'Id' 2>/dev/null || echo "")
    if [ -n "$STMT_ID" ]; then
      for i in $(seq 1 10); do
        QSTATUS=$(aws redshift-data describe-statement --id "$STMT_ID" --region "$REGION" --output text --query 'Status' 2>/dev/null || echo "")
        [ "$QSTATUS" = "FINISHED" ] || [ "$QSTATUS" = "FAILED" ] && break
        sleep 2
      done
    fi

    # Grant on each table
    IFS=';' read -ra TABLES <<< "$GRANT_TABLES"
    for table in "${TABLES[@]}"; do
      if [ "$table" = "*" ]; then
        GRANT_SQL="GRANT ${GRANT_LEVEL} ON ALL TABLES IN SCHEMA public TO \"${RS_USER}\""
      else
        GRANT_SQL="GRANT ${GRANT_LEVEL} ON TABLE public.${table} TO \"${RS_USER}\""
      fi

      STMT_ID=$(aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" --database "$REDSHIFT_DB" --db-user "$REDSHIFT_USER" \
        --sql "$GRANT_SQL" \
        --region "$REGION" --output text --query 'Id' 2>/dev/null || echo "")

      if [ -n "$STMT_ID" ]; then
        for i in $(seq 1 10); do
          QSTATUS=$(aws redshift-data describe-statement --id "$STMT_ID" --region "$REGION" --output text --query 'Status' 2>/dev/null || echo "")
          [ "$QSTATUS" = "FINISHED" ] || [ "$QSTATUS" = "FAILED" ] && break
          sleep 2
        done
        if [ "$QSTATUS" = "FINISHED" ]; then
          echo "    ✅ ${GRANT_LEVEL} on ${table}"
        else
          ERR=$(aws redshift-data describe-statement --id "$STMT_ID" --region "$REGION" --query 'Error' --output text 2>/dev/null || echo "unknown")
          echo "    ⚠️  ${table}: $ERR"
        fi
      fi
    done
  done
fi

###############################################################################
# Summary
###############################################################################
log "🎉 TIP PROJECT SETUP COMPLETE"
echo ""
echo "  Project:         $TIP_PROJECT_NAME ($TIP_PROJECT_ID)"
echo "  Profile:         $TIP_PROFILE_NAME ($PROFILE_ID)"
echo "  Tooling env:     $TOOLING_ENV_ID"
echo "  Lakehouse env:   $LAKEHOUSE_ENV_ID"
echo "  Connection:      $TIP_CONN_ID"
echo "  User role:       $TIP_USER_ROLE"
echo ""
echo "  TIP identity pattern: IAMR:user-<idcUserId>@${TIP_PROJECT_ID}"
echo ""
echo "  IMPORTANT NOTES:"
echo "  - Per-user isolation works on the DIRECT Redshift connection path (dev → public)"
echo "  - The federated catalog path (dev@cluster → public) uses the project role"
echo "    so all users see the same tables in the Lakehouse data explorer tree"
echo "  - TIP does NOT support subscription/publish of data products yet"
echo "  - Users must use the direct 'dev' database path for isolated queries"
