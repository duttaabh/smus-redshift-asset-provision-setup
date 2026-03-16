#!/bin/bash
###############################################################################
# Generate .env file for SMUS Redshift setup scripts
#
# Prompts for key inputs, auto-resolves everything else from AWS APIs.
#
# Usage: ./generate-env.sh
#        ./generate-env.sh --profile azure-ad --cluster redshift-cluster-1
###############################################################################
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"

# Parse CLI overrides
while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)  _PROFILE="$2"; shift 2 ;;
    --cluster)  _CLUSTER="$2"; shift 2 ;;
    --region)   _REGION="$2"; shift 2 ;;
    --db-user)  _DBUSER="$2"; shift 2 ;;
    --db-pass)  _DBPASS="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

prompt() {
  local var="$1" msg="$2" default="${3:-}"
  if [ -n "$default" ]; then
    read -rp "$msg [$default]: " val
    eval "$var=\"${val:-$default}\""
  else
    read -rp "$msg: " val
    eval "$var=\"$val\""
  fi
}

echo "=== SMUS Redshift .env Generator ==="
echo ""

###############################################################################
# 1. AWS Profile & Region
###############################################################################
prompt AWS_PROFILE "AWS CLI profile" "${_PROFILE:-}"
export AWS_PROFILE
prompt REGION "AWS region" "${_REGION:-us-east-1}"

echo ""
echo "--- Verifying AWS access ---"
ACCOUNT_ID=$(aws sts get-caller-identity --region "$REGION" \
  --query Account --output text 2>/dev/null || echo "")
if [ -z "$ACCOUNT_ID" ]; then
  echo "❌ Cannot reach AWS with profile '$AWS_PROFILE'."
  exit 1
fi
echo "  Account: $ACCOUNT_ID"

###############################################################################
# 2. Redshift cluster
###############################################################################
echo ""
echo "--- Redshift cluster ---"
CLUSTERS=$(aws redshift describe-clusters --region "$REGION" \
  --query 'Clusters[*].ClusterIdentifier' --output text 2>/dev/null || echo "")
[ -n "$CLUSTERS" ] && echo "  Available: $CLUSTERS"

prompt CLUSTER_ID "Cluster ID" "${_CLUSTER:-}"

echo "  Resolving cluster details..."
_CJ=$(aws redshift describe-clusters --cluster-identifier "$CLUSTER_ID" \
  --region "$REGION" --output json 2>/dev/null || echo '{"Clusters":[]}')

read_cluster() { echo "$_CJ" | python3 -c "$1" 2>/dev/null || echo "$2"; }

REDSHIFT_HOST=$(read_cluster "
import json,sys; c=json.load(sys.stdin)['Clusters']
print(c[0]['Endpoint']['Address'] if c else '')" "")
REDSHIFT_PORT=$(read_cluster "
import json,sys; c=json.load(sys.stdin)['Clusters']
print(c[0]['Endpoint']['Port'] if c else 5439)" "5439")
REDSHIFT_DB=$(read_cluster "
import json,sys; c=json.load(sys.stdin)['Clusters']
print(c[0].get('DBName','dev') if c else 'dev')" "dev")
VPC_ID=$(read_cluster "
import json,sys; c=json.load(sys.stdin)['Clusters']
print(c[0]['VpcId'] if c else '')" "")
REDSHIFT_SG=$(read_cluster "
import json,sys; c=json.load(sys.stdin)['Clusters']
sgs=c[0].get('VpcSecurityGroups',[]) if c else []
print(sgs[0]['VpcSecurityGroupId'] if sgs else '')" "")
_DEF_USER=$(read_cluster "
import json,sys; c=json.load(sys.stdin)['Clusters']
print(c[0].get('MasterUsername','awsuser') if c else 'awsuser')" "awsuser")

echo "  Host: $REDSHIFT_HOST | Port: $REDSHIFT_PORT | DB: $REDSHIFT_DB"
echo "  VPC: $VPC_ID | SG: $REDSHIFT_SG"

prompt REDSHIFT_USER "Redshift admin user" "${_DBUSER:-$_DEF_USER}"
prompt REDSHIFT_PASSWORD "Redshift admin password" "${_DBPASS:-}"

# Auto-resolve subnet from VPC (SMUS_SG resolved after DataZone section)
SUBNET_ID=""
SMUS_SG=""
if [ -n "$VPC_ID" ]; then
  # Pick the subnet in the earliest AZ (e.g. us-east-1a) for consistency
  SUBNET_ID=$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC_ID" \
    --region "$REGION" --output json 2>/dev/null \
    | python3 -c "
import json,sys
subnets=json.load(sys.stdin).get('Subnets',[])
subnets.sort(key=lambda s: s.get('AvailabilityZone',''))
print(subnets[0]['SubnetId'] if subnets else '')
" 2>/dev/null || echo "")
fi

###############################################################################
# 3. DataZone / SMUS domain
###############################################################################
echo ""
echo "--- DataZone / SMUS ---"

_DJ=$(aws datazone list-domains --region "$REGION" --output json 2>/dev/null \
  || echo '{"items":[]}')
echo "$_DJ" | python3 -c "
import json,sys
for d in json.load(sys.stdin).get('items',[]):
    print(f\"  {d['id']}  {d['name']}  ({d.get('status','')})\")" 2>/dev/null || true

AUTO_DOMAIN=$(echo "$_DJ" | python3 -c "
import json,sys; items=json.load(sys.stdin).get('items',[])
print(items[0]['id'] if len(items)==1 else '')" 2>/dev/null || echo "")

prompt DOMAIN_ID "DataZone domain ID" "$AUTO_DOMAIN"

# Resolve projects
echo "  Resolving projects..."
_PJ=$(aws datazone list-projects --domain-identifier "$DOMAIN_ID" \
  --region "$REGION" --output json 2>/dev/null || echo '{"items":[]}')

echo "$_PJ" | python3 -c "
import json,sys
for p in json.load(sys.stdin).get('items',[]):
    print(f\"    {p['id']}  {p['name']}\")" 2>/dev/null || true

# Pick admin project: prefer one with 'admin' in name, else first
AUTO_ADMIN=$(echo "$_PJ" | python3 -c "
import json,sys; items=json.load(sys.stdin).get('items',[])
for p in items:
    if 'admin' in p['name'].lower(): print(p['id']); break
else:
    if items: print(items[0]['id'])
" 2>/dev/null || echo "")

prompt ADMIN_PROJECT_ID "Admin project ID" "$AUTO_ADMIN"

# Resolve admin environment
ADMIN_ENV_ID=$(aws datazone list-environments \
  --domain-identifier "$DOMAIN_ID" --project-identifier "$ADMIN_PROJECT_ID" \
  --region "$REGION" --output json 2>/dev/null | python3 -c "
import json,sys
for e in json.load(sys.stdin).get('items',[]):
    if 'Tooling' in e.get('name','') or e.get('status')=='ACTIVE':
        print(e['id']); break
" 2>/dev/null || echo "")
echo "  Admin env: $ADMIN_ENV_ID"

# Auto-resolve SMUS SG: find the DataZone SG for the admin environment.
# The domain's environments may be in a different VPC than Redshift (cross-VPC
# connections are common). We resolve from the admin env's provisioned resources.
SMUS_SG=""
if [ -n "$ADMIN_ENV_ID" ]; then
  SMUS_SG=$(aws datazone get-environment --domain-identifier "$DOMAIN_ID" \
    --identifier "$ADMIN_ENV_ID" --region "$REGION" --output json 2>/dev/null \
    | python3 -c "
import json,sys
env=json.load(sys.stdin)
for r in env.get('provisionedResources',[]):
    if r.get('name')=='securityGroup':
        print(r['value']); break
" 2>/dev/null || echo "")
fi

# If not found from env, fall back to DataZone SGs in the Redshift VPC
if [ -z "$SMUS_SG" ] && [ -n "$VPC_ID" ]; then
  # Collect all environment IDs across all projects in this domain
  _ALL_ENV_IDS=""
  echo "$_PJ" | python3 -c "
import json,sys
for p in json.load(sys.stdin).get('items',[]):
    print(p['id'])
" 2>/dev/null | while read -r _pid; do
    _envs=$(aws datazone list-environments \
      --domain-identifier "$DOMAIN_ID" --project-identifier "$_pid" \
      --region "$REGION" --query 'items[*].id' --output text 2>/dev/null || echo "")
    _ALL_ENV_IDS="${_ALL_ENV_IDS} ${_envs}"
  done

  SMUS_SG=$(aws ec2 describe-security-groups \
    --filters "Name=vpc-id,Values=$VPC_ID" "Name=group-name,Values=datazone-*" \
    --region "$REGION" --output json 2>/dev/null \
    | python3 -c "
import json,sys
env_ids=set('${_ALL_ENV_IDS}'.split())
sgs=json.load(sys.stdin).get('SecurityGroups',[])
for sg in sgs:
    tags={t['Key']:t['Value'] for t in sg.get('Tags',[])}
    if tags.get('AmazonDataZoneEnvironment','') in env_ids:
        print(sg['GroupId']); exit()
print('')
" 2>/dev/null || echo "")
fi

if [ -z "$SMUS_SG" ]; then
  echo "  ⚠️  No DataZone SG auto-resolved (set SMUS_SG manually in .env if needed)"
else
  echo "  SMUS SG: $SMUS_SG"
fi

###############################################################################
# 4. IdC groups & grants
###############################################################################
echo ""
echo "--- IdC Groups ---"
echo "  Format: GroupName:user@email,GroupName2:user@email"
echo "  (multiple users per group: GroupName:user1@email;user2@email)"
prompt IDC_GROUPS "IdC groups" ""

echo ""
echo "  Format: GroupName:LEVEL:table1;table2 (use * for all tables)"
prompt IDC_GROUP_GRANTS "IdC group grants" ""

###############################################################################
# 5. Subscription config
###############################################################################
echo ""
echo "--- Subscriptions (for 07-subscribe-assets.sh) ---"

# List consumer projects (non-admin)
echo "$_PJ" | python3 -c "
import json,sys
for p in json.load(sys.stdin).get('items',[]):
    if p['id'] != '$ADMIN_PROJECT_ID':
        print(f\"    {p['id']}  {p['name']}\")" 2>/dev/null || true

prompt CONSUMER_PROJECT_ID "Consumer project ID" ""

# List Redshift tables
echo "  Discovering tables in ${REDSHIFT_DB}.public..."
_STMT=$(aws redshift-data execute-statement \
  --cluster-identifier "$CLUSTER_ID" --database "$REDSHIFT_DB" \
  --db-user "$REDSHIFT_USER" --region "$REGION" \
  --sql "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY 1" \
  --query 'Id' --output text 2>/dev/null || echo "")
if [ -n "$_STMT" ]; then
  sleep 3
  AVAIL_TABLES=$(aws redshift-data get-statement-result --id "$_STMT" \
    --region "$REGION" --query 'Records[*][0].stringValue' \
    --output text 2>/dev/null | tr '\t' ',' || echo "")
  [ -n "$AVAIL_TABLES" ] && echo "  Available tables: $AVAIL_TABLES"
fi

prompt SUBSCRIBE_TABLES "Tables to subscribe (comma-separated)" "${AVAIL_TABLES:-}"

###############################################################################
# 6. TIP config — auto-resolve profile, IdC user IDs, and grants
###############################################################################
echo ""
echo "--- TIP (Trusted Identity Propagation) ---"

prompt TIP_PROJECT_NAME "TIP project name" "tip-redshift-project"

# Auto-resolve project profile name
AUTO_PROFILE=$(aws datazone list-project-profiles \
  --domain-identifier "$DOMAIN_ID" --region "$REGION" --output json 2>/dev/null \
  | python3 -c "
import json,sys
for p in json.load(sys.stdin).get('items',[]):
    if 'sql' in p['name'].lower() or 'analytics' in p['name'].lower():
        print(p['name']); break
" 2>/dev/null || echo "SQL analytics")
prompt TIP_PROFILE_NAME "TIP profile name" "$AUTO_PROFILE"

# Auto-resolve IdC user IDs from IDC_GROUPS emails and build TIP_USER_GRANTS
echo "  Resolving IdC user IDs from group members..."
IDC_INSTANCE_ARN=$(aws sso-admin list-instances \
  --query 'Instances[0].InstanceArn' --output text --region "$REGION" 2>/dev/null || echo "")
IDENTITY_STORE_ID=$(aws sso-admin list-instances \
  --query 'Instances[0].IdentityStoreId' --output text --region "$REGION" 2>/dev/null || echo "")

TIP_USER_GRANTS=""
if [ -n "$IDENTITY_STORE_ID" ] && [ "$IDENTITY_STORE_ID" != "None" ] && [ -n "$IDC_GROUPS" ]; then
  # Parse IDC_GROUP_GRANTS into a lookup: email -> grant level + tables
  # We map group members to their group's grant level
  IFS=',' read -ra _TIP_GROUP_ENTRIES <<< "$IDC_GROUPS"
  for _tge in "${_TIP_GROUP_ENTRIES[@]}"; do
    _TIP_GNAME="${_tge%%:*}"
    _TIP_USERS="${_tge#*:}"

    # Find this group's grant from IDC_GROUP_GRANTS
    _TIP_GLEVEL=""
    _TIP_GTABLES=""
    IFS=',' read -ra _TIP_GRANT_ENTRIES <<< "$IDC_GROUP_GRANTS"
    for _tgg in "${_TIP_GRANT_ENTRIES[@]}"; do
      _GG_NAME=$(echo "$_tgg" | cut -d: -f1)
      if [ "$_GG_NAME" = "$_TIP_GNAME" ]; then
        _TIP_GLEVEL=$(echo "$_tgg" | cut -d: -f2)
        _TIP_GTABLES=$(echo "$_tgg" | cut -d: -f3)
        break
      fi
    done

    [ -z "$_TIP_GLEVEL" ] && continue

    # Resolve each user email to IdC user ID
    IFS=';' read -ra _TIP_EMAILS <<< "$_TIP_USERS"
    for _email in "${_TIP_EMAILS[@]}"; do
      _IDC_UID=$(aws identitystore list-users \
        --identity-store-id "$IDENTITY_STORE_ID" \
        --filters "[{\"AttributePath\":\"UserName\",\"AttributeValue\":\"${_email}\"}]" \
        --region "$REGION" --query 'Users[0].UserId' --output text 2>/dev/null || echo "None")

      if [ "$_IDC_UID" != "None" ] && [ -n "$_IDC_UID" ]; then
        echo "    $_email → $_IDC_UID (${_TIP_GLEVEL} on ${_TIP_GTABLES})"
        [ -n "$TIP_USER_GRANTS" ] && TIP_USER_GRANTS="${TIP_USER_GRANTS},"
        TIP_USER_GRANTS="${TIP_USER_GRANTS}${_IDC_UID}:${_TIP_GLEVEL}:${_TIP_GTABLES}"
      else
        echo "    ⚠️  $_email — not found in IdC"
      fi
    done
  done

  if [ -n "$TIP_USER_GRANTS" ]; then
    echo "  Auto-resolved TIP_USER_GRANTS: $TIP_USER_GRANTS"
  fi
else
  echo "  ⚠️  No IdC instance or IDC_GROUPS — cannot auto-resolve TIP grants"
fi

# Allow override
if [ -n "$TIP_USER_GRANTS" ]; then
  prompt TIP_USER_GRANTS "TIP user grants (auto-resolved, edit or Enter to keep)" "$TIP_USER_GRANTS"
else
  prompt TIP_USER_GRANTS "TIP user grants (idcUserId:LEVEL:tables,...)" ""
fi

###############################################################################
# Write .env
###############################################################################
echo ""
echo "--- Writing $ENV_FILE ---"

cat > "$ENV_FILE" << ENVEOF
# SMUS Redshift Setup — auto-generated by generate-env.sh
# $(date)

# AWS
AWS_PROFILE=${AWS_PROFILE}
REGION=${REGION}

# Redshift
CLUSTER_ID=${CLUSTER_ID}
REDSHIFT_HOST=${REDSHIFT_HOST}
REDSHIFT_PORT=${REDSHIFT_PORT}
REDSHIFT_DB=${REDSHIFT_DB}
REDSHIFT_USER=${REDSHIFT_USER}
REDSHIFT_PASSWORD='${REDSHIFT_PASSWORD}'

# DataZone / SMUS
DOMAIN_ID=${DOMAIN_ID}
ADMIN_PROJECT_ID=${ADMIN_PROJECT_ID}
ADMIN_ENV_ID=${ADMIN_ENV_ID}

# VPC / Network (auto-resolved from cluster)
VPC_ID=${VPC_ID}
SUBNET_ID=${SUBNET_ID}
SMUS_SG=${SMUS_SG}
REDSHIFT_SG=${REDSHIFT_SG}

# IdC Integration (for 01-setup-idc-redshift.sh)
IDC_NAMESPACE=AWSIDC
IDC_ROLE_NAME=RedshiftIdCIntegrationRole
IDC_GROUPS="${IDC_GROUPS}"
IDC_GROUP_GRANTS="${IDC_GROUP_GRANTS}"

# Subscriptions (for 07-subscribe-assets.sh)
CONSUMER_PROJECT_ID=${CONSUMER_PROJECT_ID}
SUBSCRIBE_TABLES="${SUBSCRIBE_TABLES}"

# TIP (for 04-setup-tip-project.sh)
TIP_PROJECT_NAME=${TIP_PROJECT_NAME}
TIP_PROFILE_NAME="${TIP_PROFILE_NAME}"
TIP_USER_GRANTS="${TIP_USER_GRANTS}"
ENVEOF

echo "✅ .env written successfully"
echo ""
echo "Review: cat $ENV_FILE"
echo "Then run: ./01-setup-idc-redshift.sh"
