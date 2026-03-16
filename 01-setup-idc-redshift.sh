#!/bin/bash
set -euo pipefail
###############################################################################
# Set up IAM Identity Center (IdC) integration with Redshift
#
# This script automates the full end-to-end IdC ↔ Redshift setup:
#   1. Creates an IAM role for Redshift ↔ IdC communication
#   2. Attaches the role to the Redshift cluster
#   3. Creates the Redshift IdC application
#   4. Enables the Redshift Connect service integration (via boto3)
#   5. Creates IdC groups and assigns users to them
#   6. Assigns groups/users to ALL THREE IdC apps:
#      - Redshift app (cluster federation)
#      - QEV2/SQL Workbench app (Query Editor v2 token exchange)
#      - Console TIP app (Trusted Identity Propagation for AWS Console)
#   7. Creates a Permission Set with Redshift + QEV2 access
#   8. Assigns the Permission Set to the AWS account for IdC groups
#   9. Registers the identity provider inside Redshift (CREATE IDENTITY PROVIDER)
#  10. Creates matching Redshift roles and grants table-level permissions
#
# KEY LEARNINGS:
#   - CREATE IDENTITY PROVIDER must use IdcManagedApplicationArn (SSO app ARN
#     like arn:aws:sso::ACCOUNT:application/...), NOT the Redshift IdC app ARN.
#     Using the wrong one gives "Failed to obtain AWS IdC Info".
#   - CLI v2.27 doesn't support the 'Redshift' service integration param in
#     modify-redshift-idc-application — must use boto3 instead.
#   - Users need assignments on THREE IdC apps (Redshift, QEV2, Console TIP),
#     not just the Redshift app. Missing QEV2 assignment causes "Invalid scope".
#   - Users need an IdC Permission Set assigned to the AWS account, otherwise
#     the access portal "Accounts" tab is empty and they can't reach the console.
#   - The Permission Set needs AmazonRedshiftFullAccess +
#     AmazonRedshiftQueryEditorV2FullAccess managed policies. The ReadSharing
#     policy is insufficient — QEV2 needs FullAccess for initial account setup.
#   - An inline policy with sso:*, sso-oauth:*, sts:*, redshift:*,
#     redshift-serverless:*, sqlworkbench:* prevents console permission errors.
#   - Grant table-level access (not schema-level) for proper isolation.
#     Users only see tables they have SELECT on in QEV2.
#   - QEV2 IdC app (sqlworkbench provider) and Console TIP app are auto-created
#     when IdC integration is enabled from the Redshift console. This script
#     discovers them automatically.
#   - Users must access QEV2 in the SAME REGION as the Redshift cluster.
#     Wrong region gives "User information couldn't be retrieved".
#   - If IdC session expires in QEV2, right-click the cluster → Refresh.
#   - Browser third-party cookies must be allowed for IdC auth to work.
#
# Usage: ./01-setup-idc-redshift.sh
#
# Required env vars (via .env or environment):
#   CLUSTER_ID, REDSHIFT_USER, REDSHIFT_DB, REGION
#
# Optional env vars:
#   IDC_NAMESPACE        — namespace prefix for Redshift roles (default: AWSIDC)
#   IDC_ROLE_NAME        — IAM role for IdC integration (default: RedshiftIdCIntegrationRole)
#   IDC_APP_NAME         — Redshift IdC application name (default: ${CLUSTER_ID}-idc)
#   IDC_GROUPS           — comma-separated "group:user_email" pairs
#                          e.g. "DataAnalysts:viewer@example.com,DataEngineers:admin@example.com"
#   IDC_GROUP_GRANTS     — comma-separated "group:grant_level:table1,table2" triples
#                          grant_level is SELECT or ALL
#                          tables is a comma-separated list (or * for all tables)
#                          e.g. "DataAnalysts:SELECT:employees,DataEngineers:ALL:departments"
#   IDC_PS_NAME          — Permission Set name (default: RedshiftQueryAccess)
###############################################################################

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  echo "Usage: $0"
  echo ""
  echo "Sets up IAM Identity Center integration with a Redshift cluster."
  echo "Creates IdC groups, assigns users, registers identity provider in"
  echo "Redshift, creates Permission Set, and grants table-level access."
  echo ""
  echo "Configure via .env file or environment variables. See config.sh."
  echo ""
  echo "IdC-specific variables:"
  echo "  IDC_NAMESPACE     Redshift namespace prefix (default: AWSIDC)"
  echo "  IDC_ROLE_NAME     IAM role name (default: RedshiftIdCIntegrationRole)"
  echo "  IDC_GROUPS        Group-to-user mappings: 'Group1:user@email,Group2:user@email'"
  echo "  IDC_GROUP_GRANTS  Table-level grants: 'Group1:SELECT:table1,Group2:ALL:table2'"
  echo "                    Use * for all tables: 'Group1:SELECT:*'"
  echo "  IDC_PS_NAME       Permission Set name (default: RedshiftQueryAccess)"
  exit 0
fi

source "$(dirname "$0")/config.sh"

IDC_NAMESPACE="${IDC_NAMESPACE:-AWSIDC}"
IDC_ROLE_NAME="${IDC_ROLE_NAME:-RedshiftIdCIntegrationRole}"
IDC_APP_NAME="${IDC_APP_NAME:-${CLUSTER_ID}-idc}"
IDC_GROUPS="${IDC_GROUPS:-}"
IDC_GROUP_GRANTS="${IDC_GROUP_GRANTS:-}"
IDC_PS_NAME="${IDC_PS_NAME:-RedshiftQueryAccess}"

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
# IAM role for Redshift ↔ IdC
###############################################################################
log "Creating IAM role for Redshift IdC integration"

TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"redshift.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

IDC_ROLE_ARN=$(aws iam create-role \
  --role-name "$IDC_ROLE_NAME" \
  --assume-role-policy-document "$TRUST_POLICY" \
  --query 'Role.Arn' --output text 2>/dev/null || \
  aws iam get-role --role-name "$IDC_ROLE_NAME" --query 'Role.Arn' --output text)

echo "  Role ARN: $IDC_ROLE_ARN"

aws iam put-role-policy --role-name "$IDC_ROLE_NAME" \
  --policy-name IdCAccess \
  --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["sso:*","sso-oauth:*","identitystore:*"],"Resource":"*"}]}'

echo "  ✅ IAM role ready"

###############################################################################
# Attach role to Redshift cluster
###############################################################################
log "Attaching IAM role to Redshift cluster"

EXISTING_ROLES=$(aws redshift describe-clusters --cluster-identifier "$CLUSTER_ID" \
  --region "$REGION" --query 'Clusters[0].IamRoles[*].IamRoleArn' --output text 2>/dev/null || echo "")

if echo "$EXISTING_ROLES" | grep -q "$IDC_ROLE_ARN"; then
  echo "  Already attached"
else
  aws redshift modify-cluster-iam-roles \
    --cluster-identifier "$CLUSTER_ID" \
    --add-iam-roles "$IDC_ROLE_ARN" \
    --region "$REGION" > /dev/null
  echo "  Attached — waiting for in-sync..."
  for i in $(seq 1 20); do
    STATUS=$(aws redshift describe-clusters --cluster-identifier "$CLUSTER_ID" \
      --region "$REGION" --output text \
      --query "Clusters[0].IamRoles[?IamRoleArn=='${IDC_ROLE_ARN}'].ApplyStatus" 2>/dev/null || echo "")
    [ "$STATUS" = "in-sync" ] && break
    sleep 5
  done
fi

echo "  ✅ Role attached to cluster"

###############################################################################
# Create Redshift IdC application
###############################################################################
log "Creating Redshift IdC application"

EXISTING_APP=$(aws redshift describe-redshift-idc-applications \
  --region "$REGION" --output json \
  --query "RedshiftIdcApplications[?RedshiftIdcApplicationName=='${IDC_APP_NAME}']" 2>/dev/null || echo "[]")

APP_COUNT=$(echo "$EXISTING_APP" | python3 -c "import json,sys; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")

if [ "$APP_COUNT" -gt "0" ]; then
  REDSHIFT_IDC_APP_ARN=$(echo "$EXISTING_APP" | python3 -c "import json,sys; print(json.load(sys.stdin)[0]['RedshiftIdcApplicationArn'])")
  IDC_MANAGED_APP_ARN=$(echo "$EXISTING_APP" | python3 -c "import json,sys; print(json.load(sys.stdin)[0]['IdcManagedApplicationArn'])")
  echo "  Already exists"
else
  APP_RESULT=$(aws redshift create-redshift-idc-application \
    --idc-instance-arn "$IDC_INSTANCE_ARN" \
    --redshift-idc-application-name "$IDC_APP_NAME" \
    --idc-display-name "${CLUSTER_ID}-IdC" \
    --iam-role-arn "$IDC_ROLE_ARN" \
    --region "$REGION" --output json)
  REDSHIFT_IDC_APP_ARN=$(echo "$APP_RESULT" | python3 -c "import json,sys; print(json.load(sys.stdin)['RedshiftIdcApplication']['RedshiftIdcApplicationArn'])")
  IDC_MANAGED_APP_ARN=$(echo "$APP_RESULT" | python3 -c "import json,sys; print(json.load(sys.stdin)['RedshiftIdcApplication']['IdcManagedApplicationArn'])")
fi

echo "  Redshift App ARN: $REDSHIFT_IDC_APP_ARN"
echo "  IdC Managed ARN:  $IDC_MANAGED_APP_ARN"

###############################################################################
# Enable Redshift Connect service integration (requires boto3)
# NOTE: CLI v2.27 doesn't support the 'Redshift' service integration param.
###############################################################################
log "Enabling Redshift Connect service integration"

python3 -c "
import boto3, sys
session = boto3.Session(region_name='${REGION}')
client = session.client('redshift')
try:
    resp = client.modify_redshift_idc_application(
        RedshiftIdcApplicationArn='${REDSHIFT_IDC_APP_ARN}',
        ServiceIntegrations=[{'Redshift': [{'Connect': {'Authorization': 'Enabled'}}]}]
    )
    integrations = resp.get('RedshiftIdcApplication', {}).get('ServiceIntegrations', [])
    print(f'  ✅ Service integrations: {integrations}')
except Exception as e:
    print(f'  ⚠️  Could not enable via boto3: {e}')
    print('  You may need to enable this in the Redshift console under IdC integration.')
"

###############################################################################
# Create IdC groups and assign users
###############################################################################
log "Setting up IdC groups and user assignments"

# Using parallel arrays instead of associative arrays (bash 3.x compat)
GROUP_NAMES_LIST=()
GROUP_IDS_LIST=()
ALL_USER_IDS=()

if [ -n "$IDC_GROUPS" ]; then
  IFS=',' read -ra PAIRS <<< "$IDC_GROUPS"
  for pair in "${PAIRS[@]}"; do
    GROUP_NAME="${pair%%:*}"
    USER_EMAIL="${pair#*:}"

    # Create or find group
    EXISTING_GID=$(aws identitystore list-groups --identity-store-id "$IDENTITY_STORE_ID" \
      --filters "[{\"AttributePath\":\"DisplayName\",\"AttributeValue\":\"${GROUP_NAME}\"}]" \
      --query 'Groups[0].GroupId' --output text --region "$REGION" 2>/dev/null || echo "None")

    if [ "$EXISTING_GID" != "None" ] && [ -n "$EXISTING_GID" ]; then
      GID="$EXISTING_GID"
      echo "  Group '$GROUP_NAME' exists: $GID"
    else
      GID=$(aws identitystore create-group \
        --identity-store-id "$IDENTITY_STORE_ID" \
        --display-name "$GROUP_NAME" \
        --description "Redshift access group" \
        --query 'GroupId' --output text --region "$REGION")
      echo "  Created group '$GROUP_NAME': $GID"
    fi
    GROUP_NAMES_LIST+=("$GROUP_NAME")
    GROUP_IDS_LIST+=("$GID")

    # Find user by email
    USER_ID=$(aws identitystore list-users --identity-store-id "$IDENTITY_STORE_ID" \
      --region "$REGION" --output json 2>/dev/null | python3 -c "
import json, sys
data = json.load(sys.stdin)
for u in data.get('Users', []):
    if u.get('UserName', '').lower() == '${USER_EMAIL}'.lower():
        print(u['UserId']); sys.exit(0)
    for e in u.get('Emails', []):
        if e.get('Value', '').lower() == '${USER_EMAIL}'.lower():
            print(u['UserId']); sys.exit(0)
print('')
")

    if [ -z "$USER_ID" ]; then
      echo "  ⚠️  User '$USER_EMAIL' not found in IdC — skipping"
      continue
    fi

    ALL_USER_IDS+=("$USER_ID")

    # Add user to group
    aws identitystore create-group-membership \
      --identity-store-id "$IDENTITY_STORE_ID" \
      --group-id "$GID" \
      --member-id "UserId=$USER_ID" \
      --region "$REGION" 2>/dev/null || true
    echo "  Added '$USER_EMAIL' → '$GROUP_NAME'"
  done
else
  echo "  IDC_GROUPS not set — skipping group creation"
  echo "  Set IDC_GROUPS='GroupName:user@email,GroupName2:user2@email' to create groups"
fi

###############################################################################
# Assign groups/users to ALL THREE IdC applications
# LEARNING: Users need assignments on Redshift app, QEV2 app, AND Console TIP
# app. Missing QEV2 assignment causes "Invalid scope" error.
###############################################################################
log "Assigning users/groups to IdC applications (Redshift + QEV2 + Console TIP)"

# Discover QEV2 (sqlworkbench) and Console TIP apps
ALL_APPS=$(aws sso-admin list-applications \
  --instance-arn "$IDC_INSTANCE_ARN" \
  --region "$REGION" --output json 2>/dev/null || echo '{"Applications":[]}')

QEV2_APP_ARN=$(echo "$ALL_APPS" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for app in data.get('Applications', []):
    if 'sqlworkbench' in app.get('ApplicationProviderArn', ''):
        print(app['ApplicationArn']); break
" 2>/dev/null || echo "")

CONSOLE_TIP_APP_ARN=$(echo "$ALL_APPS" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for app in data.get('Applications', []):
    if 'trusted-identity-propagation-for-console' in app.get('ApplicationProviderArn', ''):
        print(app['ApplicationArn']); break
" 2>/dev/null || echo "")

[ -n "$QEV2_APP_ARN" ] && echo "  QEV2 App: $QEV2_APP_ARN" || echo "  ⚠️  QEV2 app not found — enable IdC from Redshift console first"
[ -n "$CONSOLE_TIP_APP_ARN" ] && echo "  Console TIP App: $CONSOLE_TIP_APP_ARN" || echo "  ⚠️  Console TIP app not found"

# Build list of all target apps
TARGET_APPS=("$IDC_MANAGED_APP_ARN")
[ -n "$QEV2_APP_ARN" ] && TARGET_APPS+=("$QEV2_APP_ARN")
[ -n "$CONSOLE_TIP_APP_ARN" ] && TARGET_APPS+=("$CONSOLE_TIP_APP_ARN")

# Assign all groups to all apps
for TARGET_APP in "${TARGET_APPS[@]}"; do
  for GID in "${GROUP_IDS_LIST[@]}"; do
    aws sso-admin create-application-assignment \
      --application-arn "$TARGET_APP" \
      --principal-id "$GID" \
      --principal-type GROUP \
      --region "$REGION" 2>/dev/null || true
  done
done

# Assign all individual users to all apps
for TARGET_APP in "${TARGET_APPS[@]}"; do
  for USERID in "${ALL_USER_IDS[@]}"; do
    aws sso-admin create-application-assignment \
      --application-arn "$TARGET_APP" \
      --principal-id "$USERID" \
      --principal-type USER \
      --region "$REGION" 2>/dev/null || true
  done
done

echo "  ✅ All groups/users assigned to ${#TARGET_APPS[@]} IdC apps"

###############################################################################
# Create Permission Set for AWS Console access
# LEARNING: Without a Permission Set + account assignment, the IdC access
# portal "Accounts" tab is empty and users can't reach the AWS Console at all.
# LEARNING: Must use FullAccess managed policies, not ReadSharing — QEV2
# needs FullAccess for initial account bootstrapping.
# LEARNING: The Redshift console calls many APIs (redshift-serverless:List*,
# cloudwatch, ec2 describe, etc.) — broad permissions prevent errors.
###############################################################################
log "Creating Permission Set for Redshift console access"

# Create or find permission set
PS_ARN=$(aws sso-admin list-permission-sets --instance-arn "$IDC_INSTANCE_ARN" \
  --region "$REGION" --output json 2>/dev/null | python3 -c "
import json, sys, subprocess
data = json.load(sys.stdin)
for ps_arn in data.get('PermissionSets', []):
    result = subprocess.run(
        ['aws', 'sso-admin', 'describe-permission-set',
         '--instance-arn', '${IDC_INSTANCE_ARN}',
         '--permission-set-arn', ps_arn,
         '--region', '${REGION}', '--output', 'json'],
        capture_output=True, text=True)
    if result.returncode == 0:
        ps = json.loads(result.stdout).get('PermissionSet', {})
        if ps.get('Name') == '${IDC_PS_NAME}':
            print(ps_arn); sys.exit(0)
print('')
" 2>/dev/null || echo "")

if [ -z "$PS_ARN" ]; then
  PS_ARN=$(aws sso-admin create-permission-set \
    --instance-arn "$IDC_INSTANCE_ARN" \
    --name "$IDC_PS_NAME" \
    --description "Redshift Query Editor v2 access with IdC authentication" \
    --session-duration "PT8H" \
    --region "$REGION" \
    --query 'PermissionSet.PermissionSetArn' --output text 2>/dev/null || echo "")
  if [ -z "$PS_ARN" ]; then
    echo "  ⚠️  Could not create permission set (may already exist)"
    PS_ARN=""
  else
    echo "  Created: $PS_ARN"
  fi
else
  echo "  Exists: $PS_ARN"
fi

if [ -n "$PS_ARN" ]; then
  # Attach managed policies
  aws sso-admin attach-managed-policy-to-permission-set \
    --instance-arn "$IDC_INSTANCE_ARN" \
    --permission-set-arn "$PS_ARN" \
    --managed-policy-arn "arn:aws:iam::aws:policy/AmazonRedshiftFullAccess" \
    --region "$REGION" 2>/dev/null || true

  aws sso-admin attach-managed-policy-to-permission-set \
    --instance-arn "$IDC_INSTANCE_ARN" \
    --permission-set-arn "$PS_ARN" \
    --managed-policy-arn "arn:aws:iam::aws:policy/AmazonRedshiftQueryEditorV2FullAccess" \
    --region "$REGION" 2>/dev/null || true

  # Inline policy for broad console access (prevents AccessDenied on various APIs)
  INLINE_POLICY='{"Version":"2012-10-17","Statement":[{"Sid":"RedshiftConsole","Effect":"Allow","Action":["redshift:*","redshift-data:*","redshift-serverless:*","sqlworkbench:*"],"Resource":"*"},{"Sid":"IdCTokenExchange","Effect":"Allow","Action":["sso:*","sso-oauth:*","sts:*"],"Resource":"*"},{"Sid":"SupportingServices","Effect":"Allow","Action":["secretsmanager:ListSecrets","secretsmanager:GetSecretValue","tag:GetResources","tag:GetTagKeys","ec2:DescribeVpcs","ec2:DescribeSubnets","ec2:DescribeSecurityGroups","ec2:DescribeAccountAttributes","cloudwatch:GetMetricData","cloudwatch:ListMetrics","logs:DescribeLogGroups","iam:ListRoles","iam:GetRole","servicequotas:GetServiceQuota"],"Resource":"*"}]}'

  aws sso-admin put-inline-policy-to-permission-set \
    --instance-arn "$IDC_INSTANCE_ARN" \
    --permission-set-arn "$PS_ARN" \
    --inline-policy "$INLINE_POLICY" \
    --region "$REGION" 2>/dev/null || true

  echo "  ✅ Permission Set policies attached"

  # Assign groups to the AWS account
  for GID in "${GROUP_IDS_LIST[@]}"; do
    aws sso-admin create-account-assignment \
      --instance-arn "$IDC_INSTANCE_ARN" \
      --target-id "$ACCOUNT_ID" \
      --target-type AWS_ACCOUNT \
      --permission-set-arn "$PS_ARN" \
      --principal-type GROUP \
      --principal-id "$GID" \
      --region "$REGION" 2>/dev/null || true
  done

  # Provision to push changes
  aws sso-admin provision-permission-set \
    --instance-arn "$IDC_INSTANCE_ARN" \
    --permission-set-arn "$PS_ARN" \
    --target-type AWS_ACCOUNT \
    --target-id "$ACCOUNT_ID" \
    --region "$REGION" > /dev/null 2>&1 || true

  echo "  ✅ Permission Set assigned to account $ACCOUNT_ID"
fi

###############################################################################
# Register identity provider in Redshift
# LEARNING: Must use IdcManagedApplicationArn (SSO app ARN), NOT the Redshift
# IdC application ARN. Using the wrong one gives "Failed to obtain AWS IdC Info".
###############################################################################
log "Registering identity provider in Redshift"

STMT=$(aws redshift-data execute-statement \
  --cluster-identifier "$CLUSTER_ID" --database "$REDSHIFT_DB" --db-user "$REDSHIFT_USER" \
  --sql "SELECT name FROM svv_identity_providers WHERE type = 'awsidc';" \
  --region "$REGION" --query 'Id' --output text)
sleep 6
EXISTING_IDP=$(aws redshift-data get-statement-result --id "$STMT" \
  --region "$REGION" --query 'Records[0][0].stringValue' --output text 2>/dev/null || echo "None")

if [ "$EXISTING_IDP" != "None" ] && [ -n "$EXISTING_IDP" ]; then
  echo "  Identity provider '$EXISTING_IDP' already registered"
else
  STMT=$(aws redshift-data execute-statement \
    --cluster-identifier "$CLUSTER_ID" --database "$REDSHIFT_DB" --db-user "$REDSHIFT_USER" \
    --sql "CREATE IDENTITY PROVIDER \"${IDC_APP_NAME}\" TYPE AWSIDC NAMESPACE '${IDC_NAMESPACE}' APPLICATION_ARN '${IDC_MANAGED_APP_ARN}' IAM_ROLE '${IDC_ROLE_ARN}';" \
    --region "$REGION" --query 'Id' --output text)
  sleep 10
  IDP_STATUS=$(aws redshift-data describe-statement --id "$STMT" \
    --region "$REGION" --query 'Status' --output text)
  if [ "$IDP_STATUS" = "FINISHED" ]; then
    echo "  ✅ Identity provider registered (namespace: $IDC_NAMESPACE)"
  else
    IDP_ERROR=$(aws redshift-data describe-statement --id "$STMT" \
      --region "$REGION" --query 'Error' --output text)
    echo "  ❌ Failed: $IDP_ERROR"
    exit 1
  fi
fi

###############################################################################
# Create Redshift roles and grant TABLE-LEVEL permissions
# LEARNING: Grant per-table, not per-schema. Users only see tables they have
# SELECT on in QEV2. This provides proper data isolation between groups.
#
# IDC_GROUP_GRANTS format: "Group:LEVEL:table1;table2,Group2:LEVEL:table3"
#   - LEVEL is SELECT or ALL
#   - tables separated by semicolons (;) within a group
#   - use * for all tables in public schema
#   e.g. "DataAnalysts:SELECT:employees,DataEngineers:ALL:departments"
###############################################################################
log "Creating Redshift roles and granting table-level permissions"

# Parse IDC_GROUP_GRANTS into parallel arrays (bash 3.x compat)
GRANT_GROUP_NAMES=()
GRANT_LEVELS=()
GRANT_TABLES_LIST=()

if [ -n "$IDC_GROUP_GRANTS" ]; then
  IFS=',' read -ra GPAIRS <<< "$IDC_GROUP_GRANTS"
  for gpair in "${GPAIRS[@]}"; do
    GNAME=$(echo "$gpair" | cut -d: -f1)
    GLEVEL=$(echo "$gpair" | cut -d: -f2)
    GTABLES=$(echo "$gpair" | cut -d: -f3-)
    GRANT_GROUP_NAMES+=("$GNAME")
    GRANT_LEVELS+=("$GLEVEL")
    GRANT_TABLES_LIST+=("${GTABLES:-*}")
  done
fi

# Helper: look up grant level for a group name
get_grant_level() {
  local name="$1"
  for i in "${!GRANT_GROUP_NAMES[@]}"; do
    if [ "${GRANT_GROUP_NAMES[$i]}" = "$name" ]; then
      echo "${GRANT_LEVELS[$i]}"; return
    fi
  done
  echo "SELECT"
}

get_grant_tables() {
  local name="$1"
  for i in "${!GRANT_GROUP_NAMES[@]}"; do
    if [ "${GRANT_GROUP_NAMES[$i]}" = "$name" ]; then
      echo "${GRANT_TABLES_LIST[$i]}"; return
    fi
  done
  echo "*"
}

# Collect all group names
ALL_GROUPS=()
if [ ${#GROUP_NAMES_LIST[@]} -gt 0 ]; then
  ALL_GROUPS=("${GROUP_NAMES_LIST[@]}")
elif [ -n "$IDC_GROUPS" ]; then
  IFS=',' read -ra PAIRS <<< "$IDC_GROUPS"
  for pair in "${PAIRS[@]}"; do
    ALL_GROUPS+=("${pair%%:*}")
  done
fi

for GROUP_NAME in "${ALL_GROUPS[@]}"; do
  ROLE_NAME="${IDC_NAMESPACE}:${GROUP_NAME}"
  GRANT_LEVEL=$(get_grant_level "$GROUP_NAME")
  GRANT_TABLES=$(get_grant_tables "$GROUP_NAME")

  # Create role
  STMT=$(aws redshift-data execute-statement \
    --cluster-identifier "$CLUSTER_ID" --database "$REDSHIFT_DB" --db-user "$REDSHIFT_USER" \
    --sql "CREATE ROLE \"${ROLE_NAME}\";" \
    --region "$REGION" --query 'Id' --output text)
  sleep 5
  RSTATUS=$(aws redshift-data describe-statement --id "$STMT" \
    --region "$REGION" --query 'Status' --output text)
  if [ "$RSTATUS" = "FINISHED" ]; then
    echo "  Created role: $ROLE_NAME"
  else
    echo "  Role $ROLE_NAME may already exist (continuing)"
  fi

  # Always grant USAGE on schema
  GRANT_SQL="GRANT USAGE ON SCHEMA public TO ROLE \"${ROLE_NAME}\";"

  if [ "$GRANT_TABLES" = "*" ]; then
    # All tables in schema
    if [ "$GRANT_LEVEL" = "ALL" ]; then
      GRANT_SQL="$GRANT_SQL GRANT ALL ON ALL TABLES IN SCHEMA public TO ROLE \"${ROLE_NAME}\";"
    else
      GRANT_SQL="$GRANT_SQL GRANT SELECT ON ALL TABLES IN SCHEMA public TO ROLE \"${ROLE_NAME}\";"
    fi
    echo "  Granting $GRANT_LEVEL on public.* → $ROLE_NAME"
  else
    # Table-level grants
    IFS=';' read -ra TABLES <<< "$GRANT_TABLES"
    for TBL in "${TABLES[@]}"; do
      TBL=$(echo "$TBL" | xargs)  # trim whitespace
      if [ "$GRANT_LEVEL" = "ALL" ]; then
        GRANT_SQL="$GRANT_SQL GRANT ALL ON public.${TBL} TO ROLE \"${ROLE_NAME}\";"
      else
        GRANT_SQL="$GRANT_SQL GRANT SELECT ON public.${TBL} TO ROLE \"${ROLE_NAME}\";"
      fi
      echo "  Granting $GRANT_LEVEL on public.${TBL} → $ROLE_NAME"
    done
  fi

  STMT=$(aws redshift-data execute-statement \
    --cluster-identifier "$CLUSTER_ID" --database "$REDSHIFT_DB" --db-user "$REDSHIFT_USER" \
    --sql "$GRANT_SQL" \
    --region "$REGION" --query 'Id' --output text)
  sleep 5
  GSTATUS=$(aws redshift-data describe-statement --id "$STMT" \
    --region "$REGION" --query 'Status' --output text)
  if [ "$GSTATUS" != "FINISHED" ]; then
    GERR=$(aws redshift-data describe-statement --id "$STMT" \
      --region "$REGION" --query 'Error' --output text)
    echo "  ⚠️  Grant status: $GSTATUS — $GERR"
  fi
done

###############################################################################
log "🎉 IDC ↔ REDSHIFT INTEGRATION COMPLETE"
echo ""
echo "  Cluster:          $CLUSTER_ID"
echo "  IdC App:          $IDC_APP_NAME ($REDSHIFT_IDC_APP_ARN)"
echo "  Namespace:        $IDC_NAMESPACE"
echo "  IAM Role:         $IDC_ROLE_ARN"
echo "  IdC Managed App:  $IDC_MANAGED_APP_ARN"
echo "  Permission Set:   ${IDC_PS_NAME} (${PS_ARN:-not created})"
echo ""
echo "  Groups & Roles:"
for GROUP_NAME in "${ALL_GROUPS[@]}"; do
  GRANT_LEVEL=$(get_grant_level "$GROUP_NAME")
  GRANT_TABLES=$(get_grant_tables "$GROUP_NAME")
  echo "    ${IDC_NAMESPACE}:${GROUP_NAME} → ${GRANT_LEVEL} on ${GRANT_TABLES}"
done
echo ""
echo "  IdC App Assignments:"
echo "    Redshift app:    ✅"
[ -n "$QEV2_APP_ARN" ] && echo "    QEV2 app:        ✅" || echo "    QEV2 app:        ⚠️  not found"
[ -n "$CONSOLE_TIP_APP_ARN" ] && echo "    Console TIP app: ✅" || echo "    Console TIP app: ⚠️  not found"
echo ""
echo "  Access flow:"
echo "    1. User logs into IdC access portal"
echo "    2. Clicks account → ${IDC_PS_NAME} role"
echo "    3. Opens Redshift Query Editor v2 (must be in ${REGION})"
echo "    4. Connects with 'IAM Identity Center' authentication"
echo "    5. Sees only tables granted to their group's Redshift role"
echo ""
echo "  Troubleshooting:"
echo "    - 'Invalid scope' → check QEV2 + Console TIP app assignments"
echo "    - 'Session expired' → right-click cluster in QEV2 → Refresh"
echo "    - 'User info not retrieved' → check you're in the right region (${REGION})"
echo "    - Cookie errors → allow third-party cookies for amazonaws.com"
