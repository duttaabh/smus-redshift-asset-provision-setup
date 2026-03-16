#!/bin/bash
set -euo pipefail
###############################################################################
# Tear down existing Redshift resources in the admin project
#
# Usage: ./02-cleanup.sh [--include-idc] [--include-tip]
#   or:  CLUSTER_ID=my-cluster DOMAIN_ID=dzd-xxx ... ./02-cleanup.sh
#
# Flags:
#   --include-idc   Also tear down IdC integration resources created by
#                   01-setup-idc-redshift.sh (IAM role, Redshift IdC app,
#                   Permission Set, identity provider, Redshift roles, etc.)
#   --include-tip   Also tear down TIP project resources created by
#                   04-setup-tip-project.sh (TIP Redshift users, grants,
#                   IAM policies, connections, project)
#
# KEY LEARNINGS — DELETION ORDER MATTERS:
#   - DataZone connections reference Secrets Manager secrets. If you
#     force-delete the secret before the connection is fully deleted,
#     the connection enters DELETE_FAILED and gets stuck.
#   - Data sources cannot be deleted while they still contain assets
#     in inventory. You must delete the assets first.
#   - Correct deletion order:
#       1. Delete DataZone assets (from data source import runs)
#       2. Delete DataZone data sources (now empty)
#       3. Wait for data sources to finish deleting
#       4. Delete DataZone connections
#       5. Wait for connections to finish deleting
#       6. Delete Secrets Manager secrets (now unreferenced)
#   --all           Shorthand for --include-idc --include-tip
#
# Required: CLUSTER_ID, DOMAIN_ID, ADMIN_PROJECT_ID, REDSHIFT_DB
###############################################################################

INCLUDE_IDC=false
INCLUDE_TIP=false
for arg in "$@"; do
  case "$arg" in
    --include-idc) INCLUDE_IDC=true ;;
    --include-tip) INCLUDE_TIP=true ;;
    --all) INCLUDE_IDC=true; INCLUDE_TIP=true ;;
    --help|-h)
      echo "Usage: $0 [--include-idc] [--include-tip] [--all]"
      echo ""
      echo "Deletes DataZone data sources, connections, Glue catalogs/databases,"
      echo "and Secrets Manager secrets for the configured Redshift cluster."
      echo ""
      echo "Flags:"
      echo "  --include-idc   Also remove IdC integration resources:"
      echo "                  Redshift identity provider, IdC roles & grants,"
      echo "                  Redshift IdC application, Permission Set,"
      echo "                  IdC app assignments, IAM role"
      echo "  --include-tip   Also remove TIP project resources:"
      echo "                  TIP Redshift users & grants, IAM policies,"
      echo "                  Redshift connection, Lake Formation admin,"
      echo "                  project memberships, DataZone project"
      echo "  --all           Shorthand for --include-idc --include-tip"
      echo ""
      echo "Configure via .env file or environment variables. See config.sh."
      exit 0
      ;;
  esac
done

source "$(dirname "$0")/config.sh"

###############################################################################
# Step 1: Delete DataZone assets from Redshift data source imports
###############################################################################
log "Deleting DataZone Redshift assets"

DZ_DATASOURCES=$(aws datazone list-data-sources \
  --domain-identifier "$DOMAIN_ID" \
  --project-identifier "$ADMIN_PROJECT_ID" \
  --region "$REGION" \
  --query 'items[?type==`REDSHIFT`].dataSourceId' --output text 2>/dev/null || echo "")

for ds_id in $DZ_DATASOURCES; do
  # Find import runs for this data source and delete their assets
  RUN_IDS=$(aws datazone list-data-source-runs \
    --domain-identifier "$DOMAIN_ID" --data-source-identifier "$ds_id" \
    --region "$REGION" --query 'items[*].id' --output text 2>/dev/null || echo "")

  for run_id in $RUN_IDS; do
    ASSET_IDS=$(aws datazone list-data-source-run-activities \
      --domain-identifier "$DOMAIN_ID" --identifier "$run_id" --region "$REGION" \
      --query 'items[*].dataAssetActivity.dataAssetId' --output text 2>/dev/null || echo "")
    for asset_id in $ASSET_IDS; do
      [ "$asset_id" = "None" ] && continue
      echo "  Deleting asset: $asset_id"
      aws datazone delete-asset --domain-identifier "$DOMAIN_ID" --identifier "$asset_id" \
        --region "$REGION" 2>/dev/null || true
    done
  done
done

# Also search for any remaining Redshift assets in the project
REMAINING_ASSETS=$(aws datazone search --domain-identifier "$DOMAIN_ID" \
  --owning-project-identifier "$ADMIN_PROJECT_ID" --search-scope ASSET \
  --region "$REGION" \
  --query 'items[?assetItem.typeIdentifier==`amazon.datazone.RedshiftTableAssetType`].assetItem.identifier' \
  --output text 2>/dev/null || echo "")

for asset_id in $REMAINING_ASSETS; do
  echo "  Deleting remaining asset: $asset_id"
  aws datazone delete-asset --domain-identifier "$DOMAIN_ID" --identifier "$asset_id" \
    --region "$REGION" 2>/dev/null || true
done

echo "  ✅ Assets deleted"

###############################################################################
# Step 2: Delete DataZone data sources (now empty)
###############################################################################
log "Deleting DataZone data sources (Redshift)"

for ds_id in $DZ_DATASOURCES; do
  echo "  Deleting data source: $ds_id"
  aws datazone delete-data-source \
    --domain-identifier "$DOMAIN_ID" --identifier "$ds_id" \
    --region "$REGION" 2>/dev/null || true
done

# Wait for data sources to finish deleting
for ds_id in $DZ_DATASOURCES; do
  for attempt in $(seq 1 20); do
    DS_STATUS=$(aws datazone get-data-source --domain-identifier "$DOMAIN_ID" \
      --identifier "$ds_id" --region "$REGION" --query 'status' --output text 2>&1 || echo "GONE")
    if echo "$DS_STATUS" | grep -qi "ResourceNotFoundException\|GONE"; then break; fi
    if [ "$DS_STATUS" = "DELETING" ]; then
      echo "  ⏳ Data source $ds_id still deleting (attempt $attempt/20)..."
      sleep 5
    else
      break
    fi
  done
done

echo "  ✅ Data sources deleted"

###############################################################################
# Step 3: Delete DataZone Redshift connections
###############################################################################
log "Deleting DataZone Redshift connections"

DZ_CONNECTIONS=$(aws datazone list-connections \
  --domain-identifier "$DOMAIN_ID" \
  --project-identifier "$ADMIN_PROJECT_ID" \
  --region "$REGION" \
  --query 'items[?type==`REDSHIFT`].connectionId' --output text 2>/dev/null || echo "")

for conn_id in $DZ_CONNECTIONS; do
  echo "  Deleting connection: $conn_id"
  aws datazone delete-connection \
    --domain-identifier "$DOMAIN_ID" --identifier "$conn_id" \
    --region "$REGION" 2>/dev/null || true
done

# Wait for connections to finish deleting
for conn_id in $DZ_CONNECTIONS; do
  for attempt in $(seq 1 30); do
    CONN_STATUS=$(aws datazone get-connection --domain-identifier "$DOMAIN_ID" \
      --identifier "$conn_id" --region "$REGION" \
      --query 'props.redshiftProperties.status' --output text 2>&1 || echo "GONE")
    if echo "$CONN_STATUS" | grep -qi "ResourceNotFoundException\|GONE"; then break; fi
    if [ "$CONN_STATUS" = "DELETING" ]; then
      echo "  ⏳ Connection $conn_id still deleting (attempt $attempt/30)..."
      sleep 5
    elif [ "$CONN_STATUS" = "DELETE_FAILED" ]; then
      echo "  ⚠️  Connection $conn_id DELETE_FAILED — retrying..."
      aws datazone delete-connection --domain-identifier "$DOMAIN_ID" --identifier "$conn_id" \
        --region "$REGION" 2>/dev/null || true
      sleep 5
    else
      break
    fi
  done
done

echo "  ✅ Connections deleted"

###############################################################################
# Step 4: Delete Glue federated catalogs and databases
###############################################################################
log "Deleting Glue federated catalogs"

aws glue delete-database \
  --catalog-id "${ACCOUNT_ID}:${CLUSTER_ID}/${REDSHIFT_DB}" \
  --name "public" --region "$REGION" 2>/dev/null || true
aws glue delete-catalog \
  --catalog-id "${ACCOUNT_ID}:${CLUSTER_ID}/${REDSHIFT_DB}" \
  --region "$REGION" 2>/dev/null || true
aws glue delete-catalog \
  --catalog-id "${ACCOUNT_ID}:${CLUSTER_ID}" \
  --region "$REGION" 2>/dev/null || true

log "Deleting Glue connection and database"
aws glue delete-connection \
  --connection-name "${CLUSTER_ID}-connection" \
  --region "$REGION" 2>/dev/null || true
GLUE_DB_NAME="smus_redshift_${CLUSTER_ID//-/_}"
aws glue delete-database \
  --name "$GLUE_DB_NAME" --region "$REGION" 2>/dev/null || true

###############################################################################
# Step 5: Delete Secrets Manager secrets (connections are gone, safe to delete)
###############################################################################
log "Deleting Secrets Manager secrets matching *${CLUSTER_ID}*"

OLD_SECRETS=$(aws secretsmanager list-secrets --region "$REGION" \
  --query "SecretList[?contains(Name,'${CLUSTER_ID}')].Name" --output text 2>/dev/null || echo "")

for secret in $OLD_SECRETS; do
  echo "  Deleting: $secret"
  aws secretsmanager delete-secret --secret-id "$secret" \
    --force-delete-without-recovery --region "$REGION" 2>/dev/null || true
done

log "✅ Cleanup complete"

###############################################################################
# IdC cleanup (only when --include-idc is passed)
###############################################################################
if [ "$INCLUDE_IDC" = true ]; then

  IDC_NAMESPACE="${IDC_NAMESPACE:-AWSIDC}"
  IDC_ROLE_NAME="${IDC_ROLE_NAME:-RedshiftIdCIntegrationRole}"
  IDC_APP_NAME="${IDC_APP_NAME:-${CLUSTER_ID}-idc}"
  IDC_GROUPS="${IDC_GROUPS:-}"
  IDC_PS_NAME="${IDC_PS_NAME:-RedshiftQueryAccess}"

  # Discover IdC instance
  IDC_INSTANCE_ARN=$(aws sso-admin list-instances --query 'Instances[0].InstanceArn' --output text --region "$REGION" 2>/dev/null || echo "")
  IDENTITY_STORE_ID=$(aws sso-admin list-instances --query 'Instances[0].IdentityStoreId' --output text --region "$REGION" 2>/dev/null || echo "")

  if [ -z "$IDC_INSTANCE_ARN" ] || [ "$IDC_INSTANCE_ARN" = "None" ]; then
    echo "  ⚠️  No IdC instance found — skipping IdC cleanup"
  else

    log "Removing Redshift identity provider and roles"

    # Drop Redshift roles created for IdC groups
    if [ -n "$IDC_GROUPS" ]; then
      IFS=',' read -ra PAIRS <<< "$IDC_GROUPS"
      for pair in "${PAIRS[@]}"; do
        GROUP_NAME="${pair%%:*}"
        ROLE_NAME="${IDC_NAMESPACE}:${GROUP_NAME}"
        echo "  Dropping role: $ROLE_NAME"
        STMT=$(aws redshift-data execute-statement \
          --cluster-identifier "$CLUSTER_ID" --database "$REDSHIFT_DB" --db-user "$REDSHIFT_USER" \
          --sql "DROP ROLE IF EXISTS \"${ROLE_NAME}\";" \
          --region "$REGION" --query 'Id' --output text 2>/dev/null || echo "")
        [ -n "$STMT" ] && sleep 3
      done
    fi

    # Drop identity provider
    echo "  Dropping identity provider"
    STMT=$(aws redshift-data execute-statement \
      --cluster-identifier "$CLUSTER_ID" --database "$REDSHIFT_DB" --db-user "$REDSHIFT_USER" \
      --sql "DROP IDENTITY PROVIDER IF EXISTS \"${IDC_APP_NAME}\";" \
      --region "$REGION" --query 'Id' --output text 2>/dev/null || echo "")
    [ -n "$STMT" ] && sleep 5

    log "Deleting Permission Set '${IDC_PS_NAME}'"

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

    if [ -n "$PS_ARN" ]; then
      # Remove account assignments for all IdC groups before deleting PS
      if [ -n "$IDC_GROUPS" ]; then
        IFS=',' read -ra PAIRS <<< "$IDC_GROUPS"
        for pair in "${PAIRS[@]}"; do
          GROUP_NAME="${pair%%:*}"
          GID=$(aws identitystore list-groups --identity-store-id "$IDENTITY_STORE_ID" \
            --filters "[{\"AttributePath\":\"DisplayName\",\"AttributeValue\":\"${GROUP_NAME}\"}]" \
            --query 'Groups[0].GroupId' --output text --region "$REGION" 2>/dev/null || echo "None")
          if [ "$GID" != "None" ] && [ -n "$GID" ]; then
            aws sso-admin delete-account-assignment \
              --instance-arn "$IDC_INSTANCE_ARN" \
              --target-id "$ACCOUNT_ID" --target-type AWS_ACCOUNT \
              --permission-set-arn "$PS_ARN" \
              --principal-type GROUP --principal-id "$GID" \
              --region "$REGION" 2>/dev/null || true
            echo "  Removed account assignment for $GROUP_NAME"
          fi
        done
      fi

      # Detach managed policies
      aws sso-admin detach-managed-policy-from-permission-set \
        --instance-arn "$IDC_INSTANCE_ARN" --permission-set-arn "$PS_ARN" \
        --managed-policy-arn "arn:aws:iam::aws:policy/AmazonRedshiftFullAccess" \
        --region "$REGION" 2>/dev/null || true
      aws sso-admin detach-managed-policy-from-permission-set \
        --instance-arn "$IDC_INSTANCE_ARN" --permission-set-arn "$PS_ARN" \
        --managed-policy-arn "arn:aws:iam::aws:policy/AmazonRedshiftQueryEditorV2FullAccess" \
        --region "$REGION" 2>/dev/null || true

      # Remove inline policy
      aws sso-admin delete-inline-policy-from-permission-set \
        --instance-arn "$IDC_INSTANCE_ARN" --permission-set-arn "$PS_ARN" \
        --region "$REGION" 2>/dev/null || true

      # Delete permission set
      aws sso-admin delete-permission-set \
        --instance-arn "$IDC_INSTANCE_ARN" --permission-set-arn "$PS_ARN" \
        --region "$REGION" 2>/dev/null || true
      echo "  ✅ Permission Set deleted"
    else
      echo "  Permission Set not found — skipping"
    fi

    log "Removing IdC app assignments"

    # Discover all IdC apps (Redshift managed app, QEV2, Console TIP)
    EXISTING_APP=$(aws redshift describe-redshift-idc-applications \
      --region "$REGION" --output json \
      --query "RedshiftIdcApplications[?RedshiftIdcApplicationName=='${IDC_APP_NAME}']" 2>/dev/null || echo "[]")

    IDC_MANAGED_APP_ARN=$(echo "$EXISTING_APP" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d[0]['IdcManagedApplicationArn'] if d else '')" 2>/dev/null || echo "")

    ALL_APPS=$(aws sso-admin list-applications \
      --instance-arn "$IDC_INSTANCE_ARN" \
      --region "$REGION" --output json 2>/dev/null || echo '{"Applications":[]}')

    QEV2_APP_ARN=$(echo "$ALL_APPS" | python3 -c "
import json, sys
for app in json.load(sys.stdin).get('Applications', []):
    if 'sqlworkbench' in app.get('ApplicationProviderArn', ''):
        print(app['ApplicationArn']); break
" 2>/dev/null || echo "")

    CONSOLE_TIP_APP_ARN=$(echo "$ALL_APPS" | python3 -c "
import json, sys
for app in json.load(sys.stdin).get('Applications', []):
    if 'trusted-identity-propagation-for-console' in app.get('ApplicationProviderArn', ''):
        print(app['ApplicationArn']); break
" 2>/dev/null || echo "")

    TARGET_APPS=()
    [ -n "$IDC_MANAGED_APP_ARN" ] && TARGET_APPS+=("$IDC_MANAGED_APP_ARN")
    [ -n "$QEV2_APP_ARN" ] && TARGET_APPS+=("$QEV2_APP_ARN")
    [ -n "$CONSOLE_TIP_APP_ARN" ] && TARGET_APPS+=("$CONSOLE_TIP_APP_ARN")

    if [ -n "$IDC_GROUPS" ]; then
      IFS=',' read -ra PAIRS <<< "$IDC_GROUPS"
      for pair in "${PAIRS[@]}"; do
        GROUP_NAME="${pair%%:*}"
        USER_EMAIL="${pair#*:}"

        GID=$(aws identitystore list-groups --identity-store-id "$IDENTITY_STORE_ID" \
          --filters "[{\"AttributePath\":\"DisplayName\",\"AttributeValue\":\"${GROUP_NAME}\"}]" \
          --query 'Groups[0].GroupId' --output text --region "$REGION" 2>/dev/null || echo "None")

        USER_ID=$(aws identitystore list-users --identity-store-id "$IDENTITY_STORE_ID" \
          --region "$REGION" --output json 2>/dev/null | python3 -c "
import json, sys
for u in json.load(sys.stdin).get('Users', []):
    if u.get('UserName', '').lower() == '${USER_EMAIL}'.lower():
        print(u['UserId']); sys.exit(0)
print('')
" 2>/dev/null || echo "")

        for TARGET_APP in "${TARGET_APPS[@]}"; do
          if [ "$GID" != "None" ] && [ -n "$GID" ]; then
            aws sso-admin delete-application-assignment \
              --application-arn "$TARGET_APP" --principal-id "$GID" --principal-type GROUP \
              --region "$REGION" 2>/dev/null || true
          fi
          if [ -n "$USER_ID" ]; then
            aws sso-admin delete-application-assignment \
              --application-arn "$TARGET_APP" --principal-id "$USER_ID" --principal-type USER \
              --region "$REGION" 2>/dev/null || true
          fi
        done
        echo "  Removed assignments for $GROUP_NAME / $USER_EMAIL"
      done
    fi

    echo "  ✅ App assignments removed"

    log "Deleting Redshift IdC application"

    REDSHIFT_IDC_APP_ARN=$(echo "$EXISTING_APP" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d[0]['RedshiftIdcApplicationArn'] if d else '')" 2>/dev/null || echo "")

    if [ -n "$REDSHIFT_IDC_APP_ARN" ]; then
      aws redshift delete-redshift-idc-application \
        --redshift-idc-application-arn "$REDSHIFT_IDC_APP_ARN" \
        --region "$REGION" 2>/dev/null || true
      echo "  ✅ Redshift IdC application deleted"
    else
      echo "  Not found — skipping"
    fi

    log "Detaching and deleting IAM role '${IDC_ROLE_NAME}'"

    IDC_ROLE_ARN=$(aws iam get-role --role-name "$IDC_ROLE_NAME" --query 'Role.Arn' --output text 2>/dev/null || echo "")

    if [ -n "$IDC_ROLE_ARN" ]; then
      # Detach from Redshift cluster
      aws redshift modify-cluster-iam-roles \
        --cluster-identifier "$CLUSTER_ID" \
        --remove-iam-roles "$IDC_ROLE_ARN" \
        --region "$REGION" 2>/dev/null || true
      echo "  Detached from cluster"

      # Delete inline policy
      aws iam delete-role-policy --role-name "$IDC_ROLE_NAME" --policy-name IdCAccess 2>/dev/null || true

      # Delete role
      aws iam delete-role --role-name "$IDC_ROLE_NAME" 2>/dev/null || true
      echo "  ✅ IAM role deleted"
    else
      echo "  Role not found — skipping"
    fi

    log "✅ IdC cleanup complete"
  fi
fi

###############################################################################
# TIP project cleanup (only when --include-tip is passed)
###############################################################################
if [ "$INCLUDE_TIP" = true ]; then

  TIP_PROJECT_NAME="${TIP_PROJECT_NAME:-tip-redshift-project}"
  IDC_GROUPS="${IDC_GROUPS:-}"

  # Discover IdC instance (reuse if already discovered above)
  if [ -z "${IDENTITY_STORE_ID:-}" ]; then
    IDC_INSTANCE_ARN=$(aws sso-admin list-instances --query 'Instances[0].InstanceArn' --output text --region "$REGION" 2>/dev/null || echo "")
    IDENTITY_STORE_ID=$(aws sso-admin list-instances --query 'Instances[0].IdentityStoreId' --output text --region "$REGION" 2>/dev/null || echo "")
  fi

  # Find the TIP project
  TIP_PROJECT_ID=$(aws datazone list-projects \
    --domain-identifier "$DOMAIN_ID" \
    --region "$REGION" --output json 2>/dev/null | python3 -c "
import json, sys
data = json.load(sys.stdin)
for p in data.get('items', []):
    if p['name'] == '${TIP_PROJECT_NAME}':
        print(p['id']); break
" 2>/dev/null || echo "")

  if [ -z "$TIP_PROJECT_ID" ]; then
    echo "  ⚠️  TIP project '${TIP_PROJECT_NAME}' not found — skipping TIP cleanup"
  else
    log "Cleaning up TIP project '${TIP_PROJECT_NAME}' ($TIP_PROJECT_ID)"

    # Discover TIP project environments
    TIP_ENVS_JSON=$(aws datazone list-environments \
      --domain-identifier "$DOMAIN_ID" \
      --project-identifier "$TIP_PROJECT_ID" \
      --region "$REGION" --output json 2>/dev/null || echo '{"items":[]}')

    TOOLING_ENV_ID=$(echo "$TIP_ENVS_JSON" | python3 -c "
import json, sys
for e in json.load(sys.stdin).get('items', []):
    if 'Tooling' in e.get('name', '') or 'tooling' in e.get('name', '').lower():
        print(e['id']); break
" 2>/dev/null || echo "")

    TIP_USER_ROLE="datazone_usr_role_${TIP_PROJECT_ID}_${TOOLING_ENV_ID}"

    # --- Drop TIP Redshift users and revoke grants ---
    log "Dropping TIP Redshift users"

    if [ -n "$IDC_GROUPS" ] && [ -n "${IDENTITY_STORE_ID:-}" ]; then
      IFS=',' read -ra PAIRS <<< "$IDC_GROUPS"
      for pair in "${PAIRS[@]}"; do
        USERS_PART="${pair#*:}"
        IFS=';' read -ra USERS <<< "$USERS_PART"
        for user_email in "${USERS[@]}"; do
          USER_ID=$(aws identitystore list-users \
            --identity-store-id "$IDENTITY_STORE_ID" \
            --filters "[{\"AttributePath\":\"UserName\",\"AttributeValue\":\"${user_email}\"}]" \
            --region "$REGION" --query 'Users[0].UserId' --output text 2>/dev/null || echo "None")

          if [ "$USER_ID" != "None" ] && [ -n "$USER_ID" ]; then
            RS_USER="IAMR:user-${USER_ID}@${TIP_PROJECT_ID}"
            echo "  Dropping user: $RS_USER"
            STMT=$(aws redshift-data execute-statement \
              --cluster-identifier "$CLUSTER_ID" --database "$REDSHIFT_DB" --db-user "$REDSHIFT_USER" \
              --sql "REVOKE ALL ON ALL TABLES IN SCHEMA public FROM \"${RS_USER}\"; DROP USER IF EXISTS \"${RS_USER}\";" \
              --region "$REGION" --query 'Id' --output text 2>/dev/null || echo "")
            [ -n "$STMT" ] && sleep 3
          fi
        done
      done
    fi

    # Also handle TIP_USER_GRANTS if set
    TIP_USER_GRANTS="${TIP_USER_GRANTS:-}"
    if [ -n "$TIP_USER_GRANTS" ]; then
      IFS=',' read -ra GRANT_ENTRIES <<< "$TIP_USER_GRANTS"
      for grant_entry in "${GRANT_ENTRIES[@]}"; do
        GRANT_USER_ID=$(echo "$grant_entry" | cut -d: -f1)
        RS_USER="IAMR:user-${GRANT_USER_ID}@${TIP_PROJECT_ID}"
        echo "  Dropping user: $RS_USER"
        STMT=$(aws redshift-data execute-statement \
          --cluster-identifier "$CLUSTER_ID" --database "$REDSHIFT_DB" --db-user "$REDSHIFT_USER" \
          --sql "REVOKE ALL ON ALL TABLES IN SCHEMA public FROM \"${RS_USER}\"; DROP USER IF EXISTS \"${RS_USER}\";" \
          --region "$REGION" --query 'Id' --output text 2>/dev/null || echo "")
        [ -n "$STMT" ] && sleep 3
      done
    fi

    echo "  ✅ TIP Redshift users dropped"

    # --- Remove IAM policy from project user role ---
    log "Removing IAM policies from TIP user role"

    if [ -n "$TOOLING_ENV_ID" ]; then
      aws iam delete-role-policy --role-name "$TIP_USER_ROLE" \
        --policy-name "TIPRedshiftAccess" 2>/dev/null || true
      echo "  ✅ Removed TIPRedshiftAccess policy from $TIP_USER_ROLE"

      # Remove from Lake Formation admins
      TIP_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${TIP_USER_ROLE}"
      CURRENT_ADMINS=$(aws lakeformation get-data-lake-settings \
        --region "$REGION" --output json 2>/dev/null || echo "")

      if [ -n "$CURRENT_ADMINS" ]; then
        UPDATED_SETTINGS=$(echo "$CURRENT_ADMINS" | python3 -c "
import json, sys
data = json.load(sys.stdin)
settings = data.get('DataLakeSettings', {})
admins = settings.get('DataLakeAdmins', [])
tip_arn = '${TIP_ROLE_ARN}'
admins = [a for a in admins if a.get('DataLakePrincipalIdentifier') != tip_arn]
settings['DataLakeAdmins'] = admins
settings.pop('CreateDatabaseDefaultPermissions', None)
settings.pop('CreateTableDefaultPermissions', None)
print(json.dumps(settings))
")
        aws lakeformation put-data-lake-settings \
          --data-lake-settings "$UPDATED_SETTINGS" \
          --region "$REGION" 2>/dev/null || true
        echo "  ✅ Removed $TIP_USER_ROLE from Lake Formation admins"
      fi
    fi

    # --- Delete Redshift connections in TIP project ---
    log "Deleting TIP project Redshift connections"

    if [ -n "$TOOLING_ENV_ID" ]; then
      TIP_CONNECTIONS=$(aws datazone list-connections \
        --domain-identifier "$DOMAIN_ID" \
        --project-identifier "$TIP_PROJECT_ID" \
        --environment-identifier "$TOOLING_ENV_ID" \
        --region "$REGION" \
        --query 'items[?type==`REDSHIFT`].connectionId' --output text 2>/dev/null || echo "")

      for conn_id in $TIP_CONNECTIONS; do
        echo "  Deleting connection: $conn_id"
        aws datazone delete-connection \
          --domain-identifier "$DOMAIN_ID" --identifier "$conn_id" \
          --region "$REGION" 2>/dev/null || true
      done

      # Wait for connections to finish deleting
      for conn_id in $TIP_CONNECTIONS; do
        for attempt in $(seq 1 30); do
          CONN_STATUS=$(aws datazone get-connection --domain-identifier "$DOMAIN_ID" \
            --identifier "$conn_id" --region "$REGION" \
            --query 'props.redshiftProperties.status' --output text 2>&1 || echo "GONE")
          if echo "$CONN_STATUS" | grep -qi "ResourceNotFoundException\|GONE"; then break; fi
          if [ "$CONN_STATUS" = "DELETING" ]; then
            echo "  ⏳ Connection $conn_id still deleting (attempt $attempt/30)..."
            sleep 5
          elif [ "$CONN_STATUS" = "DELETE_FAILED" ]; then
            echo "  ⚠️  Connection $conn_id DELETE_FAILED — retrying..."
            aws datazone delete-connection --domain-identifier "$DOMAIN_ID" --identifier "$conn_id" \
              --region "$REGION" 2>/dev/null || true
            sleep 5
          else
            break
          fi
        done
      done
    fi

    echo "  ✅ TIP connections deleted"

    # --- Delete the TIP project ---
    log "Deleting TIP project"

    aws datazone delete-project \
      --domain-identifier "$DOMAIN_ID" \
      --identifier "$TIP_PROJECT_ID" \
      --skip-deletion-check \
      --region "$REGION" 2>/dev/null || true

    echo "  ✅ TIP project '$TIP_PROJECT_NAME' deleted"

    log "✅ TIP cleanup complete"
  fi
fi
