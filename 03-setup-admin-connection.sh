#!/bin/bash
set -euo pipefail
###############################################################################
# Set up the admin project's Redshift connection in DataZone/SMUS
#
# Usage: ./03-setup-admin-connection.sh
#   or:  CLUSTER_ID=my-cluster DOMAIN_ID=dzd-xxx ... ./03-setup-admin-connection.sh
#
# Required: CLUSTER_ID, REDSHIFT_HOST, REDSHIFT_USER, REDSHIFT_PASSWORD,
#           DOMAIN_ID, ADMIN_PROJECT_ID, ADMIN_ENV_ID,
#           VPC_ID, SUBNET_ID, SMUS_SG, REDSHIFT_SG
###############################################################################

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  echo "Usage: $0"
  echo ""
  echo "Creates secret, SG rules, VPC endpoint, IAM policies, DataZone connection,"
  echo "data source, runs import, creates Glue database, disables auto-approve."
  echo ""
  echo "Configure via .env file or environment variables. See config.sh."
  exit 0
fi

source "$(dirname "$0")/config.sh"

# Validate params needed by this script
: "${REDSHIFT_PASSWORD:?'REDSHIFT_PASSWORD is required for admin connection setup'}"
: "${VPC_ID:?'VPC_ID is required'}"
: "${SUBNET_ID:?'SUBNET_ID is required'}"
: "${SMUS_SG:?'SMUS_SG is required (SMUS project security group)'}"
: "${REDSHIFT_SG:?'REDSHIFT_SG is required (Redshift cluster security group)'}"

TIMESTAMP=$(date +%s)
SECRET_NAME="smus-redshift-${CLUSTER_ID}-${TIMESTAMP}"
DZ_CONNECTION_NAME="${CLUSTER_ID}-${TIMESTAMP}"
DATASOURCE_NAME="redshift-${CLUSTER_ID}-public-${TIMESTAMP}"

###############################################################################
# Cleanup existing Redshift connections, data sources, and secrets
###############################################################################
log "Cleaning up existing Redshift resources in admin project"

# Always clean up orphaned Redshift assets/listings (survive connection deletion)
OLD_ASSETS=$(aws datazone search --domain-identifier "$DOMAIN_ID" \
  --owning-project-identifier "$ADMIN_PROJECT_ID" \
  --search-scope ASSET --region "$REGION" --output json 2>/dev/null | python3 -c "
import json, sys
for item in json.load(sys.stdin).get('items', []):
    at = item.get('assetItem', {})
    if 'Redshift' in at.get('typeName', ''):
        print(at.get('identifier',''), at.get('name',''))
" 2>/dev/null || echo "")

if [ -n "$OLD_ASSETS" ]; then
  echo "$OLD_ASSETS" | while read -r ASSET_ID ASSET_NAME; do
    echo "  Deleting asset: $ASSET_NAME ($ASSET_ID)"
    aws datazone delete-asset --domain-identifier "$DOMAIN_ID" \
      --identifier "$ASSET_ID" --region "$REGION" 2>/dev/null || echo "    (could not delete)"
  done
else
  echo "  (no existing Redshift assets)"
fi

EXISTING_CONNS=$(aws datazone list-connections \
  --domain-identifier "$DOMAIN_ID" --project-identifier "$ADMIN_PROJECT_ID" \
  --type REDSHIFT --region "$REGION" --output json 2>/dev/null | python3 -c "
import json, sys
for c in json.load(sys.stdin).get('items', []):
    print(c['connectionId'])
" 2>/dev/null || echo "")

if [ -n "$EXISTING_CONNS" ]; then
  # Delete Redshift data sources
  ALL_DS=$(aws datazone list-data-sources \
    --domain-identifier "$DOMAIN_ID" --project-identifier "$ADMIN_PROJECT_ID" \
    --region "$REGION" --output json 2>/dev/null | python3 -c "
import json, sys
for ds in json.load(sys.stdin).get('items', []):
    if ds.get('type') == 'REDSHIFT':
        print(ds['dataSourceId'])
" 2>/dev/null || echo "")

  for DS_DEL in $ALL_DS; do
    echo "  Deleting data source: $DS_DEL"
    aws datazone delete-data-source \
      --domain-identifier "$DOMAIN_ID" --identifier "$DS_DEL" \
      --region "$REGION" 2>/dev/null || echo "    (could not delete)"
  done

  # Now delete connections
  for CONN_DEL in $EXISTING_CONNS; do
    echo "  Deleting connection: $CONN_DEL"
    aws datazone delete-connection \
      --domain-identifier "$DOMAIN_ID" --identifier "$CONN_DEL" \
      --region "$REGION" 2>/dev/null || echo "    (could not delete)"
  done

  # Delete orphaned smus-redshift secrets (admin project only)
  ORPHAN_SECRETS=$(aws secretsmanager list-secrets \
    --filters "Key=name,Values=smus-redshift-${CLUSTER_ID}" \
    --region "$REGION" --query 'SecretList[*].Name' --output text 2>/dev/null || echo "")

  for SEC_NAME in $ORPHAN_SECRETS; do
    echo "  Deleting secret: $SEC_NAME"
    aws secretsmanager delete-secret --secret-id "$SEC_NAME" \
      --force-delete-without-recovery --region "$REGION" 2>/dev/null || echo "    (could not delete)"
  done

  echo "  ✅ Cleanup complete"
  sleep 5
else
  echo "  (no existing Redshift connections)"
fi

###############################################################################
# Secret
###############################################################################
log "Creating Secrets Manager secret"

SECRET_ARN=$(aws secretsmanager create-secret \
  --name "$SECRET_NAME" \
  --description "Redshift credentials for ${CLUSTER_ID}" \
  --secret-string "{\"username\":\"${REDSHIFT_USER}\",\"password\":\"${REDSHIFT_PASSWORD}\"}" \
  --tags "[
    {\"Key\":\"AmazonDataZoneProject\",\"Value\":\"${ADMIN_PROJECT_ID}\"},
    {\"Key\":\"AmazonDataZoneDomain\",\"Value\":\"${DOMAIN_ID}\"},
    {\"Key\":\"AmazonDataZoneEnvironment\",\"Value\":\"${ADMIN_ENV_ID}\"},
    {\"Key\":\"AmazonDataZoneCreatedVia\",\"Value\":\"SageMakerUnifiedStudio\"}
  ]" --region "$REGION" --query 'ARN' --output text)

echo "  Secret ARN: $SECRET_ARN"

aws secretsmanager put-resource-policy --secret-id "$SECRET_ARN" \
  --resource-policy "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [{
      \"Effect\": \"Allow\",
      \"Principal\": {
        \"AWS\": [\"${SAGEMAKER_MANAGE_ROLE}\",\"arn:aws:iam::${ACCOUNT_ID}:role/${DZ_USER_ROLE}\"]
      },
      \"Action\": \"secretsmanager:GetSecretValue\",
      \"Resource\": \"*\"
    }]
  }" --region "$REGION" > /dev/null

echo "  ✅ Secret created with resource policy"

###############################################################################
# Security group rules
###############################################################################
log "Ensuring security group rules"

aws ec2 authorize-security-group-ingress \
  --group-id "$REDSHIFT_SG" --protocol tcp --port "$REDSHIFT_PORT" \
  --source-group "$SMUS_SG" --region "$REGION" 2>/dev/null || echo "  (rule already exists)"

echo "  ✅ SG rules in place"

###############################################################################
# Secrets Manager VPC endpoint
###############################################################################
log "Checking Secrets Manager VPC endpoint"

SM_VPCE=$(aws ec2 describe-vpc-endpoints \
  --filters "Name=vpc-id,Values=${VPC_ID}" "Name=service-name,Values=com.amazonaws.${REGION}.secretsmanager" \
  --region "$REGION" --query 'VpcEndpoints[0].VpcEndpointId' --output text 2>/dev/null || echo "None")

if [ "$SM_VPCE" != "None" ] && [ -n "$SM_VPCE" ]; then
  EXISTING_SGS=$(aws ec2 describe-vpc-endpoints --vpc-endpoint-ids "$SM_VPCE" \
    --region "$REGION" --query 'VpcEndpoints[0].Groups[*].GroupId' --output text)
  if echo "$EXISTING_SGS" | grep -q "$SMUS_SG"; then
    echo "  SMUS SG already attached"
  else
    aws ec2 modify-vpc-endpoint --vpc-endpoint-id "$SM_VPCE" \
      --add-security-group-ids "$SMUS_SG" --region "$REGION" > /dev/null
    echo "  Added SMUS SG to endpoint"
  fi
else
  SM_VPCE=$(aws ec2 create-vpc-endpoint \
    --vpc-id "$VPC_ID" --service-name "com.amazonaws.${REGION}.secretsmanager" \
    --vpc-endpoint-type Interface --subnet-ids "$SUBNET_ID" \
    --security-group-ids "$SMUS_SG" "$REDSHIFT_SG" --private-dns-enabled \
    --region "$REGION" --query 'VpcEndpoint.VpcEndpointId' --output text)
  echo "  Created VPC endpoint: $SM_VPCE"
fi

echo "  ✅ VPC endpoint configured"

###############################################################################
# IAM permissions
###############################################################################
log "Granting IAM permissions"

aws iam put-role-policy --role-name "$DZ_USER_ROLE" \
  --policy-name "RedshiftDataSourceAccess" \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": [
        "redshift-data:ListDatabases","redshift-data:ListSchemas","redshift-data:ListTables",
        "redshift-data:DescribeTable","redshift-data:ExecuteStatement",
        "redshift-data:DescribeStatement","redshift-data:GetStatementResult",
        "redshift:GetClusterCredentials","redshift:DescribeClusters"
      ],
      "Resource": "*"
    }]
  }'

aws iam put-role-policy \
  --role-name "AmazonSageMakerManageAccess-${REGION}-${DOMAIN_ID}" \
  --policy-name "SecretsManagerRedshiftAccess" \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [{
      \"Effect\": \"Allow\",
      \"Action\": \"secretsmanager:GetSecretValue\",
      \"Resource\": \"arn:aws:secretsmanager:${REGION}:${ACCOUNT_ID}:secret:smus-redshift-*\"
    }]
  }"

echo "  ✅ IAM policies attached"
echo "  Waiting 15s for IAM propagation..."
sleep 15

###############################################################################
# DataZone Redshift connection
###############################################################################
log "Creating DataZone Redshift connection"

DZ_CONN_ID=$(aws datazone create-connection \
  --domain-identifier "$DOMAIN_ID" \
  --environment-identifier "$ADMIN_ENV_ID" \
  --name "$DZ_CONNECTION_NAME" \
  --description "Redshift cluster ${CLUSTER_ID}" \
  --props "{
    \"redshiftProperties\": {
      \"credentials\": {\"secretArn\": \"${SECRET_ARN}\"},
      \"databaseName\": \"${REDSHIFT_DB}\",
      \"host\": \"${REDSHIFT_HOST}\",
      \"port\": ${REDSHIFT_PORT},
      \"storage\": {\"clusterName\": \"${CLUSTER_ID}\"}
    }
  }" --region "$REGION" --query 'connectionId' --output text)

echo "  Connection ID: $DZ_CONN_ID"

aws secretsmanager tag-resource --secret-id "$SECRET_ARN" \
  --tags "[{\"Key\":\"AmazonDataZoneConnection\",\"Value\":\"${DZ_CONN_ID}\"}]" \
  --region "$REGION" > /dev/null

echo "  ✅ Connection created and secret tagged"

###############################################################################
# Data source + import
###############################################################################
log "Creating data source and running import"

cat > /tmp/dz-datasource.json <<EOFDS
{
  "configuration": {
    "redshiftRunConfiguration": {
      "relationalFilterConfigurations": [{
        "databaseName": "${REDSHIFT_DB}",
        "schemaName": "public",
        "filterExpressions": [{"expression": "*", "type": "INCLUDE"}]
      }]
    }
  },
  "connectionIdentifier": "${DZ_CONN_ID}",
  "domainIdentifier": "${DOMAIN_ID}",
  "projectIdentifier": "${ADMIN_PROJECT_ID}",
  "name": "${DATASOURCE_NAME}",
  "description": "Import Redshift public schema tables",
  "enableSetting": "ENABLED",
  "publishOnImport": true,
  "type": "REDSHIFT"
}
EOFDS

DS_ID=$(aws datazone create-data-source \
  --cli-input-json file:///tmp/dz-datasource.json \
  --region "$REGION" --query 'id' --output text)

echo "  Data source: $DS_ID"

wait_for \
  "aws datazone get-data-source --domain-identifier $DOMAIN_ID --identifier $DS_ID --region $REGION --query 'status' --output text" \
  "status" "READY" 20 5

RUN_ID=$(aws datazone start-data-source-run \
  --domain-identifier "$DOMAIN_ID" --data-source-identifier "$DS_ID" \
  --region "$REGION" --query 'id' --output text)

echo "  Import run: $RUN_ID"

wait_for \
  "aws datazone get-data-source-run --domain-identifier $DOMAIN_ID --identifier $RUN_ID --region $REGION --query 'status' --output text" \
  "status" "SUCCESS" 30 10

echo "  ✅ Import complete"

###############################################################################
# Disable auto-approve on imported assets
###############################################################################
log "Disabling auto-approve on imported assets"

ASSET_IDS=$(aws datazone list-data-source-run-activities \
  --domain-identifier "$DOMAIN_ID" --identifier "$RUN_ID" --region "$REGION" \
  --query 'items[?dataAssetActivity.isPublished==`true`].dataAssetActivity.dataAssetId' \
  --output text 2>/dev/null || echo "")

for ASSET_ID in $ASSET_IDS; do
  ASSET_NAME=$(aws datazone get-asset --domain-identifier "$DOMAIN_ID" --identifier "$ASSET_ID" \
    --region "$REGION" --query 'name' --output text 2>/dev/null || echo "$ASSET_ID")
  aws datazone create-asset-revision \
    --domain-identifier "$DOMAIN_ID" --identifier "$ASSET_ID" --name "$ASSET_NAME" \
    --forms-input "[{\"formName\":\"SubscriptionTermsForm\",\"content\":\"{\\\"approvalRequired\\\":\\\"YES\\\"}\"}]" \
    --region "$REGION" --output text --query 'revision' 2>/dev/null \
    && echo "  ✅ $ASSET_NAME: approval required" \
    || echo "  ⚠️  $ASSET_NAME: could not update"
done

###############################################################################
log "🎉 ADMIN CONNECTION SETUP COMPLETE"
echo ""
echo "  Cluster:       $CLUSTER_ID"
echo "  Connection:    $DZ_CONN_ID"
echo "  Data Source:   $DS_ID"
echo "  Secret:        $SECRET_ARN"
