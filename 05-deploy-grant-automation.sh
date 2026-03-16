#!/bin/bash
set -euo pipefail
###############################################################################
# Deploy EventBridge + Lambda for automatic Redshift subscription grants
#
# Usage: ./04-deploy-grant-automation.sh
#   or:  CLUSTER_ID=my-cluster DOMAIN_ID=dzd-xxx ... ./04-deploy-grant-automation.sh
#
# Required: CLUSTER_ID, REDSHIFT_USER, DOMAIN_ID, ADMIN_PROJECT_ID
###############################################################################

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  echo "Usage: $0"
  echo ""
  echo "Deploys Lambda + EventBridge rule that auto-grants Redshift SELECT"
  echo "when a DataZone subscription grant completes."
  echo ""
  echo "Configure via .env file or environment variables. See config.sh."
  exit 0
fi

source "$(dirname "$0")/config.sh"

###############################################################################
# IAM role
###############################################################################
log "Creating Lambda execution role"

TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

ROLE_ARN=$(aws iam create-role \
  --role-name "$LAMBDA_ROLE_NAME" \
  --assume-role-policy-document "$TRUST_POLICY" \
  --query 'Role.Arn' --output text 2>/dev/null || \
  aws iam get-role --role-name "$LAMBDA_ROLE_NAME" --query 'Role.Arn' --output text)

echo "  Role ARN: $ROLE_ARN"

aws iam attach-role-policy --role-name "$LAMBDA_ROLE_NAME" \
  --policy-arn "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole" 2>/dev/null || true

aws iam put-role-policy --role-name "$LAMBDA_ROLE_NAME" \
  --policy-name "RedshiftGrantAccess" \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [
      {\"Effect\":\"Allow\",\"Action\":[\"redshift-data:ExecuteStatement\",\"redshift-data:DescribeStatement\",\"redshift-data:GetStatementResult\",\"redshift:GetClusterCredentials\"],\"Resource\":\"*\"},
      {\"Effect\":\"Allow\",\"Action\":\"datazone:*\",\"Resource\":\"*\"},
      {\"Effect\":\"Allow\",\"Action\":\"sts:GetCallerIdentity\",\"Resource\":\"*\"},
      {\"Effect\":\"Allow\",\"Action\":[\"identitystore:ListGroupMembershipsForMember\",\"identitystore:DescribeGroup\",\"identitystore:ListUsers\",\"identitystore:DescribeUser\"],\"Resource\":\"*\"},
      {\"Effect\":\"Allow\",\"Action\":\"sso-admin:ListInstances\",\"Resource\":\"*\"},
      {\"Effect\":\"Allow\",\"Action\":\"sso:ListInstances\",\"Resource\":\"*\"},
      {\"Effect\":\"Allow\",\"Action\":\"sso:ListInstances\",\"Resource\":\"*\"},
      {\"Effect\":\"Allow\",\"Action\":[\"secretsmanager:GetSecretValue\",\"secretsmanager:CreateSecret\",\"secretsmanager:DeleteSecret\",\"secretsmanager:TagResource\",\"secretsmanager:PutResourcePolicy\",\"secretsmanager:ListSecrets\"],\"Resource\":\"*\"},
      {\"Effect\":\"Allow\",\"Action\":\"iam:PassRole\",\"Resource\":\"arn:aws:iam::${ACCOUNT_ID}:role/service-role/AmazonSageMakerManageAccess-${REGION}-${DOMAIN_ID}\"}
    ]
  }"

echo "  Waiting for role propagation..."
sleep 10

###############################################################################
# Tag SageMaker manage role with RedshiftDbRoles
# (required for DataZone to fulfill Redshift subscription grants)
###############################################################################
log "Tagging SageMaker manage role with RedshiftDbRoles"

SM_MANAGE_ROLE_NAME="AmazonSageMakerManageAccess-${REGION}-${DOMAIN_ID}"
EXISTING_TAG=$(aws iam list-role-tags --role-name "$SM_MANAGE_ROLE_NAME" \
  --query "Tags[?Key=='RedshiftDbRoles'].Value" --output text 2>/dev/null || echo "")

if [ -z "$EXISTING_TAG" ] || [ "$EXISTING_TAG" = "None" ]; then
  # Collect all Redshift DB roles that match our naming pattern
  DB_ROLES=$(aws redshift-data execute-statement \
    --cluster-identifier "$CLUSTER_ID" --database "$REDSHIFT_DB" --db-user "$REDSHIFT_USER" \
    --sql "SELECT role_name FROM svv_roles WHERE role_name NOT LIKE 'sys:%' AND role_name NOT LIKE 'ds:%'" \
    --region "$REGION" --query 'Id' --output text 2>/dev/null || echo "")
  if [ -n "$DB_ROLES" ]; then
    sleep 4
    ROLE_NAMES=$(aws redshift-data get-statement-result --id "$DB_ROLES" --region "$REGION" \
      --query 'Records[*][0].stringValue' --output text 2>/dev/null | tr '\t' ',' || echo "")
  fi
  ROLE_NAMES="${ROLE_NAMES:-dataengineers_consumer_role}"

  aws iam tag-role --role-name "$SM_MANAGE_ROLE_NAME" \
    --tags "[{\"Key\":\"RedshiftDbRoles\",\"Value\":\"${ROLE_NAMES}\"}]" 2>/dev/null || true
  echo "  ✅ Tagged with RedshiftDbRoles=${ROLE_NAMES}"
else
  echo "  Already tagged: RedshiftDbRoles=${EXISTING_TAG}"
fi


###############################################################################
# DataZone permissions — admin project contributor + domain root unit owner
###############################################################################
log "Adding Lambda role to DataZone"

# Add as PROJECT_CONTRIBUTOR to admin project (needed for GetAsset)
aws datazone create-project-membership \
  --domain-identifier "$DOMAIN_ID" --project-identifier "$ADMIN_PROJECT_ID" \
  --member "{\"userIdentifier\":\"${ROLE_ARN}\"}" \
  --designation "PROJECT_CONTRIBUTOR" \
  --region "$REGION" 2>/dev/null || echo "  (already a member of admin project)"

echo "  ✅ Admin project contributor"

# Add as domain root unit owner (needed for self-add to consumer projects)
ROOT_UNIT_ID=$(aws datazone get-domain --identifier "$DOMAIN_ID" --region "$REGION" \
  --query 'rootDomainUnitId' --output text 2>/dev/null || echo "")

if [ -n "$ROOT_UNIT_ID" ] && [ "$ROOT_UNIT_ID" != "None" ]; then
  aws datazone add-entity-owner \
    --domain-identifier "$DOMAIN_ID" --entity-type DOMAIN_UNIT \
    --entity-identifier "$ROOT_UNIT_ID" \
    --owner "{\"user\":{\"userIdentifier\":\"${ROLE_ARN}\"}}" \
    --region "$REGION" 2>/dev/null || echo "  (already a domain unit owner)"
  echo "  ✅ Domain root unit owner (unit: $ROOT_UNIT_ID)"
else
  echo "  ⚠️  Could not resolve root domain unit — Lambda may not be able to self-add to consumer projects"
fi

###############################################################################
# Lambda function
###############################################################################
log "Deploying Lambda function"

LAMBDA_ZIP="/tmp/${LAMBDA_NAME}.zip"
(cd "${SCRIPT_DIR}/lambda" && zip -j "$LAMBDA_ZIP" redshift_grant_handler.py > /dev/null)

EXISTING_LAMBDA=$(aws lambda get-function --function-name "$LAMBDA_NAME" \
  --region "$REGION" --query 'Configuration.FunctionArn' --output text 2>/dev/null || echo "")

if [ -n "$EXISTING_LAMBDA" ] && [ "$EXISTING_LAMBDA" != "None" ]; then
  aws lambda update-function-code \
    --function-name "$LAMBDA_NAME" --zip-file "fileb://${LAMBDA_ZIP}" \
    --region "$REGION" > /dev/null
  echo "  Waiting for code update to complete..."
  aws lambda wait function-updated --function-name "$LAMBDA_NAME" --region "$REGION" 2>/dev/null || sleep 10
  ENV_JSON=$(python3 -c "
import json, os
print(json.dumps({'Variables': {
  'REDSHIFT_CLUSTER_ID': '${CLUSTER_ID}',
  'REDSHIFT_DATABASE': '${REDSHIFT_DB}',
  'REDSHIFT_ADMIN_USER': '${REDSHIFT_USER}',
  'REDSHIFT_HOST': '${REDSHIFT_HOST}',
  'DOMAIN_ID': '${DOMAIN_ID}',
  'ADMIN_PROJECT_ID': '${ADMIN_PROJECT_ID}',
  'IDC_NAMESPACE': '${IDC_NAMESPACE:-AWSIDC}',
}}))")
  aws lambda update-function-configuration \
    --function-name "$LAMBDA_NAME" \
    --environment "$ENV_JSON" \
    --timeout 900 --region "$REGION" > /dev/null
  LAMBDA_ARN="$EXISTING_LAMBDA"
  echo "  Updated existing Lambda"
else
  ENV_JSON=$(python3 -c "
import json
print(json.dumps({'Variables': {
  'REDSHIFT_CLUSTER_ID': '${CLUSTER_ID}',
  'REDSHIFT_DATABASE': '${REDSHIFT_DB}',
  'REDSHIFT_ADMIN_USER': '${REDSHIFT_USER}',
  'REDSHIFT_HOST': '${REDSHIFT_HOST}',
  'DOMAIN_ID': '${DOMAIN_ID}',
  'ADMIN_PROJECT_ID': '${ADMIN_PROJECT_ID}',
  'IDC_NAMESPACE': '${IDC_NAMESPACE:-AWSIDC}',
}})")
  LAMBDA_ARN=$(aws lambda create-function \
    --function-name "$LAMBDA_NAME" \
    --runtime python3.12 --handler redshift_grant_handler.lambda_handler \
    --role "$ROLE_ARN" \
    --zip-file "fileb://${LAMBDA_ZIP}" \
    --timeout 900 --memory-size 128 \
    --environment "$ENV_JSON" \
    --region "$REGION" --query 'FunctionArn' --output text)
  echo "  Created Lambda: $LAMBDA_ARN"
fi

rm -f "$LAMBDA_ZIP"

###############################################################################
# EventBridge rule
###############################################################################
log "Creating EventBridge rule"

EB_RULE_ARN=$(aws events put-rule \
  --name "$EB_RULE_NAME" \
  --event-pattern "{
    \"source\": [\"aws.datazone\"],
    \"detail-type\": [\"Subscription Request Accepted\", \"Subscription Grant Completed\", \"Subscription Revoked\", \"Subscription Cancelled\", \"Subscription Grant Revoke Completed\"]
  }" \
  --state ENABLED \
  --description "Trigger Lambda on DataZone subscription grant/revoke events for Redshift access management" \
  --region "$REGION" --query 'RuleArn' --output text)

echo "  Rule ARN: $EB_RULE_ARN"

# Allow EventBridge to invoke the Lambda
aws lambda add-permission \
  --function-name "$LAMBDA_NAME" \
  --statement-id "EventBridgeInvoke" \
  --action "lambda:InvokeFunction" \
  --principal "events.amazonaws.com" \
  --source-arn "$EB_RULE_ARN" \
  --region "$REGION" 2>/dev/null || true

aws events put-targets \
  --rule "$EB_RULE_NAME" \
  --targets "[{\"Id\":\"redshift-grant-lambda\",\"Arn\":\"${LAMBDA_ARN}\"}]" \
  --region "$REGION" > /dev/null

echo "  ✅ EventBridge → Lambda wired"

###############################################################################
log "🎉 GRANT AUTOMATION DEPLOYED"
echo ""
echo "  Lambda:        $LAMBDA_NAME ($LAMBDA_ARN)"
echo "  EventBridge:   $EB_RULE_NAME"
echo "  IAM Role:      $ROLE_ARN"
echo "  Root Unit:     ${ROOT_UNIT_ID:-unknown}"
echo ""
echo "  Flow:"
echo "    1. Subscription approved → Lambda creates Redshift subscription target"
echo "       in consumer project (if missing)"
echo "    2. DataZone fulfills grant using the target"
echo "    3. Grant completed → Lambda executes Redshift GRANT statements"
echo "    4. Subscription revoked/cancelled → Lambda executes Redshift REVOKE statements"
