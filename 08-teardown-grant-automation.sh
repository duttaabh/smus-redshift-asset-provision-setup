#!/bin/bash
set -euo pipefail
###############################################################################
# Tear down the grant automation (Lambda + EventBridge + IAM role)
#
# Usage: ./08-teardown-grant-automation.sh
#   or:  DOMAIN_ID=dzd-xxx ... ./08-teardown-grant-automation.sh
#
# Required: DOMAIN_ID, ADMIN_PROJECT_ID
###############################################################################

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  echo "Usage: $0"
  echo ""
  echo "Removes the Lambda function, EventBridge rule, DataZone memberships,"
  echo "and IAM role created by 04-deploy-grant-automation.sh."
  echo ""
  echo "Configure via .env file or environment variables. See config.sh."
  exit 0
fi

source "$(dirname "$0")/config.sh"

log "Removing EventBridge targets and rule"
aws events remove-targets --rule "$EB_RULE_NAME" --ids "redshift-grant-lambda" \
  --region "$REGION" 2>/dev/null || true
aws events delete-rule --name "$EB_RULE_NAME" --region "$REGION" 2>/dev/null || true
echo "  ✅ EventBridge rule deleted"

log "Deleting Lambda function"
aws lambda delete-function --function-name "$LAMBDA_NAME" \
  --region "$REGION" 2>/dev/null || true
echo "  ✅ Lambda deleted"

log "Cleaning up temporary secrets and Redshift users"
TEMP_SECRETS=$(aws secretsmanager list-secrets \
  --filters "Key=tag-key,Values=smus-temp-resource" "Key=tag-value,Values=true" \
  --region "$REGION" --query 'SecretList[*].[Name,ARN]' --output text 2>/dev/null || echo "")

if [ -n "$TEMP_SECRETS" ]; then
  echo "$TEMP_SECRETS" | while read -r SEC_NAME SEC_ARN; do
    # Try to extract and drop the temp Redshift user
    TEMP_USER=$(aws secretsmanager get-secret-value --secret-id "$SEC_NAME" \
      --region "$REGION" --query 'SecretString' --output text 2>/dev/null | \
      python3 -c "import json,sys; print(json.load(sys.stdin).get('username',''))" 2>/dev/null || echo "")
    if [ -n "$TEMP_USER" ] && echo "$TEMP_USER" | grep -q "^temp_dz_"; then
      echo "  Revoking grants and dropping temp Redshift user: $TEMP_USER"
      aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" --database "$REDSHIFT_DB" --db-user "$REDSHIFT_USER" \
        --sql "REVOKE ALL ON ALL TABLES IN SCHEMA public FROM ${TEMP_USER}; REVOKE USAGE ON SCHEMA public FROM ${TEMP_USER}; DROP USER IF EXISTS ${TEMP_USER}" \
        --region "$REGION" > /dev/null 2>&1 || true
    fi
    echo "  Deleting temp secret: $SEC_NAME"
    aws secretsmanager delete-secret --secret-id "$SEC_ARN" \
      --force-delete-without-recovery --region "$REGION" 2>/dev/null || true
  done
  echo "  ✅ Temp secrets cleaned up"
else
  echo "  (no temp secrets found)"
fi

log "Removing Lambda role from DataZone"
ROLE_ARN=$(aws iam get-role --role-name "$LAMBDA_ROLE_NAME" \
  --query 'Role.Arn' --output text 2>/dev/null || echo "")

if [ -n "$ROLE_ARN" ]; then
  # Remove from admin project
  aws datazone delete-project-membership \
    --domain-identifier "$DOMAIN_ID" --project-identifier "$ADMIN_PROJECT_ID" \
    --member "{\"userIdentifier\":\"${ROLE_ARN}\"}" \
    --region "$REGION" 2>/dev/null || true

  # Remove from domain root unit
  ROOT_UNIT_ID=$(aws datazone get-domain --identifier "$DOMAIN_ID" --region "$REGION" \
    --query 'rootDomainUnitId' --output text 2>/dev/null || echo "")
  if [ -n "$ROOT_UNIT_ID" ] && [ "$ROOT_UNIT_ID" != "None" ]; then
    aws datazone remove-entity-owner \
      --domain-identifier "$DOMAIN_ID" --entity-type DOMAIN_UNIT \
      --entity-identifier "$ROOT_UNIT_ID" \
      --owner "{\"user\":{\"userIdentifier\":\"${ROLE_ARN}\"}}" \
      --region "$REGION" 2>/dev/null || true
  fi
  echo "  ✅ DataZone memberships removed"
fi

log "Deleting IAM role"
aws iam delete-role-policy --role-name "$LAMBDA_ROLE_NAME" \
  --policy-name "RedshiftGrantAccess" 2>/dev/null || true
aws iam detach-role-policy --role-name "$LAMBDA_ROLE_NAME" \
  --policy-arn "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole" 2>/dev/null || true
aws iam delete-role --role-name "$LAMBDA_ROLE_NAME" 2>/dev/null || true
echo "  ✅ IAM role deleted"

log "🎉 GRANT AUTOMATION REMOVED"
