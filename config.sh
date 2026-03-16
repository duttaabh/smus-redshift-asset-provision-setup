#!/bin/bash
###############################################################################
# Shared configuration for all SMUS Redshift setup scripts
#
# All values can be overridden via environment variables or a .env file.
# Place a .env file in this directory to set values without editing this file.
#
# Example .env:
#   REGION=us-west-2
#   CLUSTER_ID=my-redshift-cluster
#   DOMAIN_ID=dzd-abc123
###############################################################################

# Load .env if present (same directory as this script)
_CONFIG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "${_CONFIG_DIR}/.env" ]; then
  set -a
  source "${_CONFIG_DIR}/.env"
  set +a
fi

# --- AWS ---
REGION="${REGION:-us-east-1}"
# Export AWS_PROFILE so all aws CLI calls in every script use it automatically
if [ -n "${AWS_PROFILE:-}" ]; then
  export AWS_PROFILE
fi
ACCOUNT_ID="${ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "")}"

# --- Redshift cluster ---
CLUSTER_ID="${CLUSTER_ID:?'CLUSTER_ID is required (e.g. my-redshift-cluster)'}"
REDSHIFT_HOST="${REDSHIFT_HOST:-${CLUSTER_ID}.$(aws redshift describe-clusters --cluster-identifier "$CLUSTER_ID" --region "$REGION" --query 'Clusters[0].Endpoint.Address' --output text 2>/dev/null || echo "${CLUSTER_ID}.UNKNOWN.${REGION}.redshift.amazonaws.com")}"
REDSHIFT_PORT="${REDSHIFT_PORT:-5439}"
REDSHIFT_DB="${REDSHIFT_DB:-dev}"
REDSHIFT_USER="${REDSHIFT_USER:?'REDSHIFT_USER is required (e.g. awsuser)'}"
# Password is only required by scripts that use secret-based connections (01, 02).
# Scripts 06 (IdC) and 07 (TIP) use IAM auth and don't need it.
REDSHIFT_PASSWORD="${REDSHIFT_PASSWORD:-}"

# --- DataZone / SMUS ---
DOMAIN_ID="${DOMAIN_ID:?'DOMAIN_ID is required (e.g. dzd-abc123)'}"
ADMIN_PROJECT_ID="${ADMIN_PROJECT_ID:?'ADMIN_PROJECT_ID is required'}"
ADMIN_ENV_ID="${ADMIN_ENV_ID:?'ADMIN_ENV_ID is required'}"
CONSUMER_PROJECT_ID="${CONSUMER_PROJECT_ID:-}"   # only needed by 07-subscribe-assets.sh
CONSUMER_ENV_ID="${CONSUMER_ENV_ID:-}"           # only needed by 07-subscribe-assets.sh

# --- VPC / Network ---
VPC_ID="${VPC_ID:-}"
SUBNET_ID="${SUBNET_ID:-}"
SMUS_SG="${SMUS_SG:-}"
REDSHIFT_SG="${REDSHIFT_SG:-}"

# Auto-discover VPC/SG from Redshift cluster if not provided
if [ -z "$VPC_ID" ] || [ -z "$REDSHIFT_SG" ]; then
  _CLUSTER_INFO=$(aws redshift describe-clusters --cluster-identifier "$CLUSTER_ID" \
    --region "$REGION" --output json 2>/dev/null || echo '{"Clusters":[]}')
  if [ -z "$VPC_ID" ]; then
    VPC_ID=$(echo "$_CLUSTER_INFO" | python3 -c "import json,sys; c=json.load(sys.stdin)['Clusters']; print(c[0]['VpcId'] if c else '')" 2>/dev/null || echo "")
  fi
  if [ -z "$REDSHIFT_SG" ]; then
    REDSHIFT_SG=$(echo "$_CLUSTER_INFO" | python3 -c "import json,sys; c=json.load(sys.stdin)['Clusters']; print(c[0]['VpcSecurityGroups'][0]['VpcSecurityGroupId'] if c and c[0].get('VpcSecurityGroups') else '')" 2>/dev/null || echo "")
  fi
fi

# --- IAM roles (derived — override if your naming differs) ---
DZ_USER_ROLE="${DZ_USER_ROLE:-datazone_usr_role_${ADMIN_PROJECT_ID}_${ADMIN_ENV_ID}}"
SAGEMAKER_MANAGE_ROLE="${SAGEMAKER_MANAGE_ROLE:-arn:aws:iam::${ACCOUNT_ID}:role/service-role/AmazonSageMakerManageAccess-${REGION}-${DOMAIN_ID}}"

# --- Grant automation resource names ---
LAMBDA_NAME="${LAMBDA_NAME:-smus-redshift-grant-handler}"
EB_RULE_NAME="${EB_RULE_NAME:-smus-redshift-subscription-grant}"
LAMBDA_ROLE_NAME="${LAMBDA_ROLE_NAME:-smus-redshift-grant-lambda-role}"
# --- Helpers ---
log() { echo -e "\n=== $1 ==="; }

wait_for() {
  local cmd="$1" field="$2" expected="$3" max="${4:-30}" interval="${5:-5}"
  for i in $(seq 1 $max); do
    val=$(eval "$cmd" 2>/dev/null || echo "")
    if [ "$val" = "$expected" ]; then echo "  ✅ $field = $expected"; return 0; fi
    echo "  ⏳ $field = $val (attempt $i/$max)..."
    sleep "$interval"
  done
  echo "  ❌ Timed out waiting for $field = $expected"; return 1
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
