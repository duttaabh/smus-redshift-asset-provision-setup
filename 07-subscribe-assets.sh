#!/bin/bash
set -uo pipefail
###############################################################################
# Subscribe a consumer project to specific Redshift table assets
#
# Usage:
#   ./07-subscribe-assets.sh                  # cleanup + resubscribe (default)
#   ./07-subscribe-assets.sh --cleanup-only   # remove subscriptions only
#   ./07-subscribe-assets.sh --subscribe-only # subscribe without cleanup
#   ./07-subscribe-assets.sh --status         # check subscription status
#   ./07-subscribe-assets.sh --help
#
# Modes:
#   (no flag)        Full cycle: cancel existing subscriptions, then resubscribe
#   --cleanup-only   Cancel subscriptions, delete targets/grants/temp resources
#   --subscribe-only Create + approve subscription requests (no cleanup first)
#                    Use this when subscribing from the SMUS UI — just check status
#   --status         Print current subscription status for all configured tables
#
# Note: When subscribing via the SMUS UI, raise the request as the SSO user
#       (not via CLI). The Lambda resolves grants from subscription.createdBy,
#       so SSO-raised requests get scoped to the requester's IdC groups only.
#       CLI/IAM-raised requests fall back to all project SSO members.
#
# Reads from .env:
#   CONSUMER_PROJECT_ID=<project-id>
#   SUBSCRIBE_TABLES="employees,departments"
#
# Required: DOMAIN_ID, ADMIN_PROJECT_ID, CONSUMER_PROJECT_ID, SUBSCRIBE_TABLES
###############################################################################

# --- Parse flags ---
ACTION="all"
case "${1:-}" in
  --cleanup-only)  ACTION="cleanup" ;;
  --subscribe-only) ACTION="subscribe" ;;
  --status)        ACTION="status" ;;
  --help|-h)
    echo "Usage: $0 [--cleanup-only | --subscribe-only | --status | --help]"
    echo ""
    echo "Modes:"
    echo "  (no flag)        Clean up existing subscriptions, then resubscribe via CLI"
    echo "  --cleanup-only   Cancel subscriptions, delete targets/grants/temp resources"
    echo "  --subscribe-only Create + approve subscription requests (no cleanup first)"
    echo "  --status         Check current subscription status"
    echo ""
    echo "Tip: For SSO-scoped grants, raise the subscription from the SMUS UI"
    echo "     as the SSO user. CLI subscriptions fall back to all project members."
    echo ""
    echo "Configure in .env: CONSUMER_PROJECT_ID, SUBSCRIBE_TABLES"
    exit 0
    ;;
esac

source "$(dirname "$0")/config.sh"

: "${CONSUMER_PROJECT_ID:?'CONSUMER_PROJECT_ID is required in .env'}"
: "${SUBSCRIBE_TABLES:?'SUBSCRIBE_TABLES is required in .env (e.g. employees,departments)'}"

IFS=',' read -ra TABLES <<< "$SUBSCRIBE_TABLES"

###############################################################################
# Look up listing IDs for requested tables
###############################################################################
log "Looking up listings for: ${SUBSCRIBE_TABLES}"

ALL_LISTINGS=$(aws datazone search-listings \
  --domain-identifier "$DOMAIN_ID" --region "$REGION" --output json | python3 -c "
import json, sys
data = json.load(sys.stdin)
seen = set()
for item in data.get('items', []):
    al = item.get('assetListing', {})
    if al and al.get('owningProjectId') == '$ADMIN_PROJECT_ID':
        name = al.get('name', '')
        lid = al.get('listingId', '')
        rev = al.get('listingRevision', '1')
        if name not in seen:
            seen.add(name)
            print(f'{name}|{lid}|{rev}')
")

LISTINGS=""
for tbl in "${TABLES[@]}"; do
  MATCH=$(echo "$ALL_LISTINGS" | grep "^${tbl}|" || echo "")
  if [ -n "$MATCH" ]; then
    LID=$(echo "$MATCH" | cut -d'|' -f2)
    REV=$(echo "$MATCH" | cut -d'|' -f3)
    LISTINGS="${LISTINGS}${LID}|${REV}|${tbl}
"
    echo "  ✅ $tbl → listing $LID (rev $REV)"
  else
    echo "  ❌ $tbl — no published listing found"
  fi
done
LISTINGS=$(echo "$LISTINGS" | sed '/^$/d')

if [ -z "$LISTINGS" ]; then
  echo "No matching listings found."
  exit 1
fi

###############################################################################
# FUNCTION: do_cleanup
###############################################################################
do_cleanup() {
  log "Cleaning up existing subscriptions and temp resources"

  echo "  Cancelling subscriptions..."
  while IFS='|' read -r LID LREV LNAME; do
    while true; do
      SUBS=$(aws datazone list-subscriptions \
        --domain-identifier "$DOMAIN_ID" --subscribed-listing-id "$LID" \
        --region "$REGION" --query 'items[?status!=`CANCELLED`].[id,status]' --output text 2>/dev/null || echo "")
      [ -z "$SUBS" ] && break
      echo "$SUBS" | while read -r SUB_ID SUB_STATUS; do
        echo "    Cancelling $SUB_ID ($SUB_STATUS) for $LNAME"
        aws datazone cancel-subscription --domain-identifier "$DOMAIN_ID" \
          --identifier "$SUB_ID" --region "$REGION" --query 'status' --output text 2>/dev/null || true
      done
      sleep 3
    done
  done <<< "$LISTINGS"

  echo "  Deleting pending subscription requests..."
  REQS=$(aws datazone list-subscription-requests \
    --domain-identifier "$DOMAIN_ID" --approver-project-id "$ADMIN_PROJECT_ID" \
    --region "$REGION" --query 'items[*].[id,status]' --output text 2>/dev/null || echo "")
  if [ -n "$REQS" ]; then
    echo "$REQS" | while read -r REQ_ID REQ_STATUS; do
      aws datazone delete-subscription-request --domain-identifier "$DOMAIN_ID" \
        --identifier "$REQ_ID" --region "$REGION" 2>/dev/null || true
    done
    sleep 3
  fi

  echo "  Cleaning up subscription targets and temp connections..."
  CONSUMER_ENVS=$(aws datazone list-environments \
    --domain-identifier "$DOMAIN_ID" --project-identifier "$CONSUMER_PROJECT_ID" \
    --region "$REGION" --query 'items[*].id' --output text 2>/dev/null || echo "")

  for ENV_ID in $CONSUMER_ENVS; do
    TARGETS=$(aws datazone list-subscription-targets \
      --domain-identifier "$DOMAIN_ID" --environment-identifier "$ENV_ID" \
      --region "$REGION" --query 'items[*].id' --output text 2>/dev/null || echo "")
    for TGT_ID in $TARGETS; do
      GRANTS=$(aws datazone list-subscription-grants \
        --domain-identifier "$DOMAIN_ID" --environment-id "$ENV_ID" \
        --region "$REGION" --output json 2>/dev/null | python3 -c "
import json, sys
data = json.load(sys.stdin)
for g in data.get('items', []):
    if g.get('subscriptionTargetId') == '${TGT_ID}':
        print(g['id'])
" 2>/dev/null || echo "")
      for GRANT_ID in $GRANTS; do
        echo "    Deleting subscription grant: $GRANT_ID (target: $TGT_ID)"
        for attempt in $(seq 1 12); do
          GRANT_STATUS=$(aws datazone get-subscription-grant \
            --domain-identifier "$DOMAIN_ID" --identifier "$GRANT_ID" \
            --region "$REGION" --query 'status' --output text 2>/dev/null || echo "UNKNOWN")
          if [ "$GRANT_STATUS" != "PENDING" ] && [ "$GRANT_STATUS" != "GRANT_IN_PROGRESS" ]; then
            break
          fi
          echo "      Grant status: $GRANT_STATUS — waiting..."
          sleep 5
        done
        aws datazone delete-subscription-grant --domain-identifier "$DOMAIN_ID" \
          --identifier "$GRANT_ID" --region "$REGION" 2>/dev/null || true
      done

      if [ -n "$GRANTS" ]; then
        for i in $(seq 1 12); do
          REMAINING_GRANTS=$(aws datazone list-subscription-grants \
            --domain-identifier "$DOMAIN_ID" --environment-id "$ENV_ID" \
            --region "$REGION" --output json 2>/dev/null | python3 -c "
import json, sys
data = json.load(sys.stdin)
for g in data.get('items', []):
    if g.get('subscriptionTargetId') == '${TGT_ID}':
        print(g['id'])
" 2>/dev/null || echo "")
          [ -z "$REMAINING_GRANTS" ] && break
          echo "    Waiting for grant deletion to complete..."
          sleep 5
        done
      fi
    done

    for TGT_ID in $TARGETS; do
      echo "    Deleting subscription target: $TGT_ID"
      aws datazone delete-subscription-target --domain-identifier "$DOMAIN_ID" \
        --environment-identifier "$ENV_ID" --identifier "$TGT_ID" \
        --region "$REGION" 2>/dev/null || true
    done

    if [ -n "$TARGETS" ]; then
      for i in $(seq 1 6); do
        REMAINING=$(aws datazone list-subscription-targets \
          --domain-identifier "$DOMAIN_ID" --environment-identifier "$ENV_ID" \
          --region "$REGION" --query 'items[*].id' --output text 2>/dev/null || echo "")
        [ -z "$REMAINING" ] && break
        echo "    Waiting for target deletion to propagate..."
        sleep 5
      done
    fi

    TEMP_CONNS=$(aws datazone list-connections \
      --domain-identifier "$DOMAIN_ID" --project-identifier "$CONSUMER_PROJECT_ID" \
      --region "$REGION" --output json 2>/dev/null | python3 -c "
import json, sys
for c in json.load(sys.stdin).get('items', []):
    if c.get('name','').startswith('temp-'):
        print(c['connectionId'])
" 2>/dev/null || echo "")
    for CONN_ID in $TEMP_CONNS; do
      echo "    Deleting temp connection: $CONN_ID"
      aws datazone delete-connection --domain-identifier "$DOMAIN_ID" \
        --identifier "$CONN_ID" --region "$REGION" 2>/dev/null || true
    done
  done

  echo "  Cleaning up temp secrets and Redshift users..."
  PROJECT_PREFIX="${CONSUMER_PROJECT_ID:0:8}"
  TEMP_SECRETS=$(aws secretsmanager list-secrets \
    --filters "Key=tag-key,Values=smus-temp-resource" "Key=tag-value,Values=true" \
    --region "$REGION" --query 'SecretList[*].[Name,ARN]' --output text 2>/dev/null || echo "")
  if [ -n "$TEMP_SECRETS" ]; then
    echo "$TEMP_SECRETS" | while read -r SEC_NAME SEC_ARN; do
      echo "$SEC_NAME" | grep -q "$PROJECT_PREFIX" || continue
      TEMP_USER=$(aws secretsmanager get-secret-value --secret-id "$SEC_NAME" \
        --region "$REGION" --query 'SecretString' --output text 2>/dev/null | \
        python3 -c "import json,sys; print(json.load(sys.stdin).get('username',''))" 2>/dev/null || echo "")
      if [ -n "$TEMP_USER" ] && echo "$TEMP_USER" | grep -q "^temp_dz_"; then
        echo "    Revoking grants and dropping temp user: $TEMP_USER"
        SCHEMAS=$(aws redshift-data execute-statement \
          --cluster-identifier "$CLUSTER_ID" --database "$REDSHIFT_DB" --db-user "$REDSHIFT_USER" \
          --sql "SELECT DISTINCT nspname FROM pg_namespace n JOIN pg_user u ON 1=1 WHERE u.usename = '${TEMP_USER}' AND has_schema_privilege(u.usename, n.nspname, 'USAGE') AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')" \
          --region "$REGION" --query 'Id' --output text 2>/dev/null || echo "")
        if [ -n "$SCHEMAS" ]; then
          sleep 3
          SCHEMA_LIST=$(aws redshift-data get-statement-result --id "$SCHEMAS" \
            --region "$REGION" --query 'Records[*][0].stringValue' --output text 2>/dev/null || echo "public")
          [ -z "$SCHEMA_LIST" ] && SCHEMA_LIST="public"
        else
          SCHEMA_LIST="public"
        fi
        REVOKE_SQL=""
        for S in $SCHEMA_LIST; do
          REVOKE_SQL="${REVOKE_SQL}REVOKE ALL ON ALL TABLES IN SCHEMA ${S} FROM ${TEMP_USER}; REVOKE USAGE ON SCHEMA ${S} FROM ${TEMP_USER}; "
        done
        REVOKE_SQL="${REVOKE_SQL}DROP USER IF EXISTS ${TEMP_USER}"
        aws redshift-data execute-statement \
          --cluster-identifier "$CLUSTER_ID" --database "$REDSHIFT_DB" --db-user "$REDSHIFT_USER" \
          --sql "$REVOKE_SQL" \
          --region "$REGION" > /dev/null 2>&1 || true
      fi
      echo "    Deleting temp secret: $SEC_NAME"
      aws secretsmanager delete-secret --secret-id "$SEC_ARN" \
        --force-delete-without-recovery --region "$REGION" 2>/dev/null || true
    done
  fi

  echo "  Waiting for cancellations..."
  for i in $(seq 1 12); do
    ALL_CLEAN=true
    while IFS='|' read -r LID LREV LNAME; do
      REMAINING=$(aws datazone list-subscriptions \
        --domain-identifier "$DOMAIN_ID" --subscribed-listing-id "$LID" \
        --region "$REGION" --query 'items[?status!=`CANCELLED`].[id]' --output text 2>/dev/null || echo "")
      [ -n "$REMAINING" ] && ALL_CLEAN=false && break
    done <<< "$LISTINGS"
    $ALL_CLEAN && break
    [ "$i" -eq 12 ] && echo "    ⚠️  Some subscriptions still active after 60s" && break
    sleep 5
  done
  echo "  ✅ Cleanup complete"
}

###############################################################################
# FUNCTION: do_subscribe
###############################################################################
do_subscribe() {
  log "Creating subscription requests for ${#TABLES[@]} table(s)"

  while IFS='|' read -r LID LREV LNAME; do
    echo "  Requesting subscription: $LNAME (listing $LID, rev $LREV)"
    REQ_ID=$(aws datazone create-subscription-request \
      --domain-identifier "$DOMAIN_ID" \
      --subscribed-principals "[{\"project\":{\"identifier\":\"$CONSUMER_PROJECT_ID\"}}]" \
      --subscribed-listings "[{\"identifier\":\"$LID\"}]" \
      --request-reason "Automated subscription for $LNAME" \
      --region "$REGION" --query 'id' --output text 2>&1)

    if [ $? -ne 0 ] || [ -z "$REQ_ID" ] || echo "$REQ_ID" | grep -qi "error"; then
      echo "    ❌ Failed to create request for $LNAME: $REQ_ID"
      continue
    fi
    echo "    📋 Request ID: $REQ_ID"

    echo "    Approving subscription request..."
    APPROVE_STATUS=$(aws datazone accept-subscription-request \
      --domain-identifier "$DOMAIN_ID" --identifier "$REQ_ID" \
      --region "$REGION" --query 'status' --output text 2>&1)

    if echo "$APPROVE_STATUS" | grep -qi "error"; then
      echo "    ❌ Approval failed for $LNAME: $APPROVE_STATUS"
    else
      echo "    ✅ $LNAME → $APPROVE_STATUS"
    fi
  done <<< "$LISTINGS"

  echo ""
  log "Waiting for subscriptions to activate..."
  for i in $(seq 1 24); do
    ALL_ACTIVE=true
    while IFS='|' read -r LID LREV LNAME; do
      STATUS=$(aws datazone list-subscriptions \
        --domain-identifier "$DOMAIN_ID" --subscribed-listing-id "$LID" \
        --region "$REGION" --query 'items[?status!=`CANCELLED`].[status]' --output text 2>/dev/null | head -1)
      if [ "$STATUS" != "APPROVED" ] && [ "$STATUS" != "GRANTED" ]; then
        ALL_ACTIVE=false
        echo "  ⏳ $LNAME: $STATUS"
        break
      fi
    done <<< "$LISTINGS"
    $ALL_ACTIVE && break
    [ "$i" -eq 24 ] && echo "  ⚠️  Timed out waiting for activation (120s)" && break
    sleep 5
  done

  echo ""
  do_status
}

###############################################################################
# FUNCTION: do_status
###############################################################################
do_status() {
  log "Subscription status for consumer project: $CONSUMER_PROJECT_ID"
  echo ""
  printf "  %-20s %-38s %s\n" "TABLE" "SUBSCRIPTION ID" "STATUS"
  printf "  %-20s %-38s %s\n" "-----" "---------------" "------"

  while IFS='|' read -r LID LREV LNAME; do
    SUBS=$(aws datazone list-subscriptions \
      --domain-identifier "$DOMAIN_ID" --subscribed-listing-id "$LID" \
      --region "$REGION" --query 'items[?status!=`CANCELLED`].[id,status]' --output text 2>/dev/null || echo "")
    if [ -z "$SUBS" ]; then
      printf "  %-20s %-38s %s\n" "$LNAME" "(none)" "NOT SUBSCRIBED"
    else
      echo "$SUBS" | while read -r SUB_ID SUB_STATUS; do
        printf "  %-20s %-38s %s\n" "$LNAME" "$SUB_ID" "$SUB_STATUS"
      done
    fi
  done <<< "$LISTINGS"
  echo ""
}

###############################################################################
# MAIN DISPATCH
###############################################################################
case "$ACTION" in
  cleanup)
    do_cleanup
    ;;
  subscribe)
    do_subscribe
    ;;
  status)
    do_status
    ;;
  all)
    do_cleanup
    echo ""
    do_subscribe
    ;;
esac

log "Done."
