"""
Lambda handler: Auto-grant Redshift SELECT on subscribed tables.

Triggered by EventBridge on two DataZone events:

1. "Subscription Request Accepted" — creates a temporary readonly Redshift user,
   a temporary secret + connection in the consumer project, and a subscription
   target so DataZone can fulfill the grant.

2. "Subscription Grant Completed" — executes Redshift-level GRANT statements,
   then cleans up the temporary connection, secret, and readonly user.

Grant resolution logic (for Subscription Grant Completed):
  1. Resolves the subscription requester's IdC user ID from the subscription request.
  2. Looks up the requester's IdC group memberships.
  3. If the requester belongs to IdC groups that have corresponding Redshift roles
     (AWSIDC:<GroupName>), grants SELECT to those roles (group-level grant).
  4. Also grants to any DB users found in the consumer project's connections.

Temporary resource lifecycle:
  - On accept: creates temp_dz_<project>_<timestamp> Redshift user (readonly),
    a Secrets Manager secret, a DataZone connection, and a subscription target.
  - On grant completed: deletes the connection, secret, and Redshift user.
  - The subscription target uses the ADMIN secret for grant fulfillment
    (DataZone needs admin privileges to execute the fulfillment SQL).
"""
import json
import os
import time
import secrets
import string
import boto3

redshift_data = boto3.client("redshift-data")
datazone = boto3.client("datazone")
secretsmanager = boto3.client("secretsmanager")
sts = boto3.client("sts")
identitystore = boto3.client("identitystore")
sso_admin = boto3.client("sso-admin")

CLUSTER_ID = os.environ["REDSHIFT_CLUSTER_ID"]
REDSHIFT_DB = os.environ["REDSHIFT_DATABASE"]
REDSHIFT_ADMIN_USER = os.environ["REDSHIFT_ADMIN_USER"]
DOMAIN_ID = os.environ["DOMAIN_ID"]
ADMIN_PROJECT_ID = os.environ.get("ADMIN_PROJECT_ID", "")
IDC_NAMESPACE = os.environ.get("IDC_NAMESPACE", "AWSIDC")

# IDC_GROUP_GRANTS format: "Group1:PRIV:table1,Group2:PRIV:table2"
# Used to filter which IdC group roles to grant/revoke per table
_IDC_GROUP_GRANTS_RAW = os.environ.get("IDC_GROUP_GRANTS", "")

def _parse_idc_group_grants():
    """Return dict: {table_name: [quoted_role, ...]}"""
    mapping = {}
    if not _IDC_GROUP_GRANTS_RAW:
        return mapping
    for entry in _IDC_GROUP_GRANTS_RAW.split(","):
        parts = entry.strip().split(":")
        if len(parts) == 3:
            group, _priv, table = parts
            role = f'"{IDC_NAMESPACE}:{group}"'
            mapping.setdefault(table, []).append(role)
    return mapping

# Cache per cold start
_lambda_role_arn = None
_identity_store_id = None
_account_id = None

# Prefix for temporary resources created by this Lambda
TEMP_PREFIX = "temp_dz_"


def lambda_handler(event, context):
    print(f"Event: {json.dumps(event)}")

    detail_type = event.get("detail-type", "")
    detail = event.get("detail", {})
    metadata = detail.get("metadata", {})
    data = detail.get("data", {})

    domain_id = metadata.get("domain", "") or metadata.get("domainId", "")
    if domain_id != DOMAIN_ID:
        print(f"Ignoring event for domain {domain_id} (expected {DOMAIN_ID})")
        return {"status": "skipped", "reason": "wrong domain"}

    if detail_type == "Subscription Request Accepted":
        return handle_subscription_accepted(metadata, data)
    elif detail_type == "Subscription Grant Completed":
        return handle_grant_completed(metadata, data)
    elif detail_type in ("Subscription Revoked", "Subscription Cancelled",
                         "Subscription Grant Revoke Completed"):
        return handle_revoke(detail_type, metadata, data)
    else:
        print(f"Unhandled event type: {detail_type}")
        return {"status": "skipped", "reason": f"unhandled event: {detail_type}"}


###############################################################################
# Helper: resolve schema name from a DataZone asset
###############################################################################

def resolve_asset_schema(asset_id):
    """
    Resolve the schema name from a DataZone asset's form data.
    The asset's formsOutput contains a RedshiftTableFormType with schemaName.
    Falls back to 'public' if not resolvable.
    """
    try:
        asset = datazone.get_asset(
            domainIdentifier=DOMAIN_ID, identifier=asset_id)
        forms = asset.get("formsOutput", [])
        for form in forms:
            content = form.get("content", "")
            if not content:
                continue
            try:
                form_data = json.loads(content)
                schema = form_data.get("schemaName", "")
                if schema:
                    print(f"Resolved schema '{schema}' from asset {asset_id}")
                    return schema
            except (json.JSONDecodeError, TypeError):
                continue
        print(f"No schema found in asset {asset_id} forms, defaulting to 'public'")
    except Exception as e:
        print(f"Could not resolve schema for asset {asset_id}: {e}")
    return "public"


def resolve_schemas_from_subscription_request(request_id):
    """
    Resolve all unique schema names from the assets in a subscription request.
    Returns a set of schema names.
    """
    schemas = set()
    try:
        req = datazone.get_subscription_request_details(
            domainIdentifier=DOMAIN_ID, identifier=request_id)
        for listing in req.get("subscribedListings", []):
            items = listing.get("item", {})
            # The asset reference is in the listing item
            asset_id = items.get("assetListing", {}).get("entityId", "")
            if not asset_id:
                # Try alternate path
                asset_id = listing.get("id", "")
            if asset_id:
                schema = resolve_asset_schema(asset_id)
                schemas.add(schema)
    except Exception as e:
        print(f"Could not resolve schemas from request {request_id}: {e}")

    if not schemas:
        schemas.add("public")
        print("No schemas resolved from request, defaulting to {'public'}")
    else:
        print(f"Resolved schemas from subscription request: {schemas}")
    return schemas


###############################################################################
# Handler: Subscription Request Accepted
###############################################################################

def handle_subscription_accepted(metadata, data):
    """
    Create temporary readonly connection in consumer project + subscription target.

    Flow:
      1. Create a temporary readonly Redshift user
      2. Create a Secrets Manager secret with those credentials
      3. Create a DataZone connection in the consumer project
      4. Create a subscription target (using ADMIN secret for grant fulfillment)
    """
    request_id = metadata.get("id", "")
    principals = data.get("subscribedPrincipals", [])

    if not principals:
        print("No subscribedPrincipals in event")
        return {"status": "skipped", "reason": "no principals"}

    consumer_project_id = principals[0].get("id", "")
    if not consumer_project_id:
        print("No consumer project ID in event")
        return {"status": "skipped", "reason": "no consumer project"}

    print(f"Subscription accepted: request={request_id}, "
          f"consumer={consumer_project_id}")

    # Ensure we're a member of the consumer project
    ensure_project_membership(consumer_project_id)

    # Find the Tooling environment in the consumer project
    tooling_env_id = find_tooling_environment(consumer_project_id)
    if not tooling_env_id:
        print(f"No Tooling environment found in project {consumer_project_id}")
        return {"status": "error", "reason": "no tooling environment"}

    # Check if a Redshift subscription target already exists
    existing_target = find_redshift_subscription_target(
        consumer_project_id, tooling_env_id)
    if existing_target:
        print(f"Redshift subscription target already exists: {existing_target}")
        # Still need to initiate the grant for this new subscription request
        self_initiate_grant(consumer_project_id, tooling_env_id,
                            existing_target, request_id)
        return {"status": "ok", "reason": "target already exists",
                "targetId": existing_target}

    # --- Step 1: Create temporary readonly Redshift user ---
    timestamp = str(int(time.time()))
    temp_user = f"{TEMP_PREFIX}{consumer_project_id[:8]}_{timestamp}"
    # Redshift usernames max 128 chars, truncate if needed
    temp_user = temp_user[:128]
    temp_password = generate_password()

    print(f"Creating temporary Redshift user: {temp_user}")
    if not exec_sql_wait(f"CREATE USER {temp_user} PASSWORD '{temp_password}'"):
        return {"status": "error", "reason": "failed to create temp user"}

    # Resolve which schemas the subscribed assets belong to
    schemas = resolve_schemas_from_subscription_request(request_id)
    print(f"Granting temp user access to schemas: {schemas}")

    for schema in schemas:
        exec_sql_wait(f"GRANT USAGE ON SCHEMA {schema} TO {temp_user}")
        exec_sql_wait(f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {temp_user}")

    # --- Step 2: Create Secrets Manager secret ---
    account_id = get_account_id()
    region = os.environ.get("AWS_REGION", "us-east-1")
    secret_name = f"smus-redshift-temp-{consumer_project_id[:8]}-{timestamp}"

    manage_role_arn = (
        f"arn:aws:iam::{account_id}:role/service-role/"
        f"AmazonSageMakerManageAccess-{region}-{DOMAIN_ID}"
    )
    dz_user_role = f"datazone_usr_role_{consumer_project_id}_{tooling_env_id}"

    try:
        secret_resp = secretsmanager.create_secret(
            Name=secret_name,
            Description=f"Temp readonly creds for {consumer_project_id}",
            SecretString=json.dumps({
                "username": temp_user,
                "password": temp_password,
            }),
            Tags=[
                {"Key": "AmazonDataZoneProject", "Value": consumer_project_id},
                {"Key": "AmazonDataZoneDomain", "Value": DOMAIN_ID},
                {"Key": "AmazonDataZoneEnvironment", "Value": tooling_env_id},
                {"Key": "AmazonDataZoneCreatedVia", "Value": "SageMakerUnifiedStudio"},
                {"Key": "smus-temp-resource", "Value": "true"},
            ],
        )
        temp_secret_arn = secret_resp["ARN"]
        print(f"Created temp secret: {temp_secret_arn}")
    except Exception as e:
        print(f"Failed to create temp secret: {e}")
        # Cleanup the user we just created
        for schema in schemas:
            exec_sql_wait(f"REVOKE ALL ON ALL TABLES IN SCHEMA {schema} FROM {temp_user}")
            exec_sql_wait(f"REVOKE USAGE ON SCHEMA {schema} FROM {temp_user}")
        exec_sql_wait(f"DROP USER IF EXISTS {temp_user}")
        return {"status": "error", "reason": str(e)}

    # Resource policy so DataZone roles can read it
    try:
        secretsmanager.put_resource_policy(
            SecretId=temp_secret_arn,
            ResourcePolicy=json.dumps({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": [manage_role_arn,
                                f"arn:aws:iam::{account_id}:role/{dz_user_role}"]
                    },
                    "Action": "secretsmanager:GetSecretValue",
                    "Resource": "*",
                }],
            }),
        )
    except Exception as e:
        print(f"Warning: could not set secret resource policy: {e}")

    # --- Step 3: Create DataZone connection in consumer project ---
    conn_name = f"temp-{CLUSTER_ID}-{timestamp}"
    redshift_host = os.environ.get("REDSHIFT_HOST", "")
    if not redshift_host:
        # Derive from cluster
        redshift_host = f"{CLUSTER_ID}.{region}.redshift.amazonaws.com"

    try:
        conn_resp = datazone.create_connection(
            domainIdentifier=DOMAIN_ID,
            environmentIdentifier=tooling_env_id,
            name=conn_name,
            description=f"Temporary connection for subscription grant (auto-cleanup)",
            props={
                "redshiftProperties": {
                    "credentials": {"secretArn": temp_secret_arn},
                    "databaseName": REDSHIFT_DB,
                    "host": redshift_host,
                    "port": 5439,
                    "storage": {"clusterName": CLUSTER_ID},
                }
            },
        )
        temp_conn_id = conn_resp["connectionId"]
        print(f"Created temp connection: {temp_conn_id}")
    except Exception as e:
        print(f"Failed to create temp connection: {e}")
        # Cleanup secret and user
        cleanup_temp_secret(temp_secret_arn)
        for schema in schemas:
            exec_sql_wait(f"REVOKE ALL ON ALL TABLES IN SCHEMA {schema} FROM {temp_user}")
            exec_sql_wait(f"REVOKE USAGE ON SCHEMA {schema} FROM {temp_user}")
        exec_sql_wait(f"DROP USER IF EXISTS {temp_user}")
        return {"status": "error", "reason": str(e)}

    # Tag secret with connection ID for later cleanup
    try:
        secretsmanager.tag_resource(
            SecretId=temp_secret_arn,
            Tags=[{"Key": "AmazonDataZoneConnection", "Value": temp_conn_id}],
        )
    except Exception:
        pass

    # --- Step 4: Create subscription target (using ADMIN secret) ---
    admin_secret_arn = find_connection_secret(ADMIN_PROJECT_ID)
    if not admin_secret_arn:
        print("WARNING: No admin secret found, using temp secret for target")
        admin_secret_arn = temp_secret_arn

    authorized_principal = dz_user_role
    target_name = f"redshift-{CLUSTER_ID}-target"
    # Use the first resolved schema for the target config; actual grants
    # are per-asset and resolve schema individually in handle_grant_completed
    target_schema = sorted(schemas)[0] if schemas else "public"
    config_content = json.dumps({
        "databaseName": REDSHIFT_DB,
        "schemaName": target_schema,
        "secretManagerArn": admin_secret_arn,
        "clusterIdentifier": CLUSTER_ID,
    })

    try:
        resp = datazone.create_subscription_target(
            domainIdentifier=DOMAIN_ID,
            environmentIdentifier=tooling_env_id,
            name=target_name,
            type="RedshiftSubscriptionTargetType",
            applicableAssetTypes=[
                "RedshiftTableAssetType",
                "RedshiftViewAssetType",
            ],
            authorizedPrincipals=[authorized_principal],
            manageAccessRole=manage_role_arn,
            subscriptionTargetConfig=[{
                "formName": "RedshiftSubscriptionTargetConfigForm",
                "content": config_content,
            }],
            provider="Amazon DataZone",
        )
        target_id = resp.get("id", "")
        print(f"Created subscription target: {target_id}")

        # DataZone doesn't retry grant fulfillment if no target existed at
        # approval time. We need to explicitly create the subscription grant
        # to kick off fulfillment now that the target exists.
        self_initiate_grant(consumer_project_id, tooling_env_id, target_id,
                            request_id)

        return {
            "status": "target_created",
            "targetId": target_id,
            "project": consumer_project_id,
            "tempConnection": temp_conn_id,
            "tempSecret": temp_secret_arn,
            "tempUser": temp_user,
        }
    except Exception as e:
        print(f"Failed to create subscription target: {e}")
        # Cleanup everything
        cleanup_temp_connection(temp_conn_id)
        cleanup_temp_secret(temp_secret_arn)
        for schema in schemas:
            exec_sql_wait(f"REVOKE ALL ON ALL TABLES IN SCHEMA {schema} FROM {temp_user}")
            exec_sql_wait(f"REVOKE USAGE ON SCHEMA {schema} FROM {temp_user}")
        exec_sql_wait(f"DROP USER IF EXISTS {temp_user}")
        return {"status": "error", "reason": str(e)}


###############################################################################
# Helper: explicitly create subscription grant after target creation
###############################################################################

def self_initiate_grant(consumer_project_id, env_id, target_id, request_id):
    """
    After creating the subscription target, DataZone won't automatically
    retry grant fulfillment. We need to find the subscription created from
    this request and explicitly create a subscription grant.
    """
    try:
        # Find the subscription created from this request
        # The subscription may take a moment to appear after approval
        subscription_id = None
        for _ in range(6):
            subs = datazone.list_subscriptions(
                domainIdentifier=DOMAIN_ID,
                subscriptionRequestIdentifier=request_id,
            )
            for sub in subs.get("items", []):
                if sub.get("status") in ("APPROVED", "CREATED"):
                    subscription_id = sub.get("id", "")
                    break
            if subscription_id:
                break
            time.sleep(5)

        if not subscription_id:
            print(f"Could not find subscription for request {request_id}")
            return

        print(f"Found subscription: {subscription_id}, creating grant...")

        # Get the asset listing to find the asset ID
        sub_detail = datazone.get_subscription(
            domainIdentifier=DOMAIN_ID,
            identifier=subscription_id,
        )

        # get_subscription returns subscribedListing (singular), not plural
        subscribed_listing = sub_detail.get("subscribedListing", {})
        if not subscribed_listing:
            print("No subscribedListing in subscription")
            return

        listing_id = subscribed_listing.get("id", "")
        listing_revision = subscribed_listing.get("revision", "1")

        if not listing_id:
            print("No listing ID in subscribedListing")
            return

        print(f"Listing: {listing_id} rev={listing_revision}")

        grant_resp = datazone.create_subscription_grant(
            domainIdentifier=DOMAIN_ID,
            environmentIdentifier=env_id,
            subscriptionTargetIdentifier=target_id,
            grantedEntity={
                "listing": {
                    "identifier": listing_id,
                    "revision": listing_revision,
                }
            },
        )
        grant_id = grant_resp.get("id", "")
        grant_status = grant_resp.get("status", "")
        print(f"Created subscription grant: {grant_id} (status={grant_status})")

    except datazone.exceptions.ConflictException:
        print("Subscription grant already exists — grant fulfillment "
              "should proceed automatically")
    except Exception as e:
        print(f"Failed to self-initiate grant: {e}")


###############################################################################
# Handler: Subscription Grant Completed
###############################################################################

def handle_grant_completed(metadata, data):
    """
    Execute Redshift GRANT, then clean up temporary resources.

    Flow:
      1. Resolve table name from asset
      2. Resolve requester's IdC groups for group-level grants
      3. Execute GRANT statements
      4. Clean up: delete temp connection, secret, and Redshift user
    """
    asset_info = data.get("asset", {})
    asset_id = asset_info.get("id", "")
    asset_type = asset_info.get("typeName", "")
    consumer_project_id = data.get("projectId", "")
    grant_status = data.get("status", "")
    env_id = data.get("subscriptionTarget", {}).get("environmentId", "")
    sub_target_id = data.get("subscriptionTarget", {}).get("id", "")

    grant_id = metadata.get("id", "")
    subscription_id = resolve_subscription_id_from_grant(grant_id)

    print(f"Grant: asset={asset_id}, type={asset_type}, "
          f"project={consumer_project_id}, env={env_id}, "
          f"target={sub_target_id}, status={grant_status}, "
          f"subscription={subscription_id}")

    if asset_type != "RedshiftTableAssetType":
        print(f"Not a Redshift table ({asset_type}), skipping")
        return {"status": "skipped", "reason": "not redshift table"}

    if grant_status != "GRANTED":
        print(f"Status is {grant_status}, skipping")
        return {"status": "skipped", "reason": f"status={grant_status}"}

    # 1. Get table name and schema from asset
    try:
        asset = datazone.get_asset(
            domainIdentifier=DOMAIN_ID, identifier=asset_id)
        table_name = asset["name"]
    except Exception as e:
        print(f"Failed to get asset {asset_id}: {e}")
        return {"status": "error", "reason": str(e)}

    # Resolve schema from asset form data
    schema_name = resolve_asset_schema(asset_id)

    # 2. Resolve grant targets — scoped to the subscription requester's IdC groups
    # Grant events always have user=SYSTEM; resolve the real requester from the
    # subscription's createdBy field instead.
    requester_idc_id = ""
    if subscription_id:
        try:
            sub = datazone.get_subscription(
                domainIdentifier=DOMAIN_ID, identifier=subscription_id)
            requester_idc_id = sub.get("createdBy", "")
            print(f"Resolved requester from subscription.createdBy: {requester_idc_id}")
        except Exception as e:
            print(f"Could not resolve requester from subscription: {e}")

    # Fall back to metadata.user only if subscription lookup failed
    if not requester_idc_id:
        requester_idc_id = metadata.get("user", "")

    grant_roles = resolve_roles_for_requester(requester_idc_id)
    if grant_roles is None:
        print("Requester not a real IdC user — falling back to project member resolution")
        targets = resolve_project_idc_groups(consumer_project_id)
        grant_roles = targets.get("roles", [])
        tip_users = targets.get("tip_users", [])
    else:
        # Scope TIP user grant to the requester only
        tip_users = resolve_tip_user_for_requester(
            requester_idc_id, consumer_project_id)

    if not grant_roles and not tip_users:
        print("No grant targets found — skipping GRANT")
        cleanup_temp_resources(consumer_project_id, env_id)
        return {"status": "skipped", "reason": "no grant targets"}

    # Build GRANT statements
    stmts = []

    # Role grants (AWSIDC:GroupName)
    for role in grant_roles:
        stmts.append(f"GRANT USAGE ON SCHEMA {schema_name} TO ROLE {role}")
        stmts.append(f"GRANT SELECT ON {schema_name}.{table_name} TO ROLE {role}")

    # TIP user grants (IAMR:user-<idcUserId>@<projectId>)
    # Pre-create users with PASSWORD DISABLE if they don't exist yet
    for tip_user in tip_users:
        stmts.append(f"CREATE USER {tip_user} PASSWORD DISABLE")

    # Execute grants in phases to handle CREATE USER failures gracefully

    # Phase 1: Role grants (AWSIDC:*)
    if grant_roles:
        role_stmts = []
        for role in grant_roles:
            role_stmts.append(f"GRANT USAGE ON SCHEMA {schema_name} TO ROLE {role}")
            role_stmts.append(
                f"GRANT SELECT ON {schema_name}.{table_name} TO ROLE {role}")
        role_sql = "; ".join(role_stmts) + ";"
        print(f"Phase 1 — role grants: {grant_roles}")
        ok, error = _exec_sql_and_wait(role_sql)
        if ok:
            print(f"  Role GRANT successful")
        else:
            print(f"  Role GRANT failed: {error}")

    # Phase 2: Create TIP users (PASSWORD DISABLE) — tolerate "already exists"
    for tip_user in tip_users:
        print(f"Phase 2 — ensuring TIP user exists: {tip_user}")
        exec_sql_wait(f"CREATE USER {tip_user} PASSWORD DISABLE")

    # Phase 3: TIP user grants
    if tip_users:
        user_stmts = []
        for tip_user in tip_users:
            user_stmts.append(
                f"GRANT USAGE ON SCHEMA {schema_name} TO {tip_user}")
            user_stmts.append(
                f"GRANT SELECT ON {schema_name}.{table_name} TO {tip_user}")
        user_sql = "; ".join(user_stmts) + ";"
        print(f"Phase 3 — TIP user grants: {tip_users}")
        ok, error = _exec_sql_and_wait(user_sql)
        if ok:
            print(f"  TIP user GRANT successful")
        else:
            print(f"  TIP user GRANT failed: {error}")

    all_targets = grant_roles + tip_users

    # 4. Clean up temporary resources regardless of grant outcome
    cleanup_temp_resources(consumer_project_id, env_id)

    return {"status": "granted", "table": f"{schema_name}.{table_name}",
            "targets": all_targets}

def handle_revoke(detail_type, metadata, data):
    """
    Revoke Redshift grants when a DataZone subscription is revoked/cancelled.

    Mirrors handle_grant_completed but issues REVOKE instead of GRANT.
    Handles both IdC group roles (AWSIDC:*) and TIP user identities.
    """
    # Subscription Cancelled/Revoked uses a different payload structure:
    #   data.subscribedListing.item.assetListing.entityId / entityType
    #   data.subscribedPrincipal.id (singular, not plural)
    # Subscription Grant Revoke Completed uses:
    #   data.asset.id / data.asset.typeName
    #   data.projectId

    asset_id = ""
    asset_type = ""
    consumer_project_id = ""

    # Try Grant Revoke Completed structure first
    asset_info = data.get("asset", {})
    if asset_info:
        asset_id = asset_info.get("id", "")
        asset_type = asset_info.get("typeName", "")
    consumer_project_id = data.get("projectId", "")

    # Try Subscription Cancelled/Revoked structure
    if not asset_id:
        sub_listing = data.get("subscribedListing", {})
        item = sub_listing.get("item", {})
        asset_listing = item.get("assetListing", {})
        asset_id = asset_listing.get("entityId", "")
        asset_type = asset_listing.get("entityType", "")

    if not consumer_project_id:
        # singular subscribedPrincipal
        principal = data.get("subscribedPrincipal", {})
        consumer_project_id = principal.get("id", "")
        # also try owningProjectId from metadata
        if not consumer_project_id:
            consumer_project_id = metadata.get("owningProjectId", "")

    print(f"Revoke ({detail_type}): asset={asset_id}, type={asset_type}, "
          f"project={consumer_project_id}")

    if not asset_id:
        print("No asset ID in revoke event, skipping")
        return {"status": "skipped", "reason": "no asset id"}

    # Resolve table name and schema from asset
    try:
        asset = datazone.get_asset(
            domainIdentifier=DOMAIN_ID, identifier=asset_id)
        table_name = asset["name"]
        asset_type = asset.get("typeName", asset_type)
    except Exception as e:
        print(f"Failed to get asset {asset_id}: {e}")
        return {"status": "error", "reason": str(e)}

    if asset_type != "RedshiftTableAssetType":
        print(f"Not a Redshift table ({asset_type}), skipping revoke")
        return {"status": "skipped", "reason": "not redshift table"}

    schema_name = resolve_asset_schema(asset_id)

    # Resolve which roles to revoke — scoped to the subscription requester's groups.
    # For Grant Revoke Completed: metadata.user=SYSTEM, resolve from subscription.
    # For Subscription Cancelled: metadata.user may be an IAM user ID; also try
    # the subscriptionRequestId to find the original SSO requester.
    requester_idc_id = ""

    # Try to resolve from the subscription directly (most reliable)
    subscription_request_id = data.get("subscriptionRequestId", "")
    grant_id = metadata.get("id", "") if metadata.get("typeName", "") == "SubscriptionGrantEntityType" else ""
    subscription_id = ""
    if grant_id:
        subscription_id = resolve_subscription_id_from_grant(grant_id)
    if subscription_id:
        try:
            sub = datazone.get_subscription(
                domainIdentifier=DOMAIN_ID, identifier=subscription_id)
            requester_idc_id = sub.get("createdBy", "")
            print(f"Resolved revoke requester from subscription.createdBy: {requester_idc_id}")
        except Exception as e:
            print(f"Could not resolve requester from subscription: {e}")

    # For Subscription Cancelled, look up via subscriptionRequestId
    if not requester_idc_id and subscription_request_id:
        try:
            subs = datazone.list_subscriptions(
                domainIdentifier=DOMAIN_ID,
                subscriptionRequestIdentifier=subscription_request_id,
            )
            for sub in subs.get("items", []):
                requester_idc_id = sub.get("createdBy", "")
                if requester_idc_id:
                    print(f"Resolved revoke requester from subscription list: {requester_idc_id}")
                    break
        except Exception as e:
            print(f"Could not resolve requester from subscription request: {e}")

    # Final fallback to metadata.user
    if not requester_idc_id:
        requester_idc_id = metadata.get("user", "")

    revoke_roles = resolve_roles_for_requester(requester_idc_id)
    if revoke_roles is None:
        print("Requester not a real IdC user — falling back to project member resolution")
        targets = resolve_project_idc_groups(consumer_project_id)
        revoke_roles = targets.get("roles", [])
        tip_users = targets.get("tip_users", [])
    else:
        # Scope TIP user revoke to the requester only
        tip_users = resolve_tip_user_for_requester(
            requester_idc_id, consumer_project_id)

    if not revoke_roles and not tip_users:
        print("No revoke targets found — skipping REVOKE")
        return {"status": "skipped", "reason": "no revoke targets"}

    # Phase 1: Revoke from IdC group roles
    if revoke_roles:
        role_stmts = []
        for role in revoke_roles:
            role_stmts.append(
                f"REVOKE SELECT ON {schema_name}.{table_name} FROM ROLE {role}")
        role_sql = "; ".join(role_stmts) + ";"
        print(f"Phase 1 — role revokes: {revoke_roles}")
        ok, error = _exec_sql_and_wait(role_sql)
        if ok:
            print("  Role REVOKE successful")
        else:
            print(f"  Role REVOKE failed: {error}")

    # Phase 2: Revoke from TIP users
    if tip_users:
        user_stmts = []
        for tip_user in tip_users:
            user_stmts.append(
                f"REVOKE SELECT ON {schema_name}.{table_name} FROM {tip_user}")
        user_sql = "; ".join(user_stmts) + ";"
        print(f"Phase 2 — TIP user revokes: {tip_users}")
        ok, error = _exec_sql_and_wait(user_sql)
        if ok:
            print("  TIP user REVOKE successful")
        else:
            print(f"  TIP user REVOKE failed: {error}")

    all_targets = revoke_roles + tip_users
    return {"status": "revoked", "table": f"{schema_name}.{table_name}",
            "targets": all_targets}



###############################################################################
# Cleanup helpers for temporary resources
###############################################################################

def cleanup_temp_resources(consumer_project_id, env_id):
    """
    Find and delete all temporary resources (connection, secret, Redshift user)
    created by this Lambda in the consumer project.
    """
    print(f"Cleaning up temp resources in project {consumer_project_id}")

    ensure_project_membership(consumer_project_id)

    # Find temp connections (name starts with "temp-")
    try:
        resp = datazone.list_connections(
            domainIdentifier=DOMAIN_ID,
            projectIdentifier=consumer_project_id,
            type="REDSHIFT",
        )
        for conn in resp.get("items", []):
            conn_name = conn.get("name", "")
            conn_id = conn.get("connectionId", "")
            if conn_name.startswith("temp-"):
                print(f"  Deleting temp connection: {conn_id} ({conn_name})")
                cleanup_temp_connection(conn_id)
    except Exception as e:
        print(f"  Could not list connections for cleanup: {e}")

    # Find and delete temp secrets
    try:
        resp = secretsmanager.list_secrets(
            Filters=[
                {"Key": "tag-key", "Values": ["smus-temp-resource"]},
                {"Key": "tag-value", "Values": ["true"]},
            ],
        )
        for secret in resp.get("SecretList", []):
            secret_name = secret.get("Name", "")
            # Match by project ID prefix in the secret name
            project_prefix = consumer_project_id[:8]
            if project_prefix in secret_name:
                print(f"  Deleting temp secret: {secret_name}")
                # Extract the temp username before deleting
                try:
                    sv = secretsmanager.get_secret_value(SecretId=secret_name)
                    creds = json.loads(sv["SecretString"])
                    temp_user = creds.get("username", "")
                    if temp_user and temp_user.startswith(TEMP_PREFIX):
                        print(f"  Dropping temp Redshift user: {temp_user}")
                        # Query which schemas this temp user has access to
                        granted_schemas = get_user_schemas(temp_user)
                        for schema in granted_schemas:
                            exec_sql_wait(f"REVOKE ALL ON ALL TABLES IN SCHEMA {schema} FROM {temp_user}")
                            exec_sql_wait(f"REVOKE USAGE ON SCHEMA {schema} FROM {temp_user}")
                        exec_sql_wait(f"DROP USER IF EXISTS {temp_user}")
                except Exception as e:
                    print(f"  Could not extract temp user from secret: {e}")

                cleanup_temp_secret(secret["ARN"])
    except Exception as e:
        print(f"  Could not list secrets for cleanup: {e}")

    print("  Temp resource cleanup complete")


def cleanup_temp_connection(conn_id):
    """Delete a DataZone connection."""
    try:
        datazone.delete_connection(
            domainIdentifier=DOMAIN_ID,
            identifier=conn_id,
        )
        print(f"  Deleted connection: {conn_id}")
    except Exception as e:
        print(f"  Could not delete connection {conn_id}: {e}")


def cleanup_temp_secret(secret_arn):
    """Delete a Secrets Manager secret immediately."""
    try:
        secretsmanager.delete_secret(
            SecretId=secret_arn,
            ForceDeleteWithoutRecovery=True,
        )
        print(f"  Deleted secret: {secret_arn}")
    except Exception as e:
        print(f"  Could not delete secret {secret_arn}: {e}")


###############################################################################
# Helpers: Redshift SQL execution
###############################################################################

def exec_sql_wait(sql, timeout_secs=30):
    """Execute a Redshift SQL statement and wait for completion."""
    try:
        resp = redshift_data.execute_statement(
            ClusterIdentifier=CLUSTER_ID,
            Database=REDSHIFT_DB,
            DbUser=REDSHIFT_ADMIN_USER,
            Sql=sql,
        )
        stmt_id = resp["Id"]
        for _ in range(timeout_secs // 2):
            time.sleep(2)
            desc = redshift_data.describe_statement(Id=stmt_id)
            status = desc["Status"]
            if status == "FINISHED":
                return True
            if status in ("FAILED", "ABORTED"):
                error = desc.get("Error", "unknown")
                if "already exists" in error.lower():
                    print(f"  (already exists)")
                    return True
                print(f"  SQL failed: {error}")
                return False
        print(f"  SQL timed out after {timeout_secs}s")
        return False
    except Exception as e:
        print(f"  SQL execution error: {e}")
        return False


def _exec_sql_and_wait(sql, timeout_secs=30):
    """Execute SQL and return (success: bool, error: str|None)."""
    try:
        resp = redshift_data.execute_statement(
            ClusterIdentifier=CLUSTER_ID,
            Database=REDSHIFT_DB,
            DbUser=REDSHIFT_ADMIN_USER,
            Sql=sql,
        )
        stmt_id = resp["Id"]
        for _ in range(timeout_secs // 2):
            time.sleep(2)
            desc = redshift_data.describe_statement(Id=stmt_id)
            status = desc["Status"]
            if status == "FINISHED":
                return True, None
            if status in ("FAILED", "ABORTED"):
                error = desc.get("Error", "unknown")
                return False, error
        return False, "timed out"
    except Exception as e:
        return False, str(e)


def resolve_idc_redshift_roles():
    """
    Query Redshift for all roles matching the IdC namespace pattern (AWSIDC:*).
    Returns a list of quoted role names suitable for GRANT statements.
    """
    prefix = IDC_NAMESPACE + ":"
    sql = (
        f"SELECT role_name FROM svv_roles "
        f"WHERE role_name LIKE '{prefix}%'"
    )
    try:
        resp = redshift_data.execute_statement(
            ClusterIdentifier=CLUSTER_ID,
            Database=REDSHIFT_DB,
            DbUser=REDSHIFT_ADMIN_USER,
            Sql=sql,
        )
        stmt_id = resp["Id"]
        for _ in range(15):
            time.sleep(2)
            desc = redshift_data.describe_statement(Id=stmt_id)
            status = desc["Status"]
            if status == "FINISHED":
                result = redshift_data.get_statement_result(Id=stmt_id)
                roles = []
                for row in result.get("Records", []):
                    if row and row[0].get("stringValue"):
                        role_name = row[0]["stringValue"]
                        roles.append(f'"{role_name}"')
                if roles:
                    print(f"Found IdC Redshift roles: {roles}")
                    return roles
                break
            if status in ("FAILED", "ABORTED"):
                print(f"Role query failed: {desc.get('Error', 'unknown')}")
                break
    except Exception as e:
        print(f"Could not query IdC roles: {e}")
    print("No IdC Redshift roles found")
    return []


def resolve_project_idc_groups(project_id):
    """
    Resolve grant targets from a consumer project's SSO members.

    Returns a dict with two keys:
      - "roles": list of quoted AWSIDC:<GroupName> role names
      - "tip_users": list of quoted IAMR:user-<idcUserId>@<projectId> user names

    The subscription requester is often an IAM automation user (e.g. CLI),
    not the actual SSO user. So instead of resolving from the requester,
    we look at the project's SSO members, find their IdC groups, and
    return the corresponding AWSIDC:<GroupName> Redshift roles plus
    TIP user identities for per-user grants.

    Falls back to resolve_idc_redshift_roles() if no SSO members found.
    """
    identity_store_id = get_identity_store_id()
    if not identity_store_id:
        print("Could not resolve Identity Store ID, falling back to role query")
        return {"roles": resolve_idc_redshift_roles(), "tip_users": []}

    # List project members and find SSO users
    sso_user_ids = []
    try:
        resp = datazone.list_project_memberships(
            domainIdentifier=DOMAIN_ID,
            projectIdentifier=project_id,
        )
        for member in resp.get("members", []):
            user_info = member.get("memberDetails", {}).get("user", {})
            user_id = user_info.get("userId", "")
            if not user_id:
                continue
            try:
                profile = datazone.get_user_profile(
                    domainIdentifier=DOMAIN_ID,
                    userIdentifier=user_id,
                )
                if profile.get("type") == "SSO":
                    sso_details = profile.get("details", {}).get("sso", {})
                    username = sso_details.get("username", "")
                    if username:
                        # Resolve IdC user ID from username
                        idc_uid = find_idc_user_by_username(
                            identity_store_id, username)
                        if idc_uid:
                            sso_user_ids.append(idc_uid)
                            print(f"  SSO member: {username} -> IdC {idc_uid}")
                    else:
                        # Fallback: search_user_profiles for username
                        username = _search_sso_username(user_id)
                        if username:
                            idc_uid = find_idc_user_by_username(
                                identity_store_id, username)
                            if idc_uid:
                                sso_user_ids.append(idc_uid)
                                print(f"  SSO member (via search): "
                                      f"{username} -> IdC {idc_uid}")
            except Exception as e:
                print(f"  Could not check profile {user_id}: {e}")
    except Exception as e:
        print(f"Could not list project members: {e}")

    if not sso_user_ids:
        print("No SSO members found in project, falling back to role query")
        return {"roles": resolve_idc_redshift_roles(), "tip_users": []}

    # Build TIP user identities only if this is a TIP-enabled project
    # (has a permanent Redshift connection without credentials = IAM/IdC auth)
    tip_users = []
    if _is_tip_project(project_id):
        for idc_uid in sso_user_ids:
            tip_user = f"IAMR:user-{idc_uid}@{project_id}"
            tip_users.append(f'"{tip_user}"')
        print(f"TIP project detected — resolved TIP user identities: {tip_users}")
    else:
        print("Non-TIP project — skipping TIP user grants")

    # Resolve IdC groups for each SSO user
    all_groups = set()
    for idc_uid in sso_user_ids:
        try:
            paginator_token = None
            while True:
                kwargs = {
                    "IdentityStoreId": identity_store_id,
                    "MemberId": {"UserId": idc_uid},
                }
                if paginator_token:
                    kwargs["NextToken"] = paginator_token
                resp = identitystore.list_group_memberships_for_member(**kwargs)
                for membership in resp.get("GroupMemberships", []):
                    group_id = membership.get("GroupId", "")
                    if group_id:
                        try:
                            group = identitystore.describe_group(
                                IdentityStoreId=identity_store_id,
                                GroupId=group_id,
                            )
                            group_name = group.get("DisplayName", "")
                            if group_name:
                                all_groups.add(group_name)
                        except Exception:
                            pass
                paginator_token = resp.get("NextToken")
                if not paginator_token:
                    break
        except Exception as e:
            print(f"  Could not resolve groups for {idc_uid}: {e}")

    # Build Redshift role names
    grant_roles = []
    for group_name in all_groups:
        role_name = f"{IDC_NAMESPACE}:{group_name}"
        grant_roles.append(f'"{role_name}"')

    if not grant_roles:
        print("No IdC groups found for SSO members, falling back to role query")
        grant_roles = resolve_idc_redshift_roles()
    else:
        print(f"Resolved IdC group roles from project members: {grant_roles}")

    return {"roles": grant_roles, "tip_users": tip_users}

def resolve_roles_for_requester(requester_idc_user_id):
    """
    Resolve Redshift AWSIDC roles for a single IdC user.

    Looks up the user's IdC group memberships and returns only the
    AWSIDC:<GroupName> roles that actually exist in Redshift — so grants
    and revokes are scoped to the requesting user's groups, not all
    groups in the project.

    Returns a list of quoted role names e.g. ['"AWSIDC:DataAnalysts"'].
    Returns None if the requester is not a valid IdC user ID (e.g. "SYSTEM"),
    signalling callers to fall back to project member resolution.
    """
    if not requester_idc_user_id:
        print("No requester user ID — falling back to project member resolution")
        return None

    # requester_idc_user_id is a DataZone internal user ID.
    # Resolve it to an IdC user ID via get_user_profile.
    # If it's an IAM user (not SSO), return None to fall back to project members.
    try:
        profile = datazone.get_user_profile(
            domainIdentifier=DOMAIN_ID,
            userIdentifier=requester_idc_user_id,
        )
        profile_type = profile.get("type", "")
        if profile_type != "SSO":
            print(f"Requester {requester_idc_user_id} is {profile_type} (not SSO) — "
                  f"falling back to project member resolution")
            return None
        sso_username = profile.get("details", {}).get("sso", {}).get("username", "")
        if not sso_username:
            print(f"No SSO username for {requester_idc_user_id} — falling back")
            return None
        print(f"Requester SSO username: {sso_username}")
    except Exception as e:
        print(f"Could not get user profile for {requester_idc_user_id}: {e} — falling back")
        return None

    identity_store_id = get_identity_store_id()
    if not identity_store_id:
        print("Could not resolve Identity Store ID, falling back to role query")
        return resolve_idc_redshift_roles()

    # Resolve the IdC user ID from the SSO username
    requester_idc_user_id = find_idc_user_by_username(identity_store_id, sso_username)
    if not requester_idc_user_id:
        print(f"No IdC user found for {sso_username} — falling back to project members")
        return None

    identity_store_id = get_identity_store_id()
    if not identity_store_id:
        print("Could not resolve Identity Store ID, falling back to role query")
        return resolve_idc_redshift_roles()

    # Resolve all IdC groups for this user
    user_groups = set()
    try:
        paginator_token = None
        while True:
            kwargs = {
                "IdentityStoreId": identity_store_id,
                "MemberId": {"UserId": requester_idc_user_id},
            }
            if paginator_token:
                kwargs["NextToken"] = paginator_token
            resp = identitystore.list_group_memberships_for_member(**kwargs)
            for membership in resp.get("GroupMemberships", []):
                group_id = membership.get("GroupId", "")
                if group_id:
                    try:
                        group = identitystore.describe_group(
                            IdentityStoreId=identity_store_id,
                            GroupId=group_id,
                        )
                        group_name = group.get("DisplayName", "")
                        if group_name:
                            user_groups.add(group_name)
                    except Exception:
                        pass
            paginator_token = resp.get("NextToken")
            if not paginator_token:
                break
    except Exception as e:
        print(f"Could not resolve groups for user {requester_idc_user_id}: {e}")
        return resolve_idc_redshift_roles()

    if not user_groups:
        print(f"No IdC groups found for user {requester_idc_user_id}")
        return []

    print(f"IdC groups for requester {requester_idc_user_id}: {user_groups}")

    # Cross-reference with roles that actually exist in Redshift
    existing_roles = set(r.strip('"') for r in resolve_idc_redshift_roles())
    grant_roles = []
    for group_name in user_groups:
        role_name = f"{IDC_NAMESPACE}:{group_name}"
        if role_name in existing_roles:
            grant_roles.append(f'"{role_name}"')
            print(f"  Matched Redshift role: {role_name}")
        else:
            print(f"  No Redshift role for group: {group_name} (skipping)")

    return grant_roles




def resolve_tip_user_for_requester(requester_idc_id, project_id):
    """
    Build the TIP user identity for a single requester if the project is TIP-enabled.
    Returns a list with one quoted IAMR:user-<idcUserId>@<projectId> entry, or [].
    """
    if not requester_idc_id:
        return []
    if not _is_tip_project(project_id):
        print("Non-TIP project — skipping TIP user grant for requester")
        return []
    tip_user = f'"IAMR:user-{requester_idc_id}@{project_id}"'
    print(f"TIP user for requester: {tip_user}")
    return [tip_user]


def _is_tip_project(project_id):
    """
    Detect whether a project is TIP-enabled by checking if it has a permanent
    (non-temp) Redshift connection without credentials (IAM/IdC auth).
    Non-TIP projects use AWSIDC:* roles; TIP projects use IAMR:user-* identities.
    """
    try:
        resp = datazone.list_connections(
            domainIdentifier=DOMAIN_ID,
            projectIdentifier=project_id,
            type="REDSHIFT",
        )
        for conn in resp.get("items", []):
            name = conn.get("name", "")
            # Skip temporary connections created by this Lambda
            if name.startswith("temp-"):
                continue
            # A permanent Redshift connection exists — check for credentials
            conn_id = conn.get("connectionId", "")
            try:
                detail = datazone.get_connection(
                    domainIdentifier=DOMAIN_ID,
                    identifier=conn_id,
                )
                props = detail.get("props", {}).get("redshiftProperties", {})
                creds = props.get("credentials", {})
                secret_arn = creds.get("secretArn", "")
                if not secret_arn:
                    # No credentials = IAM auth = TIP project
                    print(f"  TIP detection: connection {name} has no credentials")
                    return True
            except Exception:
                pass
    except Exception as e:
        print(f"  TIP detection failed: {e}")
    return False


def _search_sso_username(profile_id):
    """Search user profiles to find SSO username when get_user_profile returns empty."""
    try:
        resp = datazone.search_user_profiles(
            domainIdentifier=DOMAIN_ID,
            userType="SSO_USER",
            maxResults=50,
        )
        for p in resp.get("items", []):
            if p.get("id") == profile_id:
                username = p.get("details", {}).get("sso", {}).get("username", "")
                if username:
                    print(f"  Resolved username via search: {username}")
                    return username
    except Exception as e:
        print(f"  search_user_profiles failed: {e}")
    return ""


def get_user_schemas(username):
    """
    Query Redshift to find which schemas a user has USAGE privilege on.
    Falls back to ['public'] if the query fails.
    """
    sql = (
        f"SELECT DISTINCT nspname FROM pg_namespace n "
        f"JOIN pg_user u ON 1=1 "
        f"WHERE u.usename = '{username}' "
        f"AND has_schema_privilege(u.usename, n.nspname, 'USAGE') "
        f"AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')"
    )
    try:
        resp = redshift_data.execute_statement(
            ClusterIdentifier=CLUSTER_ID,
            Database=REDSHIFT_DB,
            DbUser=REDSHIFT_ADMIN_USER,
            Sql=sql,
        )
        stmt_id = resp["Id"]
        for _ in range(15):
            time.sleep(2)
            desc = redshift_data.describe_statement(Id=stmt_id)
            status = desc["Status"]
            if status == "FINISHED":
                result = redshift_data.get_statement_result(Id=stmt_id)
                schemas = []
                for row in result.get("Records", []):
                    if row and row[0].get("stringValue"):
                        schemas.append(row[0]["stringValue"])
                if schemas:
                    print(f"  User {username} has access to schemas: {schemas}")
                    return schemas
                break
            if status in ("FAILED", "ABORTED"):
                print(f"  Schema query failed: {desc.get('Error', 'unknown')}")
                break
    except Exception as e:
        print(f"  Could not query schemas for {username}: {e}")
    return ["public"]


def generate_password():
    """Generate a secure random password for temporary Redshift users."""
    chars = string.ascii_letters + string.digits + "!@#%^&*"
    pwd = (
        secrets.choice(string.ascii_uppercase)
        + secrets.choice(string.ascii_lowercase)
        + secrets.choice(string.digits)
        + secrets.choice("!@#%^&*")
        + "".join(secrets.choice(chars) for _ in range(12))
    )
    return pwd


###############################################################################
# Helpers: grant -> subscription resolution
###############################################################################

def resolve_subscription_id_from_grant(grant_id):
    """
    Resolve the subscriptionId from a subscription grant.
    The 'Subscription Grant Completed' event doesn't include subscriptionId
    in its payload, so we fetch it from the grant object.
    """
    if not grant_id:
        return ""
    try:
        grant = datazone.get_subscription_grant(
            domainIdentifier=DOMAIN_ID,
            identifier=grant_id,
        )
        sub_id = grant.get("subscriptionId", "")
        if sub_id:
            print(f"Resolved subscriptionId from grant: {sub_id}")
        return sub_id
    except Exception as e:
        print(f"Could not resolve subscriptionId from grant {grant_id}: {e}")
        return ""


###############################################################################
# Helpers: subscription target management
###############################################################################

def find_tooling_environment(project_id):
    """Find the Tooling environment ID in a project."""
    try:
        resp = datazone.list_environments(
            domainIdentifier=DOMAIN_ID,
            projectIdentifier=project_id,
        )
        for env in resp.get("items", []):
            name = env.get("name", "")
            if "Tooling" in name or "tooling" in name.lower():
                env_id = env.get("id", "")
                print(f"Found Tooling environment: {env_id} ({name})")
                return env_id
        for env in resp.get("items", []):
            if env.get("status") == "ACTIVE":
                env_id = env.get("id", "")
                print(f"Using first active environment: {env_id}")
                return env_id
    except Exception as e:
        print(f"Could not list environments for project {project_id}: {e}")
    return None


def find_redshift_subscription_target(project_id, env_id):
    """Check if a Redshift subscription target already exists."""
    try:
        resp = datazone.list_subscription_targets(
            domainIdentifier=DOMAIN_ID,
            environmentIdentifier=env_id,
        )
        for target in resp.get("items", []):
            asset_types = target.get("applicableAssetTypes", [])
            if "RedshiftTableAssetType" in asset_types:
                return target.get("id", "")
    except Exception as e:
        print(f"Could not list subscription targets: {e}")
    return None


def find_connection_secret(project_id):
    """Find the secret ARN from the first Redshift connection in the project."""
    try:
        resp = datazone.list_connections(
            domainIdentifier=DOMAIN_ID,
            projectIdentifier=project_id,
            type="REDSHIFT",
        )
        for conn in resp.get("items", []):
            conn_id = conn.get("connectionId", "")
            try:
                detail = datazone.get_connection(
                    domainIdentifier=DOMAIN_ID,
                    identifier=conn_id,
                )
                props = detail.get("props", {})
                rs_props = props.get("redshiftProperties", {})
                creds = rs_props.get("credentials", {})
                secret_arn = creds.get("secretArn", "")
                if secret_arn:
                    print(f"Found connection secret: {secret_arn}")
                    return secret_arn
            except Exception as e:
                print(f"Could not read connection {conn_id}: {e}")
    except Exception as e:
        print(f"Could not list connections for project {project_id}: {e}")
    return None





def get_identity_store_id():
    """Get the Identity Store ID from the IdC instance (cached per cold start)."""
    global _identity_store_id
    if _identity_store_id is None:
        try:
            resp = sso_admin.list_instances()
            instances = resp.get("Instances", [])
            if instances:
                _identity_store_id = instances[0].get("IdentityStoreId", "")
                print(f"Identity Store ID: {_identity_store_id}")
        except Exception as e:
            print(f"Could not get Identity Store ID: {e}")
    return _identity_store_id

def find_idc_user_by_username(identity_store_id, username):
    """
    Look up an IdC user by their username (typically email) in the identity store.
    Returns the IdC UserId or None.
    """
    try:
        # Try exact match on UserName first
        resp = identitystore.list_users(
            IdentityStoreId=identity_store_id,
            Filters=[{
                "AttributePath": "UserName",
                "AttributeValue": username,
            }],
        )
        users = resp.get("Users", [])
        if users:
            user_id = users[0].get("UserId", "")
            print(f"Found IdC user by UserName '{username}': {user_id}")
            return user_id

        # Fallback: try matching by email in ExternalIds or Emails
        resp = identitystore.list_users(
            IdentityStoreId=identity_store_id,
        )
        for user in resp.get("Users", []):
            emails = user.get("Emails", [])
            for email_entry in emails:
                if email_entry.get("Value", "").lower() == username.lower():
                    user_id = user.get("UserId", "")
                    print(f"Found IdC user by email '{username}': {user_id}")
                    return user_id
            if user.get("UserName", "").lower() == username.lower():
                user_id = user.get("UserId", "")
                print(f"Found IdC user by case-insensitive UserName: {user_id}")
                return user_id

        print(f"No IdC user found for username '{username}'")
        return None

    except Exception as e:
        print(f"Error looking up IdC user '{username}': {e}")
        return None




###############################################################################
# Helpers: IAM / project membership
###############################################################################

def get_lambda_role_arn():
    """Get the IAM role ARN of this Lambda function (cached per cold start)."""
    global _lambda_role_arn
    if _lambda_role_arn is None:
        caller = sts.get_caller_identity()
        assumed_arn = caller["Arn"]
        parts = assumed_arn.split(":")
        account = parts[4]
        role_name = parts[5].split("/")[1]
        _lambda_role_arn = f"arn:aws:iam::{account}:role/{role_name}"
        print(f"Lambda role ARN: {_lambda_role_arn}")
    return _lambda_role_arn


def get_account_id():
    """Get the AWS account ID (cached per cold start)."""
    global _account_id
    if _account_id is None:
        caller = sts.get_caller_identity()
        _account_id = caller["Account"]
    return _account_id


def ensure_project_membership(project_id):
    """
    Add the Lambda's IAM role as PROJECT_CONTRIBUTOR to the given project.
    Idempotent -- silently succeeds if already a member.
    """
    role_arn = get_lambda_role_arn()
    try:
        datazone.create_project_membership(
            domainIdentifier=DOMAIN_ID,
            projectIdentifier=project_id,
            member={"userIdentifier": role_arn},
            designation="PROJECT_CONTRIBUTOR",
        )
        print(f"Added self as contributor to project {project_id}")
    except datazone.exceptions.ConflictException:
        pass
    except Exception as e:
        print(f"Could not add self to project {project_id}: {e}")