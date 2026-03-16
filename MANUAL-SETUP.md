# Manual Console Setup Guide

This guide walks through setting up SMUS ↔ Redshift integration entirely via the AWS Console, without using the automation scripts.

## Prerequisites

- An existing Amazon Redshift provisioned cluster
- A SageMaker Unified Studio (SMUS) domain with an admin project
- IAM Identity Center (IdC) enabled in the same region
- IdC users and groups already created (or you'll create them in Step 1)
- Admin access to the AWS account

### Gather These IDs Before You Start

You'll need these resource IDs throughout the guide. Here's where to find each one:

| ID | Where to find it |
|----|-----------------|
| AWS Account ID | Top-right corner of any AWS Console page, or **IAM → Dashboard** |
| Region | Top-right dropdown in the AWS Console (e.g. `us-east-1`) |
| Redshift Cluster ID | **Amazon Redshift → Clusters** → the cluster name in the list |
| Redshift Endpoint (host) | **Redshift → Clusters → your cluster → Properties** → "Endpoint" field (just the hostname, without `:5439/dev`) |
| VPC ID | **Redshift → Clusters → your cluster → Properties** → "Network and security" section → VPC ID |
| Redshift Security Group ID | **Redshift → Clusters → your cluster → Properties** → "Network and security" → VPC security groups (e.g. `sg-0abc123`) |
| DataZone Domain ID | **SageMaker Unified Studio → Admin settings** → Domain ID shown at the top (starts with `dzd-`) |
| Admin Project ID | **SMUS → Projects** → click the admin project → the project ID is in the URL or project details page |
| Admin Environment ID | **SMUS → Admin project → Environments** → click the Tooling environment → the environment ID is in the URL or details (starts with a hex string) |
| SMUS Project Security Group | **SMUS → Admin project → Environments → Tooling environment → Provisioned resources** → look for the security group ID |
| IdC Instance ARN | **IAM Identity Center → Settings** → Instance ARN |
| Identity Store ID | **IAM Identity Center → Settings** → Identity Store ID |
| IdC User ID | **IAM Identity Center → Users** → click a user → User ID shown in details |
| IdcManagedApplicationArn (SSO App ARN) | **Redshift → Clusters → your cluster → Properties → IdC integration** → "IdC managed application ARN" field. Looks like `arn:aws:sso::<ACCOUNT>:application/ssoins-xxx/apl-xxx` |
| SageMaker Manage Role ARN | **IAM → Roles** → search for `AmazonSageMakerManageAccess-<region>-<domain-id>` → copy the ARN |
| DataZone User Role Name | Pattern: `datazone_usr_role_<project-id>_<environment-id>`. Find the project and environment IDs first, then search in **IAM → Roles** |
| Root Domain Unit ID | **SMUS → Admin settings → Domain units** → the root unit is the top-level entry. The ID is visible in the URL when you click it |

## Step 1: Set Up IdC ↔ Redshift Integration

### 1.1 Create an IAM Role for Redshift IdC

1. Go to **IAM → Roles → Create role**
2. Trusted entity: **Custom trust policy**
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [{
       "Effect": "Allow",
       "Principal": { "Service": "redshift.amazonaws.com" },
       "Action": "sts:AssumeRole"
     }]
   }
   ```
3. Add an inline policy named `IdCAccess`:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [{
       "Effect": "Allow",
       "Action": ["sso:*", "sso-oauth:*", "identitystore:*"],
       "Resource": "*"
     }]
   }
   ```
4. Name the role `RedshiftIdCIntegrationRole`

### 1.2 Attach the Role to Your Redshift Cluster

1. Go to **Amazon Redshift → Clusters → your cluster**
2. Click **Properties** tab → **Manage IAM roles**
3. Add `RedshiftIdCIntegrationRole` and save

### 1.3 Enable IdC Integration on the Cluster

1. In the Redshift console, go to your cluster → **Properties**
2. Under **IAM Identity Center integration**, click **Enable**
3. Select the IAM role you just created
4. This auto-creates three IdC applications: Redshift, QEV2 (SQL Workbench), and Console TIP

### 1.4 Create IdC Groups and Assign Users

1. Go to **IAM Identity Center → Groups → Create group**
2. Create groups like `DataEngineers`, `DataAnalysts`
3. Add users to each group
4. Go to **Applications** — you should see the three auto-created apps (Redshift, QEV2, Console TIP)
5. For each app, click it → **Assign users and groups** → add your groups and individual users

> Users need assignments on all three apps. Missing QEV2 assignment causes "Invalid scope" errors.

### 1.5 Create a Permission Set

1. Go to **IAM Identity Center → Permission sets → Create permission set**
2. Name: `RedshiftQueryAccess`, session duration: 8 hours
3. Attach managed policies:
   - `AmazonRedshiftFullAccess`
   - `AmazonRedshiftQueryEditorV2FullAccess`
4. Add an inline policy for broad console access:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       { "Effect": "Allow", "Action": ["redshift:*", "redshift-data:*", "redshift-serverless:*", "sqlworkbench:*"], "Resource": "*" },
       { "Effect": "Allow", "Action": ["sso:*", "sso-oauth:*", "sts:*"], "Resource": "*" },
       { "Effect": "Allow", "Action": ["secretsmanager:ListSecrets", "secretsmanager:GetSecretValue", "ec2:Describe*", "cloudwatch:GetMetricData", "iam:ListRoles", "iam:GetRole"], "Resource": "*" }
     ]
   }
   ```
5. Go to **AWS accounts** → select your account → **Assign users or groups** → assign your IdC groups with this permission set

### 1.6 Register the Identity Provider in Redshift

1. Open **Redshift Query Editor v2** (or use `redshift-data` API)
2. Connect as admin and run:
   ```sql
   CREATE IDENTITY PROVIDER "your-cluster-idc"
     TYPE AWSIDC
     NAMESPACE 'AWSIDC'
     APPLICATION_ARN 'arn:aws:sso::<ACCOUNT>:application/ssoins-xxx/apl-xxx'
     IAM_ROLE 'arn:aws:iam::<ACCOUNT>:role/RedshiftIdCIntegrationRole';
   ```

> Use the `IdcManagedApplicationArn` (SSO app ARN), NOT the Redshift IdC application ARN. Using the wrong one gives "Failed to obtain AWS IdC Info".
>
> To find the correct ARN: **Redshift → Clusters → your cluster → Properties → IAM Identity Center integration** → copy the "IdC managed application ARN". It looks like `arn:aws:sso::<ACCOUNT>:application/ssoins-xxx/apl-xxx`.

### 1.7 Create Redshift Roles and Grant Access

```sql
-- Create roles matching your IdC groups
CREATE ROLE "AWSIDC:DataEngineers";
CREATE ROLE "AWSIDC:DataAnalysts";

-- Grant per-table access
GRANT USAGE ON SCHEMA public TO ROLE "AWSIDC:DataEngineers";
GRANT SELECT ON public.employees TO ROLE "AWSIDC:DataEngineers";
GRANT ALL ON public.departments TO ROLE "AWSIDC:DataEngineers";

GRANT USAGE ON SCHEMA public TO ROLE "AWSIDC:DataAnalysts";
GRANT SELECT ON public.employees TO ROLE "AWSIDC:DataAnalysts";
```

> Grant table-level (not schema-level) for proper data isolation between groups.


## Step 2: Publish Redshift Tables as DataZone Assets

### 2.1 Create a Secrets Manager Secret

1. Go to **Secrets Manager → Store a new secret**
2. Secret type: **Other type of secret**
3. Key/value pairs:
   - `username`: your Redshift admin user (e.g. `awsuser`)
   - `password`: your Redshift admin password
4. Name: `smus-redshift-<cluster-id>-<timestamp>`
5. After creation, add these tags:
   - `AmazonDataZoneProject` = `<admin-project-id>` — find in **SMUS → Projects → admin project** → project ID in URL or details
   - `AmazonDataZoneDomain` = `<domain-id>` — find in **SMUS → Admin settings** → Domain ID (starts with `dzd-`)
   - `AmazonDataZoneEnvironment` = `<admin-env-id>` — find in **SMUS → Admin project → Environments → Tooling** → environment ID in URL
   - `AmazonDataZoneCreatedVia` = `SageMakerUnifiedStudio`
6. Add a resource policy allowing the DataZone user role and SageMaker manage role to call `secretsmanager:GetSecretValue`:
   - DataZone user role ARN: `arn:aws:iam::<ACCOUNT>:role/datazone_usr_role_<admin-project-id>_<admin-env-id>` — find in **IAM → Roles**, search for `datazone_usr_role`
   - SageMaker manage role ARN: `arn:aws:iam::<ACCOUNT>:role/service-role/AmazonSageMakerManageAccess-<region>-<domain-id>` — find in **IAM → Roles**, search for `AmazonSageMakerManageAccess`

### 2.2 Configure Security Group Rules

1. Go to **EC2 → Security Groups** → find your Redshift cluster's SG (go to **Redshift → Clusters → your cluster → Properties → Network and security** to get the SG ID)
2. Add an inbound rule:
   - Type: Custom TCP
   - Port: 5439
   - Source: the SMUS project security group (find it in **SMUS → Admin project → Environments → Tooling → Provisioned resources** → look for the security group)

### 2.3 Create a Secrets Manager VPC Endpoint

1. Go to **VPC → Endpoints → Create endpoint**
2. Service: `com.amazonaws.<region>.secretsmanager`
3. Type: Interface
4. VPC: same as your Redshift cluster (find VPC ID in **Redshift → Clusters → your cluster → Properties → Network and security**)
5. Subnets: select a subnet in the same AZ as your Redshift cluster (find the AZ in **Redshift → Clusters → your cluster → Properties → Network and security → Availability Zone**)
6. Security groups: add both the SMUS SG and Redshift SG (see step 2.2 for how to find these)
7. Enable private DNS

### 2.4 Grant IAM Permissions

1. Go to **IAM → Roles** → search for `datazone_usr_role` → find the one matching your admin project and environment: `datazone_usr_role_<admin-project-id>_<admin-env-id>`
2. Add an inline policy `RedshiftDataSourceAccess`:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [{
       "Effect": "Allow",
       "Action": [
         "redshift-data:ListDatabases", "redshift-data:ListSchemas", "redshift-data:ListTables",
         "redshift-data:DescribeTable", "redshift-data:ExecuteStatement",
         "redshift-data:DescribeStatement", "redshift-data:GetStatementResult",
         "redshift:GetClusterCredentials", "redshift:DescribeClusters"
       ],
       "Resource": "*"
     }]
   }
   ```
3. Find the SageMaker manage role: **IAM → Roles** → search for `AmazonSageMakerManageAccess` → select the one matching your region and domain ID: `AmazonSageMakerManageAccess-<region>-<domain-id>`
4. Add an inline policy `SecretsManagerRedshiftAccess` allowing `secretsmanager:GetSecretValue` on `arn:aws:secretsmanager:<region>:<account>:secret:smus-redshift-*`

### 2.5 Create a DataZone Redshift Connection

1. Open **SageMaker Unified Studio** → go to your admin project
2. Navigate to **Data** → **Connections** → **Create connection**
3. Type: Redshift
4. Provide: cluster name, host, port (5439), database (`dev`), and select the secret you created
5. Save the connection

### 2.6 Create a Data Source and Run Import

1. In the admin project, go to **Data** → **Data sources** → **Create data source**
2. Type: Redshift
3. Select the connection you just created
4. Scope: database `dev`, schema `public`
5. Enable **Publish on import**
6. Save and click **Run** to start the import
7. Wait for the run to complete — each table becomes a published asset in the catalog

### 2.7 Require Approval for Subscriptions

After import, for each published asset:
1. Go to the asset in the catalog
2. Edit → set **Approval required** to YES
3. This ensures subscriptions go through the approval workflow (needed for the grant automation)

## Step 3: Create a TIP Project (Mandatory for Per-User Grants)

Grants are provisioned through a Trusted Identity Propagation (TIP) project on the SageMaker domain. This is a required step.

### Why TIP?

- Without TIP, all users in a project share the same IAM role and see the same data
- With TIP, each user gets their own Redshift identity, enabling per-user table-level access control
- TIP provides end-to-end identity propagation from the browser through SMUS/DataZone down to Redshift
- Per-user audit trails — Redshift knows exactly which user ran each query

### 3.1 Enable TIP on a Project Profile

1. Open **SageMaker Unified Studio** → **Admin settings** → **Project profiles**
2. Find the **SQL analytics** profile → **Edit**
3. Under the **Tooling** blueprint parameters, set `enableTrustedIdentityPropagationPermissions` to `true`
4. Save the profile

> TIP only works for projects created AFTER enabling it. Existing projects won't get TIP.

### 3.2 Create a New Project Using the TIP Profile

1. Go to **Projects** → **Create project**
2. Select the **SQL analytics** profile (now TIP-enabled)
3. Name it (e.g. `tip-redshift-project`)
4. Wait for both environments (Tooling + Lakehouse) to become ACTIVE

### 3.3 Create a Redshift IAM Connection (No Credentials)

1. In the new TIP project, go to **Data** → **Connections** → **Create connection**
2. Type: Redshift
3. Provide: cluster name, host, port, database
4. Do NOT provide credentials — TIP uses IAM/IdC identity propagation
5. Save

### 3.4 Configure Lake Formation IdC Integration

1. Go to **AWS Lake Formation** → **Administration** → **IAM Identity Center**
2. Click **Enable** if not already configured
3. Select your IdC instance
4. After enabling, go to the Lake Formation IdC application in IAM Identity Center
5. Assign your IdC groups and users to this application

### 3.5 Add Users as Project Members

1. In the TIP project, go to **Members** → **Add member**
2. Search for each IdC user and add them as **Project Contributor**

### 3.6 Grant IAM Permissions to the Project Role

1. Go to **IAM → Roles** → search for `datazone_usr_role` → find the one matching your TIP project and its Tooling environment: `datazone_usr_role_<tip-project-id>_<tooling-env-id>`. You can find the project ID in the SMUS project URL and the environment ID in **SMUS → TIP project → Environments → Tooling**.
2. Add an inline policy `TIPRedshiftAccess`:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       { "Effect": "Allow", "Action": ["redshift:*", "redshift-data:*", "redshift-serverless:*"], "Resource": "*" },
       { "Effect": "Allow", "Action": ["sso:*", "sso-oauth:*"], "Resource": "*" },
       { "Effect": "Allow", "Action": "sts:*", "Resource": "*" }
     ]
   }
   ```
3. Add the project role as a **Lake Formation admin** (Lake Formation → Administrative roles → Add)

> Missing `redshift:GetClusterCredentialsWithIAM` causes CredentialsProviderError.

### 3.7 Create Redshift Users for TIP Identities

In Redshift Query Editor v2 (as admin):

```sql
-- Pattern: IAMR:user-<idcUserId>@<projectId>
-- Find IdC User IDs: IAM Identity Center → Users → click a user → User ID
-- Find Project ID: SMUS → Projects → click the TIP project → ID in URL or details
CREATE USER "IAMR:user-d418f428-xxxx@abc123projectid" PASSWORD DISABLE;
CREATE USER "IAMR:user-84f83498-xxxx@abc123projectid" PASSWORD DISABLE;
```

### 3.8 Grant Per-User Table Access

```sql
-- User A gets employees only
GRANT USAGE ON SCHEMA public TO "IAMR:user-d418f428-xxxx@abc123projectid";
GRANT SELECT ON public.employees TO "IAMR:user-d418f428-xxxx@abc123projectid";

-- User B gets departments only
GRANT USAGE ON SCHEMA public TO "IAMR:user-84f83498-xxxx@abc123projectid";
GRANT SELECT ON public.departments TO "IAMR:user-84f83498-xxxx@abc123projectid";
```

> You can also use a regular (non-TIP) consumer project — the Lambda handles group-level grants automatically. But TIP is recommended for per-user isolation.

## Step 4: Deploy the Grant Automation (Lambda + EventBridge)

### 4.1 Create the Lambda Execution Role

1. Go to **IAM → Roles → Create role**
2. Trusted entity: **AWS service → Lambda**
3. Attach managed policy: `AWSLambdaBasicExecutionRole`
4. Add an inline policy `RedshiftGrantAccess` with permissions for:
   - `redshift-data:ExecuteStatement`, `redshift-data:DescribeStatement`, `redshift-data:GetStatementResult`, `redshift:GetClusterCredentials`
   - `datazone:*`
   - `sts:GetCallerIdentity`
   - `identitystore:ListGroupMembershipsForMember`, `identitystore:DescribeGroup`, `identitystore:ListUsers`, `identitystore:DescribeUser`
   - `sso-admin:ListInstances`, `sso:ListInstances`
   - `secretsmanager:GetSecretValue`, `secretsmanager:CreateSecret`, `secretsmanager:DeleteSecret`, `secretsmanager:TagResource`, `secretsmanager:PutResourcePolicy`, `secretsmanager:ListSecrets`
   - `iam:PassRole` on the SageMaker manage role (find the ARN: **IAM → Roles** → search `AmazonSageMakerManageAccess-<region>-<domain-id>`)
5. Name the role `smus-redshift-grant-lambda-role`

### 4.2 Tag the SageMaker Manage Role

1. Go to **IAM → Roles** → search for `AmazonSageMakerManageAccess-<region>-<domain-id>`
2. Click the role → **Tags** tab → **Manage tags**
3. Add tag: `RedshiftDbRoles` = comma-separated list of your Redshift DB roles (e.g. `AWSIDC:DataEngineers,AWSIDC:DataAnalysts`)
   - To find existing Redshift roles, run in Query Editor v2: `SELECT role_name FROM svv_roles WHERE role_name NOT LIKE 'sys:%' AND role_name NOT LIKE 'ds:%';`

### 4.3 Add Lambda Role to DataZone

1. In SMUS, go to the admin project → **Members** → add the Lambda role ARN as **Project Contributor** (copy the ARN from **IAM → Roles → smus-redshift-grant-lambda-role**)
2. In the domain settings, add the Lambda role as a **Domain unit owner** on the root domain unit:
   - Go to **SMUS → Admin settings → Domain units** → click the root (top-level) unit
   - Add the Lambda role ARN as an owner
   - This is needed for the Lambda to self-add to consumer projects at runtime

### 4.4 Create the Lambda Function

1. Go to **Lambda → Create function**
2. Runtime: Python 3.12
3. Handler: `redshift_grant_handler.lambda_handler`
4. Execution role: `smus-redshift-grant-lambda-role`
5. Upload the `lambda/redshift_grant_handler.py` file as a zip
6. Timeout: 900 seconds (15 min)
7. Set environment variables:
   - `REDSHIFT_CLUSTER_ID` = your cluster ID (from **Redshift → Clusters**)
   - `REDSHIFT_DATABASE` = `dev` (or your database name)
   - `REDSHIFT_ADMIN_USER` = your admin user (e.g. `awsuser`)
   - `REDSHIFT_HOST` = cluster endpoint hostname (from **Redshift → Clusters → your cluster → Properties → Endpoint**, just the hostname without `:5439/dev`)
   - `DOMAIN_ID` = your DataZone domain ID (from **SMUS → Admin settings**, starts with `dzd-`)
   - `ADMIN_PROJECT_ID` = your admin project ID (from **SMUS → Projects → admin project** URL or details)
   - `IDC_NAMESPACE` = `AWSIDC`

### 4.5 Create the EventBridge Rule

1. Go to **EventBridge → Rules → Create rule**
2. Event bus: default
3. Event pattern:
   ```json
   {
     "source": ["aws.datazone"],
     "detail-type": [
       "Subscription Request Accepted",
       "Subscription Grant Completed",
       "Subscription Revoked",
       "Subscription Cancelled",
       "Subscription Grant Revoke Completed"
     ]
   }
   ```
4. Target: the Lambda function you just created
5. Enable the rule

## Step 5: Create Consumer Projects

### 5.1 Create a Project

1. In SMUS, go to **Projects** → **Create project**
2. Select the **SQL analytics** profile
3. Name it (e.g. `DataAnalysts-consumer`)
4. Wait for environments to provision

### 5.2 Add Members

1. Go to the project → **Members** → add IdC users as **Project Contributor**

### 5.3 Grant Consumer Role Access to Admin Secret

1. Go to **Secrets Manager** → find the admin secret (named `smus-redshift-<cluster-id>-<timestamp>`, or filter by tag `AmazonDataZoneProject` = your admin project ID)
2. Click the secret → **Resource permissions** → **Edit**
3. Add the consumer project's DataZone user role to the allowed principals:
   - Role name pattern: `datazone_usr_role_<consumer-project-id>_<consumer-tooling-env-id>`
   - Find the consumer project ID in **SMUS → Projects → consumer project** URL
   - Find the Tooling environment ID in **SMUS → Consumer project → Environments → Tooling**
   - Then look up the full role ARN in **IAM → Roles** → search for `datazone_usr_role`

## Step 6: Subscribe to Assets

### 6.1 Subscribe from a Regular (Non-TIP) Consumer Project

1. In the consumer project, go to the **Data catalog**
2. Find the Redshift table you want access to
3. Click **Subscribe** → provide a reason
4. An admin approves the subscription request
5. The Lambda automatically:
   - Creates a temporary connection in the consumer project
   - Creates a subscription target
   - Triggers grant fulfillment
   - Executes group-level Redshift GRANT statements (Phase 1 only — grants to `AWSIDC:<GroupName>` roles)
   - Cleans up temporary resources
6. The consumer team can now query the table using their IdC group's Redshift role

### 6.2 Subscribe from a TIP Project

TIP projects do not support the subscription/publish workflow natively yet. Instead, grants are applied directly to per-user Redshift identities.

There are two ways to grant access in a TIP project:

**Option A: Manual grants (no subscription needed)**

If you already created TIP users and grants in Step 3.7–3.8, users can query tables immediately through the direct Redshift connection (`dev` → `public`) without subscribing.

**Option B: Subscribe + Lambda automation**

If the TIP project also has a subscription target (created by the Lambda), the Lambda handles all three phases:

1. In the TIP project, go to the **Data catalog**
2. Find the Redshift table → click **Subscribe** → provide a reason
3. An admin approves the subscription request
4. The Lambda automatically:
   - Creates a temporary connection and subscription target
   - Resolves the requester from `subscription.createdBy` (the DataZone user who raised the request). If the requester is an SSO user, grants are scoped to their IdC groups and TIP identity only. If the requester is an IAM/automation user, falls back to all project SSO members.
   - **Phase 1**: Grants to the requester's `AWSIDC:<GroupName>` roles only (e.g. `AWSIDC:DataEngineers` if the requester is in DataEngineers)
   - **Phase 2**: Creates the requester's per-user Redshift identity (`IAMR:user-<requesterIdcId>@<projectId>`) with `PASSWORD DISABLE`
   - **Phase 3**: Grants `SELECT` on the table to the requester's TIP user identity only
   - Cleans up temporary resources
5. When the subscription is revoked or cancelled, the Lambda automatically revokes the same scoped grants
6. Each user can now query only the tables they've been individually granted access to

> For requester-scoped grants to work, raise the subscription from the SMUS UI as the SSO user — not via CLI. CLI subscriptions use an IAM user identity and fall back to granting all project SSO members.

> The Lambda auto-detects TIP projects by checking if the project has a permanent Redshift connection without credentials (IAM auth). This is the connection you created in Step 3.3.

### Important TIP Limitations

- Per-user isolation only works on the **direct Redshift connection path** (`dev` → `public`), not the federated catalog path (`dev@cluster` → `public`)
- The Lakehouse data explorer tree uses the project IAM role for browsing, so all users see the same tables in the tree — isolation is enforced at query time
- Users should use the direct `dev` database path for isolated queries

## Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| "Invalid scope" in QEV2 | Missing QEV2 or Console TIP app assignment | Assign users/groups to all three IdC apps |
| "Failed to obtain AWS IdC Info" | Wrong ARN in CREATE IDENTITY PROVIDER | Use `IdcManagedApplicationArn`, not the Redshift app ARN |
| "Session expired" in QEV2 | IdC token expired | Right-click cluster → Refresh |
| "User info not retrieved" | Wrong region | Access QEV2 in the same region as the cluster |
| CredentialsProviderError | Missing IAM permission | Add `redshift:GetClusterCredentialsWithIAM` to project role |
| "LakeFormation Identity Center Configuration not configured" | Lake Formation IdC not enabled | Enable IdC in Lake Formation console |
| Cookie errors | Third-party cookies blocked | Allow cookies for amazonaws.com |
| TIP users see all tables in tree | Expected — tree uses project IAM role | Per-user isolation only applies on direct `dev` → `public` path |
