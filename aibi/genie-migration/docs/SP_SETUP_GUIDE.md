# Service Principal Setup for Genie Migration

This guide is supplemental and covers only service principal provisioning and workspace permissions.
For the canonical migration workflow and command execution, use [README.md](../README.md).

- Workflow overview: [Cross-Workspace Migration Workflow](../README.md#cross-workspace-migration-workflow)
- Run commands: [Command Reference](../README.md#command-reference)

## Service Principal Strategy

You can use either of these patterns:

- One service principal added to both source and target workspaces
- Separate service principals per workspace/target (recommended for least privilege and environment isolation)

If you use separate principals, repeat the creation and permission steps below for each one.

## Step 1: Create a Service Principal

Choose one of the following options.

### Option A: Databricks Managed Service Principal

1. In the **Account Console**, go to **User management** → **Service principals**.
2. Click **Add service principal** → **Add new**.
3. Name it (example: `genie-deployer`).
4. Click **Add**.
5. Go to **Generate secret** and copy the client ID and secret.

You now have:

- `DATABRICKS_CLIENT_ID` = client ID (UUID)
- `DATABRICKS_CLIENT_SECRET` = secret value

### Option B: Microsoft Entra ID Service Principal

1. Go to **Microsoft Entra ID** → **App registrations**.
2. Click **+ New registration**.
3. Name it (example: `genie-deployer`).
4. Click **Register**.
5. Note the application (client) ID and directory (tenant) ID.
6. Go to **Certificates & secrets** → **+ New client secret**.
7. Copy the secret value immediately.

You now have:

- `ARM_TENANT_ID` = directory (tenant) ID
- `ARM_CLIENT_ID` = application (client) ID
- `ARM_CLIENT_SECRET` = secret value

## Step 2: Grant Workspace Access and Permissions

### Source workspace (export permissions)

1. Go to **Admin Settings** → **Identity and access** → **Service principals**.
2. Click **Add** and add the service principal.
3. Enable entitlements:
   - **Workspace access**
   - **Databricks SQL access**
4. Open the source Genie Space, select **Share**, and grant the principal **Can Edit**.

### Target workspace (deploy permissions)

1. Go to **Admin Settings** → **Identity and access** → **Service principals**.
2. Click **Add** and add the service principal.
3. Enable entitlements:
   - **Workspace access**
   - **Databricks SQL access**
4. Grant yourself **User** role on the service principal:
   - Open the principal → **Permissions** → **Grant access** → add yourself as **User**
   - This is required to create jobs that use `run_as` with this principal.
5. Go to **SQL Warehouses** → your warehouse → **Permissions** and grant the principal **Can Use**.
6. Grant the principal **Can Manage** on `target_parent_path` (configured in `databricks.yml`) so it can create/update Genie Spaces in that folder.

## Next Step

After service principal setup is complete, continue in [README.md](../README.md):

- [Cross-Workspace Migration Workflow](../README.md#cross-workspace-migration-workflow)
- [Command Reference](../README.md#command-reference)
