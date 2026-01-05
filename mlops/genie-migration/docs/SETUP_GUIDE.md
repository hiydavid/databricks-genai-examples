# Genie Space Migration Guide

Migrate a Genie Space from one Databricks workspace to another using Databricks Asset Bundles and OAuth M2M authentication (no PAT tokens).

## Your Setup

```text
┌─────────────────────────────────┐      ┌──────────────────────────────────┐
│     SOURCE WORKSPACE            │      │     DESTINATION WORKSPACE        │
│     (Workspace 1)               │      │     (Workspace 2)                │
│                                 │      │                                  │
│  ┌───────────────────────┐      │      │  ┌────────────────────────┐      │
│  │ Existing Genie Space  │      │      │  │ New Genie Space        │      │
│  │ ID: abc-123-source    │ ──Export──► │  │ ID: xyz-789-dest       │      │
│  └───────────────────────┘      │      │  │ (created on 1st deploy)│      │
│                                 │      │  └────────────────────────┘      │
└─────────────────────────────────┘      └──────────────────────────────────┘
```

**Key concept**: The source and destination Genie Spaces have **different IDs**. The destination gets a new ID when first created.

---

## Workflow Overview

| Step | Action | What Happens |
| ------ | -------- | -------------- |
| 1 | Export from SOURCE | Get `serialized_space` JSON from your existing Genie Space |
| 2 | Store in Git | Commit the JSON config to your repo |
| 3 | First deploy to DESTINATION | Creates a NEW Genie Space with a NEW ID |
| 4 | Save the new ID | Store the destination space ID for future deploys |
| 5 | Future deploys | Update the destination space using its ID |

---

## Prerequisites

- **Source workspace**: Has your existing Genie Space
- **Destination workspace**: Where you want to deploy (currently empty)
- Permission to create Service Principals

---

## Step 1: Create a Service Principal

You need a service principal that can access **both** workspaces. Choose one of the following options:

### Option A: Databricks Managed Service Principal

1. In the **Account Console**, go to **User management** → **Service principals**
2. Click **Add service principal** → **Add new**
3. Name it: `genie-deployer`
4. Click **Add**
5. Go to **Generate secret** → Copy the **Client ID** and **Secret**

You now have:

- `DATABRICKS_CLIENT_ID` = Client ID (UUID)
- `DATABRICKS_CLIENT_SECRET` = Secret value

### Option B: Microsoft Entra ID Service Principal

1. Go to **Microsoft Entra ID** → **App registrations**
2. Click **+ New registration**
3. Name it: `genie-deployer`
4. Click **Register**
5. Note the **Application (client) ID** and **Directory (tenant) ID**
6. Go to **Certificates & secrets** → **+ New client secret**
7. Copy the secret value immediately (you can't see it again)

You now have:

- `ARM_TENANT_ID` = Directory (tenant) ID
- `ARM_CLIENT_ID` = Application (client) ID
- `ARM_CLIENT_SECRET` = Secret value

---

## Step 2: Grant Service Principal Access

### In SOURCE Workspace (to export)

1. Go to **Admin Settings** → **Identity and access** → **Service principals**
2. Click **Add** → enter the Application ID (Entra ID) or select from account (Databricks managed)
3. Open your existing Genie Space → **Share** → Add the service principal with **Can Edit**

### In DESTINATION Workspace (to create/update)

1. Go to **Admin Settings** → **Identity and access** → **Service principals**
2. Click **Add** → enter the Application ID (Entra ID) or select from account (Databricks managed)
3. Enable **Workspace access** and **Databricks SQL access**
4. **Grant yourself "User" role on the SP**: Click on the SP → **Permissions** → **Grant access** → Add yourself with **User** role
   > This is required to create jobs with `run_as` that reference this SP
5. Go to **SQL Warehouses** → your warehouse → **Permissions** → Add SP with **Can Use**
6. **Grant SP access to the target folder**: Navigate to the `target_parent_path` folder (e.g., `/Workspace/Shared/genie_spaces`) → Right-click → **Permissions** → Add SP with **Can Manage**
   > This is required for the SP to create Genie Spaces in this folder

---

## Step 3: Export from Source Workspace

Run the export job via DAB to extract the Genie Space configuration.

```bash
# Configure CLI to point to SOURCE workspace
databricks configure --host https://source-workspace.azuredatabricks.net

# Deploy the bundle with source_space_id
# NOTE: --var must be on deploy, not run (base_parameters are set at deploy time)
databricks bundle deploy --target dev \
    --var source_space_id="abc-123-your-source-space-id"

# Run export job
databricks bundle run export_genie_space --target dev

# Download exported JSON to local repo
databricks workspace export \
    /Workspace/Shared/genie_exports/<title>.json \
    --file ./genie_spaces/my_space.json
```

Then commit the downloaded JSON to git.

**Where to find the source space ID**: Open the Genie Space → look at the URL:
`https://source-workspace.azuredatabricks.net/genie/abc-123-your-source-space-id`

---

## Step 4: First Deployment (Creates New Space)

Run the deploy job via DAB. Since no space_id is provided, it creates a new space.

```bash
# Configure CLI to point to DESTINATION workspace
databricks configure --host https://dest-workspace.azuredatabricks.net

# Deploy the bundle with deploy_config_path pointing to bundle location
# NOTE: --var must be on deploy, not run (base_parameters are set at deploy time)
databricks bundle deploy --target dev \
    --var deploy_config_path="/Workspace/Shared/.bundle/genie-migration/dev/files/genie_spaces/my_space.json"

# Run deploy job (creates new space since target_space_id is empty)
databricks bundle run deploy_genie_space --target dev
```

### After First Deployment

The job outputs:

```text
Created Genie Space: xyz-789-new-dest-space-id
  Title: My Space
  Workspace: https://dest-workspace.azuredatabricks.net
  Path: /Workspace/Shared/genie_spaces/xyz-789-new-dest-space-id

SPACE_ID=xyz-789-new-dest-space-id
```

**Save this ID!** You'll need it for future deployments.

---

## Step 5: Future Deployments (Updates Existing Space)

Now that you have the destination space ID, future deployments will **update** the existing space instead of creating new ones.

```bash
# Deploy with both deploy_config_path and target_space_id
databricks bundle deploy --target dev \
    --var deploy_config_path="/Workspace/Shared/.bundle/genie-migration/dev/files/genie_spaces/my_space.json" \
    --var target_space_id="xyz-789-new-dest-space-id"

databricks bundle run deploy_genie_space --target dev
```

Output:

```text
Updated Genie Space: xyz-789-new-dest-space-id
```

---

## Summary: The Two IDs

| ID | Where | When You Use It |
| ---- | ------- | ----------------- |
| **Source Space ID** | Source workspace | When running export job with `--var source_space_id` |
| **Destination Space ID** | Destination workspace | When running deploy job with `--var target_space_id` (after first deploy) |

These are **different IDs** for **different spaces** in **different workspaces**.
