# Staging/Submission Workspace Overview
This workspace serves as a staging environment to upload, prepare, analyze, and validate datasets prior to incorporation into the Terra Data Repository (TDR). The staging workspace comes pre-loaded with tools to help facilitate and streamline common tasks, letting users interact with TDR in as simple a manner as possible.

This workspace is part of the AnVIL data submission process described in the <a href="https://anvilproject.org/learn/data-submitters/submission-guide/data-submitters-overview" target="blank">AnVIL portal's Submission Process Overview</a>.

### Currently, this staging workspace includes
1.  The workspace itself, with a cloud storage Bucket for housing data file objects, data tables for housing tabular data, and compute capabilities for interactive and batch analyses.
2.  A pre-configured TDR dataset that has permission to read from the staging workspace.
3.  A number of pre-configured workflows and Jupyter notebooks to help simplify data submission work (how many depends on the project or program the staging workspace was created for).

### Workspace/TDR considerations and limitations
A staging workspace sacrifices some flexibility in the configuration of cloud resources in exchange for simplicity. Please keep the following considerations in mind when working in the submission/staging workspace.
1. **The TDR dataset linked to the workspace will reference data file objects in the staging workspace** rather than ingesting a copy of the data file objects. This prevents unnecessary duplication of data, but also means that __*moving, deleting, or updating data files that are being referenced in the TDR dataset may cause issues for users working with that dataset*__.
2. **Some TDR dataset functionality, such as adding tags or setting custom properties, is not available through the submission workspace**, and instead would require interacting with the TDR dataset directly (via Swagger APIs).

<br>

# How to submit AnVIL data to TDR

<img src="https://storage.googleapis.com/terra-featured-workspaces/TDR/TDR-SSI_diagram-of-steps.png" alt="diagram of steps for self-service data ingestion to TDR: 1) Register study/obtain approvals 2) Set up data model 3) prepare data for submission to AnVIL 4) Stage data in the submission workspace 5) push data to Terra data repository.">

<br>

## Step 1 (Prerequisites) - Register study/obtain approvals
- You should have completed this step before receiving access to this submission workspace.
- **See <a href="https://anvilproject.org/learn/data-submitters/submission-guide/data-approval-process" target="blank">Step 1 on the AnVIL portal</a> for step-by-step instructions**.

## Step 2 - Set up data model
- After your dataset has been approved by the AnVIL Data Ingestion Committee (step 1), you will need to set up and submit your data model, specifying what data you have and how data are connected. AnVIL recommends building a model that fits your dataset from the AnVIL core (minimal) findability subset.
- **For step-by-step instructions, see <a href="https://anvilproject.org/learn/data-submitters/submission-guide/set-up-a-data-model" target="blank">Step 2 in the AnVIL portal</a>.**

## Step 3 - Prepare data for AnVIL
- Before loading data into your staging workspace (step 4), you’ll organize all required data and metadata in a format compatible with AnVIL. In this step, you'll prepare for data ingestion by generating a TSV for each table in your data model using the spreadsheet editor of your choice and saving as tab separated values (TSV) format.
- **For step-by-step instructions, see <a href="https://support.terra.bio/hc/en-us/articles/28500021527451-Step-3-Prepare-data-for-submission-to-AnVIL" target="blank">Step 3 in Terra Support</a>.**

## Step 4 - Stage data in submission workspace
- Once you have prepared your omics object files and generated TSV files for each table in your data model, follow the directions below to deposit the data object files and all TSV files into AnVIL-owned data submission workspaces. Before proceeding to Step 5 - Ingest data into TDR, you'll QC the data in the submission workspace to make sure it conforms to all AnVIL standards and avoid problems during the ingestion.
- **For step-by-step instructions, see <a href="https://support.terra.bio/hc/en-us/articles/25290333623835-Step-4-Stage-data-in-submission-workspace" target="blank">Step 4 in Terra Support</a>.**

## Step 5 - Push data to the Terra Data Repository
- Finally, you'll push data in the staging workspace to the linked TDR dataset. If you choose, you can clean up the staging workspace.
- **For step-by-step instructions, see <a href="https://support.terra.bio/hc/en-us/articles/36671511533467-Step-5-Ingest-data-to-TDR" target="blank">Step 5 in Terra Support</a>.**

<br>

# Self-service WDLs
- CreateWorkspaceFileManifest - <a href="https://dockstore.org/workflows/github.com/broadinstitute/ops-terra-utils/CreateWorkspaceFileManifest" target="blank">READ ME</a>
- TerraSummaryStatistics - <a href="https://dockstore.org/workflows/github.com/broadinstitute/ops-terra-utils/TerraSummaryStatistics" target="blank">READ ME</a>

- TerraWorkspaceTableToTDRIngest - <a href="https://dockstore.org/workflows/github.com/broadinstitute/ops-terra-utils/TerraWorkspaceTableToTDRIngest" target="blank">READ ME</a>
