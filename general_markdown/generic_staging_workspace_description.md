# Staging/Submission Workspace Overview
This workspace serves as a staging environment to upload, prepare, analyze, and validate datasets prior to incorporation into the Terra Data Repository (TDR). The staging workspace comes pre-loaded with tools to help facilitate and streamline common tasks, letting users interact with TDR in as simple a manner as possible.

### This staging workspace includes
1.  The workspace itself, with a cloud storage Bucket for housing data file objects, data tables for housing tabular data, and compute capabilities for interactive and batch analyses.
2.  A pre-configured TDR dataset that has permission to read from the workspace.
3.  A number of pre-configured workflows and Jupyter notebooks to help simplify data submission work.

### Workspace/TDR considerations and limitations
A staging workspace sacrifices some flexibility in the configuration of cloud resources in exchange for simplicity. Please keep the following considerations in mind when working in the submission/staging workspace.
1. **The TDR dataset linked to the workspace will reference data file objects in the staging workspace** rather than ingesting a copy of the data file objects. This prevents unnecessary duplication of data, but also means that __*moving, deleting, or updating data files that are being referenced in the TDR dataset may cause issues for users working with that dataset*__.

2. **Some TDR dataset functionality, such as adding tags or setting custom properties, is not available through the submission workspace**, and instead would require interacting with the TDR dataset directly (via Swagger APIs).

# How to submit data to TDR

<img src="https://storage.googleapis.com/terra-featured-workspaces/TDR/Self-serve-TDR-data-ingest-flow2.png" alt="diagram of steps for self-service data ingestion to TDR: 1) Register study/obtain approvals 2) Set up data model 3) prepare data for submission to AnVIL 4) Stage data in the submission workspace 5) push data to Terra data repository.">

<br>

## Step 1 - Set up data model
- Before staging data in this submission workspace, you'll need to set up your data model, which specifies what data you have and how data are connected. TDR can accept most datasets. For some guidance developing a data model (if you don't already have one), see <a href="https://support.terra.bio/hc/en-us/articles/360055895111-Making-data-findable-the-Terra-Interoperability-Model-TIM" target="blank">Making data findable - the Terra Interoperability Model (TIM)</a>.       

**For step-by-step instructions, see <a href="https://support.terra.bio/hc/en-us/articles/37538383710235" target="blank">1. Set up data model</a> in Terra Support.**     

<br>

## Step 2 - Format/prepare data for TDR
- Before loading data into your staging workspace (step 4), you’ll organize all required data and metadata in a format compatible with AnVIL. In this step, you'll prepare for data ingestion by generating a TSV for each table in your data model using the spreadsheet editor of your choice and saving as tab separated values (TSV) format.
      
**For step-by-step instructions, see <a href="https://support.terra.bio/hc/en-us/articles/37538583166875" target="blank">2. Format/preare data</a> in Terra Support**     

<br>

## Step 3 - Stage/QC data
- Once you have prepared your omics object files and generated TSV files for each table in your data model, follow the directions below to deposit the data object files and all TSV files into this submission workspaces.
- Before proceeding to Step 4 - Ingest data into TDR, you'll QC the data in the workspace to make sure it conforms to all TDR standards and avoid problems during the ingestion.
      
**For step-by-step instructions, see <a href="https://support.terra.bio/hc/en-us/articles/37539663125659" target="blank">3. Stage/QC data in submission workspace</a> in Terra Support.**     

<br>

## Step 4 - Ingest data into Terra Data Repository
- Finally, you'll push data in the staging workspace to the linked TDR dataset. If you choose, you can clean up the staging workspace.
     
**For step-by-step instructions, see <a href="https://support.terra.bio/hc/en-us/articles/37540115125915" target="blank">4. Ingest data into TDR</a> in Terra Support.**

<br>

# Self-service WDLs
- CreateWorkspaceFileManifest - <a href="https://dockstore.org/workflows/github.com/broadinstitute/ops-terra-utils/CreateWorkspaceFileManifest" target="blank">READ ME</a>
- TerraSummaryStatistics - <a href="https://dockstore.org/workflows/github.com/broadinstitute/ops-terra-utils/TerraSummaryStatistics" target="blank">READ ME</a>

- TerraWorkspaceTableToTDRIngest - <a href="https://dockstore.org/workflows/github.com/broadinstitute/ops-terra-utils/TerraWorkspaceTableToTDRIngest" target="blank">READ ME</a>
