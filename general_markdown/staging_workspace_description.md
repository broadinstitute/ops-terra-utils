# Staging Workspace Overview
This workspace serves as a staging environment where users can upload, prepare, analyze, and validate their datasets prior to incorporation into the Terra Data Repository (TDR). The staging workspace comes pre-loaded with tools to help facilitate and streamline common tasks, as well as allow users to interact with TDR in as simple a manner as possible. Currently, this staging workspace includes:
1. The workspace itself, which includes a cloud storage bucket for housing data file objects, data tables for housing tabular data, and compute capabilities for interactive and batch analyses.
2. A pre-configured TDR dataset that has already been granted permission to read from the staging workspace.
3. A variable number of pre-configured Workflows and Notebooks aimed at helping simplify data submission work, depending on the project or program the staging workspace was created for.

#### Limitations:
A staging workspace sacrifices some flexibility in the configuration of cloud resources in exchange for simplicity. The following considerations should be kept in mind when working in a staging workspace:
1. The TDR dataset linked to the workspace will reference data file objects in the staging workspace rather than ingesting a copy of the data file objects. This prevents unnecessary duplication of data, but also means that __*moving, deleting, or updating data files that are being referenced in the TDR dataset may cause issues for users working with that dataset*__. 
2. Some TDR dataset functionality, such as adding tags or setting custom properties, is not available through the staging workspace, and instead would require interacting with the TDR dataset directly.

# Staging Workspace Walkthrough
## Step 0: Prerequisite Activities
#### Determine how the dataset will be modeled
Datasets within the context of Terra are defined as a collection of data file objects (genomics files, images, etc.) and related tabular data (metadata, phenotypic data, etc.) that adhere to a single schema. Before submission, it is important to think about how the dataset will be structured to best support the expected downstream use of the data. Some considerations include:
1. **What structured data do I want to upload into data tables, and how will I plan to reference my data file objects from within them?** In general, users will leverage data tables as an entry point for analysis, both to navigate the dataset to understand its contents but also as inputs to things like interactive and batch analyses. Data file objects that are not referenced somewhere in the data tables (by using the fully qualified gs:// path to the object) are not generally accessible for analysis in TDR, and thus will not be ingested into the TDR dataset via the staging workspace.
2. **How will I handle complex data structures within my dataset?** Tabular data within Terra is expected to be flat and relational. For datasets that use more complex, hierarchical structures for their structured data, users will need to decide how best to handle that (choosing to store the structured data as data file objects within the dataset, embedding some nested objects as strings within the data tables, etc.).
3. **Will the data file objects for my dataset be uploaded to the staging workspace or referenced from where they exist elsewhere in GCS?** Users do not need to move all of the data files for their dataset into the staging workspace in order for them to be referenced in their TDR dataset. They simply need to exist in a Google Cloud Storage (GCS) bucket where the TDR dataset can be granted read access (see guidance for this below).
4. **Are there data file objects I need for staging that I don't want to be included in my TDR dataset and shared with the research community?** To ensure these aren't pulled into the TDR dataset, they should either not be referenced in the data tables or removed from the staging workspace prior to the push of data into TDR.

#### Grant the TDR dataset read permission on remote buckets
In order for a TDR dataset to reference data file objects that don't exist in its own managed cloud storage, the dataset's service account needs read access to the bucket(s) where those remove data file objects live. To find your TDR dataset's service account, go to Workspace Data within the staging workspace and look for the value associated with the "data_ingest_sa" key. 
* **Workspace Bucket**  -- To provide the service account with access to a difference workspace bucket, simply share the workspace with the service account (Reader access) via the Terra UI. 
* **External Bucket** -- To provide the service account with access to an external GCS bucket, navigate to the bucket in the Google Cloud console and grant the service account the role of "Storage Object Viewer" on the bucket. As a secondary step, to ensure the push of data to TDR works properly, you will need to also grant the proxy email for your Terra account the role of "Storage Object Viewer" on the bucket as well. The proxy email for your Terra account can be found by going to your Terra profile and looking for "Proxy Group".

## Step 1: Upload Data File Objects to Workspace Bucket
See Terra Support Articles: [Moving/copying data in the cloud](https://support.terra.bio/hc/en-us/sections/7181665795483-Moving-copying-data-in-the-cloud)

#### Things to keep in mind:
1. While there is no required directory structure you need to adhere to, we generally recommend at least using a top-level directory of "data_files" or something similar to make navigating the staging workspace bucket a little easier going forward. 
2. All data file objects __*must have an md5 recorded in their GCS metadata, otherwise the push of data into TDR will fail.*__ Users can check md5 population through whichever means they are most comfortable with, with one option being to run the [CreateWorkspaceFileManifest](https://dockstore.org/workflows/github.com/broadinstitute/ops-terra-utils/CreateWorkspaceFileManifest) Worklow to create a file_metadata table in their staging workspace and checking the population of the md5_hash column. Note that users can then delete this table if they don't want to include it in their dataset (if they already have a file metadata table, for instance).

## Step 2: Upload Tabular Data to Workspace Data Tables
See Terra Support Articles: [Organizing data with tables](https://support.terra.bio/hc/en-us/sections/4408362836763-Organizing-data-with-tables)

#### Things to keep in mind:
1. __*In order for data file objects to be accessible in the TDR dataset, they must be referenced somewhere in the data tables.*__ One option to include data file objects that don't fit neatly into the existing data tables is to simply create and upload a table containing data file metadata. This can be done programmatically using the [CreateWorkspaceFileManifest](https://dockstore.org/workflows/github.com/broadinstitute/ops-terra-utils/CreateWorkspaceFileManifest) Worklow or handled manually by the user.
2. Some column labels that are valid in a staging workspace data table may not be valid in TDR. Specifically, __*columns should only contain letters, number, and underscores, and must start with a letter.*__
3. While data tables don't support complex, nested data structures, they do support arrays. In order to format column values as an array within a TSV, you can use JSON array notation (square brackets around comma-separated items, e.g. ["item 1", "item 2"]) and Terra will recognize it and structure the data accordingly.

## Step 3: Push Data to the Terra Data Repository
#### Push data to the TDR dataset
Once ready, data in the staging workspace can be pushed to the linked TDR dataset by using the [TerraWorkspaceTableToTDRIngest](https://dockstore.org/workflows/github.com/broadinstitute/ops-terra-utils/TerraWorkspaceTableToTDRIngest) workflow. To do this, user should:
1. Enter the tables to push into the "terra_tables" variable in the workflow configuration as a double-quotation enclosed, comma-separated list of tables with no spaces between them (e.g., "table_1,table_2").
2. Change any other parameters as desired, based on the README content of the workflow. In general, this workflow has been pre-configured such that users should not need to change any variables, particularly not the first time through. 
3. Ensure "Run workflow with inputs defined by file paths" is selected and save the configuration. Then click "Run Analysis" to kick off the workflow. 
4. If the workflow fails, see the following Terra Support Article for guidance:  [How to troubleshoot failed workflows](https://support.terra.bio/hc/en-us/articles/360027920592-How-to-troubleshoot-failed-workflows)

#### Review the data within TDR
To review the dataset once it has been pushed to TDR, navigate to the [TDR UI](https://data.terra.bio/datasets) and look for your dataset. Note that the identifier for your dataset will be recorded in the Workspace Data for the staging workspace, as the value associated with the "dataset_id" key. From within the TDR UI, you should be able to review the schema and click on "View Dataset Data" to actually look at the data that have been pushed into TDR. 

# Reference
## AnVIL Data Submission Resources
* If you are submitting data specifically to the AnVIL, please see the AnVIL-specific Terra Support Articles: [Prepare data for submission to AnVIL](https://support.terra.bio/hc/en-us/articles/28500021527451-Prepare-data-for-submission-to-AnVIL)
