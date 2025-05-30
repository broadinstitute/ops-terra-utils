# Staging/Submission Workspace Overview
This workspace serves as a staging environment to upload, prepare, analyze, and validate datasets prior to incorporation into the Terra Data Repository (TDR). The staging workspace comes pre-loaded with tools to help facilitate and streamline common tasks, letting users interact with TDR in as simple a manner as possible.

### Currently, this staging workspace includes
1.  The workspace itself, with a cloud storage Bucket for housing data file objects, data tables for housing tabular data, and compute capabilities for interactive and batch analyses.
2.  A pre-configured TDR dataset that has permission to read from the staging workspace.
3.  A number of pre-configured workflows and Jupyter notebooks to help simplify data submission work (how many depends on the project or program the staging workspace was created for).

### Workspace/TDR considerations and limitations
A staging workspace sacrifices some flexibility in the configuration of cloud resources in exchange for simplicity. Please keep the following considerations in mind when working in the submission/staging workspace.
1. **The TDR dataset linked to the workspace will reference data file objects in the staging workspace** rather than ingesting a copy of the data file objects. This prevents unnecessary duplication of data, but also means that __*moving, deleting, or updating data files that are being referenced in the TDR dataset may cause issues for users working with that dataset*__.
2. **Some TDR dataset functionality, such as adding tags or setting custom properties, is not available through the submission workspace**, and instead would require interacting with the TDR dataset directly (via Swagger APIs).
