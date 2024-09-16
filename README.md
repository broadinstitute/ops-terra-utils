# Ops terra utils Overview

Welcome to this repository! This repo contains Python scripts and WDLs designed to be used in conjunction with Terra and the Terra Data Repository (TDR).

### What does this repo do?

- **Python scripts**: Automate tasks such as ingesting metadata, managing files in TDR, and interacting with Terra workspaces.
- **WDLs**: Wrap these Python scripts for execution in Terra workflows.

### What does this repo not do?

- This repo does not include the direct setup of Terra or TDR environments.
- It does not manage external dependencies for running workflows unless done through the provided Docker image or WDL.

For more detailed information, please refer to [CONTRIBUTING.md](CONTRIBUTING.md).

## Getting Started

### How to set up the repo:

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
2. Running Python Scripts: You can run the Python scripts directly after cloning. Some of the key scripts are designed to interact with Terra and TDR for tasks such as metadata ingestion, dataset management, etc.
3. Using Docker: There is a Docker image available that comes with all the required dependencies for running the scripts: 
`us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest`
4. Running WDLs: The WDLs in this repository are designed to be run in Terra. You can import the WDLs into your Terra workspace and run them there.

## Running Code Locally
- To run a Python script locally you will need all required dependencies installed. It will be some subset, if not all, of [requirements.txt](requirements.txt).
```bash
python python/script_name.py --arg1 value --arg2 value
```
- Alternatively, if you're using Docker:
```bash
docker run -v $(pwd):/app us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest python /app/script_name.py --arg1 value --arg2 value
```

For more setup details, see [CONTRIBUTING.md](CONTRIBUTING.md).
