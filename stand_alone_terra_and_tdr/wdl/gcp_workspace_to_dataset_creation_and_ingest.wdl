version 1.0

workflow GCPWorkspaceToDatasetCreationAndIngest {
    input {
        String billing_project
        String workspace_name
        String? dataset_name
        String phs_id
        String? update_strategy
        Boolean bulk_mode
        String? docker
        String? tdr_billing_profile
        Int? file_ingest_batch_size
        Int? max_backoff_time
        Int? max_retries
        String? docker
    }

    String docker_image = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    call RunGCPWorkspaceToDataset {
        input:
            billing_project = billing_project,
            workspace_name = workspace_name,
            dataset_name = dataset_name,
            phs_id = phs_id,
            update_strategy = update_strategy,
            bulk_mode = bulk_mode,
            docker_name = docker_image,
            tdr_billing_profile = tdr_billing_profile,
            file_ingest_batch_size = file_ingest_batch_size,
            max_backoff_time = max_backoff_time,
            max_retries = max_retries
    }
}

task RunGCPWorkspaceToDataset {
    input {
        String billing_project
        String workspace_name
        String? dataset_name
        String phs_id
        String? update_strategy
        Boolean bulk_mode
        String docker_name
        String? tdr_billing_profile
        Int? file_ingest_batch_size
        Int? max_backoff_time
        Int? max_retries
    }

    command <<<
        python /etc/terra_utils/gcp_workspace_to_dataset_creation_and_ingest.py \
        --billing_project  ~{billing_project} \
        --workspace_name  ~{workspace_name} \
        --phs_id  ~{phs_id} \
        ~{"--dataset_name " + dataset_name} \
        ~{"--update_strategy " + update_strategy} \
        ~{"--tdr_billing_profile " + tdr_billing_profile} \
        ~{"--file_ingest_batch_size " + file_ingest_batch_size} \
        ~{"--max_backoff_time " + max_backoff_time} \
        ~{"--max_retries " + max_retries} \
        ~{if bulk_mode then "--bulk_mode" else ""}

    >>>

    runtime {
		docker: docker_name
	}
}