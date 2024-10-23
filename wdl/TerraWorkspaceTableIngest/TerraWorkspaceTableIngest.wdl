version 1.0

workflow GCPWorkspaceToDatasetIngest {
    input {
        String billing_project
        String workspace_name
        String dataset_id
        String terra_tables
        String? update_strategy
        String? records_to_ingest
        Boolean bulk_mode
        Int? max_retries
        Int? max_backoff_time
        Boolean filter_existing_ids
        Int? batch_size
        String? docker
    }

    String docker_image = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    call IngestWorkspaceDataToDataset {
        input:
            billing_project = billing_project,
            workspace_name = workspace_name,
            dataset_id = dataset_id,
            terra_tables = terra_tables,
            update_strategy = update_strategy,
            records_to_ingest = records_to_ingest,
            bulk_mode = bulk_mode,
            max_retries = max_retries,
            max_backoff_time = max_backoff_time,
            docker_image = docker_image,
            filter_existing_ids = filter_existing_ids,
            batch_size = batch_size
    }
}

task IngestWorkspaceDataToDataset {
    input {
        String billing_project
        String workspace_name
        String dataset_id
        String terra_tables
        String? update_strategy
        String? records_to_ingest
        Boolean bulk_mode
        Int? max_retries
        Int? max_backoff_time
        String docker_image
        Boolean filter_existing_ids
        Int? batch_size
    }

    command <<<
        python /etc/terra_utils/python/gcp_workspace_table_to_dataset_ingest.py \
        --billing_project  ~{billing_project} \
        --workspace_name  ~{workspace_name} \
        --dataset_id  ~{dataset_id} \
        --terra_tables  ~{terra_tables} \
        ~{"--update_strategy " + update_strategy} \
        ~{if bulk_mode then "--bulk_mode" else ""} \
        ~{"--max_retries " + max_retries} \
        ~{"--max_backoff_time " + max_backoff_time} \
        ~{"--records_to_ingest " + records_to_ingest} \
        ~{if filter_existing_ids then "--filter_existing_ids" else ""} \
        ~{"--batch_size " + batch_size}
    >>>

    runtime {
		docker: docker_image
	}

}
