version 1.0

workflow GCPWorkspaceToDatasetIngest {
    input {
        String billing_project
        String workspace_name
        String dataset_id
        String terra_table_name
        String? target_table_name
        String primary_key_column_name
        String? update_strategy
        Array[String]? records_to_ingest
        Boolean bulk_mode
        Int? max_retries
        Int? max_backoff_time
        String? docker
    }

    String docker_image = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    call IngestWorkspaceDataToDataset {
        input:
            billing_project = billing_project,
            workspace_name = workspace_name,
            dataset_id = dataset_id,
            terra_table_name = terra_table_name,
            target_table_name = target_table_name,
            primary_key_column_name = primary_key_column_name,
            update_strategy = update_strategy,
            records_to_ingest = records_to_ingest,
            bulk_mode = bulk_mode,
            max_retries = max_retries,
            max_backoff_time = max_backoff_time,
            docker_image = docker_image
    }
}

task IngestWorkspaceDataToDataset {
    input {
        String billing_project
        String workspace_name
        String dataset_id
        String terra_table_name
        String? target_table_name
        String primary_key_column_name
        String? update_strategy
        Array[String]? records_to_ingest
        Boolean bulk_mode
        Int? max_retries
        Int? max_backoff_time
        String docker_image
    }

    command <<<
        python /etc/terra_utils/gcp_workspace_table_to_dataset_ingest.py \
        --billing_project  ~{billing_project} \
        --workspace_name  ~{workspace_name} \
        --dataset_id  ~{dataset_id} \
        --terra_table_name  ~{terra_table_name} \
        --target_table_name  ~{target_table_name} \
        --primary_key_column_name  ~{primary_key_column_name} \
        ~{"--update_strategy " + update_strategy} \
        ~{if records_to_ingest then "--records_to_ingest " + records_to_ingest else ""} \
        ~{if bulk_mode then "--bulk_mode" else ""} \
        ~{"--max_retries " + max_retries} \
        ~{"--max_backoff_time " + max_backoff_time} \
    >>>

    runtime {
		docker: docker_image
	}

}
