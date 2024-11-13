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
        Boolean filter_existing_ids
        Boolean check_existing_ingested_files
        Boolean all_fields_non_required
        Int? max_retries
        Int? max_backoff_time
        Int? batch_size
        String? docker
        Boolean force_disparate_rows_to_string = true
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
            batch_size = batch_size,
            check_existing_ingested_files = check_existing_ingested_files,
            all_fields_non_required = all_fields_non_required,
            force_disparate_rows_to_string = force_disparate_rows_to_string
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
        Boolean check_existing_ingested_files
        Boolean all_fields_non_required
        Int? batch_size
        Boolean force_disparate_rows_to_string
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
        ~{if check_existing_ingested_files then "--check_existing_ingested_files" else ""} \
        ~{"--batch_size " + batch_size} \
        ~{if all_fields_non_required then "--all_fields_non_required" else ""} \
        ~{if force_disparate_rows_to_string then "--force_disparate_rows_to_string" else ""}

    >>>

    runtime {
		docker: docker_image
	}

}
