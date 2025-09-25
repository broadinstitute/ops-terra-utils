version 1.0


workflow RenameColumnsAndFilesInDataset {
    input {
        String dataset_id
        String column_to_update
        String column_with_new_value
        String table_name
        Int? copy_and_ingest_batch_size
        Int? max_retries
        Int? max_backoff_time
        String? docker
        String billing_project
        String workspace_name
        Boolean report_updates_only
        Boolean? update_columns_only
    }

    Boolean update_columns_only_bool = select_first([update_columns_only, false])
    String docker_image = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    call RenameColumnsAndFiles {
        input:
            dataset_id = dataset_id,
            column_to_update = column_to_update,
            column_with_new_value = column_with_new_value,
            table_name = table_name,
            copy_and_ingest_batch_size = copy_and_ingest_batch_size,
            max_retries = max_retries,
            max_backoff_time = max_backoff_time,
            docker_image = docker_image,
            billing_project = billing_project,
            workspace_name = workspace_name,
            report_updates_only = report_updates_only,
            update_columns_only = update_columns_only_bool
    }
}

task RenameColumnsAndFiles {
    input {
        String dataset_id
        String column_to_update
        String column_with_new_value
        String table_name
        String docker_image
        Int? copy_and_ingest_batch_size
        Boolean report_updates_only
        Int? max_retries
        Int? max_backoff_time
        String billing_project
        String workspace_name
        Boolean update_columns_only
    }

    command <<<
        python /etc/terra_utils/python/rename_columns_and_files_in_dataset.py \
        --dataset_id  ~{dataset_id} \
        --column_to_update  ~{column_to_update} \
        --column_with_new_value  ~{column_with_new_value} \
        --table_name  ~{table_name} \
        --billing_project ~{billing_project} \
        --workspace_name ~{workspace_name} \
        ~{"--max_retries " + max_retries} \
        ~{"--max_backoff_time " + max_backoff_time} \
        ~{"--copy_and_ingest_batch_size " + copy_and_ingest_batch_size} \
        ~{if report_updates_only then "--report_updates_only" else ""} \
        ~{if update_columns_only then "--update_columns_only" else ""}
    >>>

    runtime {
		docker: docker_image
	}

}
