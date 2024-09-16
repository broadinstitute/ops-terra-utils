version 1.0

workflow RenameAndReingestFiles {
    input {
        String dataset_id
        String original_file_basename_column
        String new_file_basename_column
        String dataset_table_name
        String row_identifier
        Int copy_and_ingest_batch_size
        Int? max_retries
        Int? max_backoff_time
        String? docker
        String? billing_project
        String? workspace_name
        String? temp_bucket
        Int? workers
    }

    String docker_image = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    call RenameAndingestFiles {
        input:
            dataset_id = dataset_id,
            original_file_basename_column = original_file_basename_column,
            new_file_basename_column = new_file_basename_column,
            dataset_table_name = dataset_table_name,
            row_identifier = row_identifier,
            copy_and_ingest_batch_size = copy_and_ingest_batch_size,
            max_retries = max_retries,
            max_backoff_time = max_backoff_time,
            docker_image = docker_image,
            billing_project = billing_project,
            workspace_name = workspace_name,
            temp_bucket = temp_bucket,
            workers = workers
    }
}

task RenameAndingestFiles {
    input {
        String dataset_id
        String original_file_basename_column
        String new_file_basename_column
        String dataset_table_name
        String row_identifier
        Int copy_and_ingest_batch_size
        Int? max_retries
        Int? max_backoff_time
        String? docker_image
        String? billing_project
        String? workspace_name
        String? temp_bucket
        Int? workers
    }

    command <<<
        python /etc/terra_utils/rename_and_reingest_files.py \
        --dataset_id  ~{dataset_id} \
        --original_file_basename_column  ~{original_file_basename_column} \
        --new_file_basename_column  ~{new_file_basename_column} \
        --dataset_table_name  ~{dataset_table_name} \
        --row_identifier  ~{row_identifier} \
        --copy_and_ingest_batch_size  ~{copy_and_ingest_batch_size} \
        ~{"--max_retries " + max_retries} \
        ~{"--max_backoff_time " + max_backoff_time} \
        ~{"--billing_project " + billing_project} \
        ~{"--workspace_name " + workspace_name} \
        ~{"--temp_bucket " + temp_bucket} \
        ~{"--workers " + workers}
    >>>

    runtime {
		docker: docker_image
	}

}