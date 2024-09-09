version 1.0

workflow gcp_workspace_to_dataset_ingest {
    input {
        String billing_project
        String workspace_name
        String dataset_id
        String target_table_name
        String tdr_row_id
        String? update_strategy
        Array[String] sample_ids_to_ingest
        Boolean? bulk_mode
        Int? max_retries
        Int? max_backoff_time
        String? docker
    }

    String docker_image = select_first([docker, "johnscira/test_docker_repo:latest"])

    call gcp_workspace_to_dataset_ingest {
        input:
            billing_project=billing_project,
            workspace_name=workspace_name,
            dataset_id=dataset_id,
            target_table_name=target_table_name,
            tdr_row_id=tdr_row_id,
            update_strategy=update_strategy,
            sample_ids_to_ingest=sample_ids_to_ingest,
            bulk_mode=bulk_mode,
            max_retries=max_retries,
            max_backoff_time=max_backoff_time
            docker_image=docker_image
    }
}

task gcp_workspace_to_dataset_ingest {
    input {
        String billing_project
        String workspace_name
        String dataset_id
        String target_table_name
        String tdr_row_id
        String? update_strategy
        Array[String]? sample_ids_to_ingest
        Boolean? bulk_mode
        Int? max_retries
        Int? max_backoff_time
        String docker_image
    }

    command <<<
        python /etc/terra_utils/gcp_workspace_to_dataset_ingest.py \
        --billing_project  ~{billing_project} \
        --workspace_name  ~{workspace_name} \
        --dataset_id  ~{dataset_id} \
        --target_table_name  ~{target_table_name} \
        --tdr_row_id  ~{tdr_row_id} \
        --tdr_row_id  ~{tdr_row_id} \
        ~{"--update_strategy " + update_strategy} \
        ~{"--sample_ids_to_ingest " + sample_ids_to_ingest} \
        ~{"--bulk_mode " + bulk_mode} \
        ~{"--max_retries " + max_retries} \
        ~{"--max_backoff_time " + max_backoff_time} \
    >>>

    runtime {
		docker: docker_image
	}

}
