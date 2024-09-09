version 1.0

workflow gcp_workspace_to_dataset_creation_and_ingest {
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
    }

    String docker_image = select_first([docker, "johnscira/test_docker_repo:latest"])

    call run_gcp_workspace_to_dataset {
        input:
            billing_project=billing_project,
            workspace_name=workspace_name,
            dataset_name=dataset_name,
            phs_id=phs_id,
            update_strategy=update_strategy,
            bulk_mode=bulk_mode,
            docker=docker_image,
            tdr_billing_profile=tdr_billing_profile,
            file_ingest_batch_size=file_ingest_batch_size,
            max_backoff_time=max_backoff_time,
            max_retries=max_retries
    }
}

task run_gcp_workspace_to_dataset {
    input {
        String billing_project
        String workspace_name
        String? dataset_name
        String phs_id
        String? update_strategy
        Boolean bulk_mode
        String docker
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