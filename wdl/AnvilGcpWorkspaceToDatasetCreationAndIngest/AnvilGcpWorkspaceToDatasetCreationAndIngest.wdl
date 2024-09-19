version 1.0

workflow GCPWorkspaceToDatasetCreationAndIngest {
    input {
        String billing_project
        String workspace_name
        String phs_id
        Boolean? already_added_to_auth_domain
        Boolean? filter_existing_ids
        Boolean? bulk_mode
        Boolean? self_hosted
        Boolean? file_path_flat
        String? dataset_name
        String? update_strategy
        String? docker
        String? tdr_billing_profile
        Int? file_ingest_batch_size
        Int? max_backoff_time
        Int? max_retries
        String? docker
    }

    Boolean already_added_to_auth_domain = select_first([already_added_to_auth_domain, true])
    Boolean bulk_mode = select_first([bulk_mode, false])
    Boolean file_path_flat = select_first([file_path_flat, true])
    Boolean self_hosted = select_first([self_hosted, true])
    Boolean filter_existing_ids = select_first([filter_existing_ids, true])
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
            max_retries = max_retries,
            self_hosted = self_hosted,
            filter_existing_ids = filter_existing_ids,
            already_added_to_auth_domain = already_added_to_auth_domain
    }
}

task RunGCPWorkspaceToDataset {
    input {
        String billing_project
        String workspace_name
        Boolean self_hosted
        String phs_id
        Boolean bulk_mode
        Boolean filter_existing_ids
        Boolean already_added_to_auth_domain
        String docker_name
        String? tdr_billing_profile
        Int? file_ingest_batch_size
        Int? max_backoff_time
        Int? max_retries
        String? dataset_name
        String? update_strategy
    }

    command <<<
        python /etc/terra_utils/anvil_gcp_workspace_to_dataset_creation_and_ingest.py \
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
        ~{if self_hosted then "--dataset_self_hosted" else ""}
        ~{if filter_existing_ids then "--filter_existing_ids" else ""}
        ~{if already_added_to_auth_domain then "--already_added_to_auth_domain" else ""}

    >>>

    runtime {
		docker: docker_name
	}
}
