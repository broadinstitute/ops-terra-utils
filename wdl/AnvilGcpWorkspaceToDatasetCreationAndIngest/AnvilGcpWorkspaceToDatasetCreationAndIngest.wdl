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

    Boolean already_added_to_auth_domain_bool = select_first([already_added_to_auth_domain, true])
    Boolean file_path_flat_bool = select_first([file_path_flat, true])
    Boolean self_hosted_bool = select_first([self_hosted, true])
    Boolean filter_existing_ids_bool = select_first([filter_existing_ids, true])
    Boolean bulk_mode_bool = select_first([bulk_mode, false])
    String docker_image = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    call RunGCPWorkspaceToDataset {
        input:
            billing_project = billing_project,
            workspace_name = workspace_name,
            dataset_name = dataset_name,
            phs_id = phs_id,
            update_strategy = update_strategy,
            docker_name = docker_image,
            tdr_billing_profile = tdr_billing_profile,
            file_ingest_batch_size = file_ingest_batch_size,
            max_backoff_time = max_backoff_time,
            max_retries = max_retries,
            self_hosted = self_hosted_bool,
            filter_existing_ids = filter_existing_ids_bool,
            already_added_to_auth_domain = already_added_to_auth_domain_bool,
            file_path_flat_bool = file_path_flat_bool,
            bulk_mode = bulk_mode_bool
    }
}

task RunGCPWorkspaceToDataset {
    input {
        String billing_project
        String workspace_name
        Boolean self_hosted
        String phs_id
        Boolean filter_existing_ids
        Boolean already_added_to_auth_domain
        Boolean file_path_flat_bool
        String docker_name
        Boolean bulk_mode
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
        ~{if bulk_mode then "--bulk_mode" else ""} \
        ~{if self_hosted then "--dataset_self_hosted" else ""} \
        ~{if filter_existing_ids then "--filter_existing_ids" else ""} \
        ~{if already_added_to_auth_domain then "--already_added_to_auth_domain" else ""} \
        ~{if file_path_flat_bool then "--file_path_flat" else ""}

    >>>

    runtime {
		docker: docker_name
	}
}
