version 1.0

workflow SetUpStagingWorkspaceAndDataset {
    input {
        String dataset_name
        String tdr_billing_profile_uuid
        String terra_billing_project
        Boolean controlled_access
        String? phs_id
        String resource_owners
        String? resource_members
        Boolean continue_if_exists
        Boolean delete_existing_dataset
        Boolean dataset_self_hosted
        String current_user_email
        String? dbgap_consent_code
        String? duos_identifier
        String? wdls_to_import
        String? notebooks_to_import
        String? docker
        Int? workspace_version
        Boolean is_anvil = false
    }

    String docker_image = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    call SetUpStagingEnvironments {
        input:
            dataset_name = dataset_name,
            tdr_billing_profile_uuid = tdr_billing_profile_uuid,
            terra_billing_project = terra_billing_project,
            controlled_access = controlled_access,
            phs_id = phs_id,
            resource_owners = resource_owners,
            resource_members = resource_members,
            continue_if_exists = continue_if_exists,
            current_user_email = current_user_email,
            dbgap_consent_code = dbgap_consent_code,
            duos_identifier = duos_identifier,
            wdls_to_import = wdls_to_import,
            notebooks_to_import = notebooks_to_import,
            delete_existing_dataset = delete_existing_dataset,
            docker = docker_image,
            dataset_self_hosted = dataset_self_hosted,
            workspace_version = workspace_version,
            is_anvil = is_anvil
    }
}

task SetUpStagingEnvironments {
    input {
        String dataset_name
        String tdr_billing_profile_uuid
        String terra_billing_project
        Boolean controlled_access
        String? phs_id
        String resource_owners
        String? resource_members
        Boolean continue_if_exists
        Boolean delete_existing_dataset
        String current_user_email
        String? dbgap_consent_code
        String? duos_identifier
        String? wdls_to_import
        String? notebooks_to_import
        String docker
        Int? workspace_version
        Boolean dataset_self_hosted
        Boolean is_anvil
    }

    command <<<
        python /etc/terra_utils/python/set_up_staging_workspace_and_dataset.py \
            --dataset_name ~{dataset_name} \
            --tdr_billing_profile_uuid ~{tdr_billing_profile_uuid} \
            --terra_billing_project ~{terra_billing_project} \
            ~{if controlled_access then "--controlled_access" else ""} \
            ~{"--phs_id " + phs_id} \
            --resource_owners ~{resource_owners} \
            ~{"--resource_members " + resource_members} \
            ~{if continue_if_exists then "--continue_if_exists" else ""} \
            --current_user_email ~{current_user_email} \
            ~{"--dbgap_consent_code " + dbgap_consent_code} \
            ~{"--duos_identifier " + duos_identifier} \
            ~{"--wdls_to_import " + wdls_to_import} \
            ~{"--notebooks_to_import " + notebooks_to_import} \
            ~{if delete_existing_dataset then "--delete_existing_dataset" else ""} \
            ~{"--workspace_version " + workspace_version} \
            ~{if dataset_self_hosted then "--dataset_self_hosted" else ""} \
            ~{if is_anvil then "--platform anvil" else ""}
    >>>

    runtime {
        docker: docker
    }
}
