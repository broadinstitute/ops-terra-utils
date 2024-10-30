version 1.0

workflow SetUpStagingWorkspaceAndDataset {
    input {
        String dataset_name
        String tdr_billing_profile
        String terra_billing_project
        Boolean controlled_access
        String? phs_id
        String? resource_owners
        String? resource_members
        Boolean continue_if_exists
        String current_user_email
        String? dbgap_consent_code
        String? duos_identifier
        String? wdls_to_import
        String? notebooks_to_import
        Boolean is_anvil
        String? docker
    }

    String docker_image = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    call SetUpStagingEnvironments {
        input:
            dataset_name = dataset_name,
            tdr_billing_profile = tdr_billing_profile,
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
            is_anvil = is_anvil,
            docker = docker_image
    }
}

task SetUpStagingEnvironments {
    input {
        String dataset_name
        String tdr_billing_profile
        String terra_billing_project
        Boolean controlled_access
        String? phs_id
        String? resource_owners
        String? resource_members
        Boolean continue_if_exists
        String current_user_email
        String? dbgap_consent_code
        String? duos_identifier
        String? wdls_to_import
        String? notebooks_to_import
        Boolean is_anvil
        String? docker
    }

    command <<<
        python /etc/terra_utils/python/rename_and_reingest_files.py \
            --dataset_name ~{dataset_name} \
            --tdr_billing_profile ~{tdr_billing_profile} \
            --terra_billing_project ~{terra_billing_project} \
            ~{if controlled_access then "--controlled_access" else ""} \
            ~{"--phs_id " + phs_id} \
            ~{"--resource_owners " + resource_owners} \
            ~{"--resource_members " + resource_members} \
            ~{if continue_if_exists then "--continue_if_exists" else ""} \
            --current_user_email ~{current_user_email} \
            ~{"--dbgap_consent_code " + dbgap_consent_code} \
            ~{"--duos_identifier " + duos_identifier} \
            ~{"--wdls_to_import " + wdls_to_import} \
            ~{"--notebooks_to_import " + notebooks_to_import} \
            ~{if is_anvil then "--is_anvil" else ""}
    >>>

    runtime {
        docker: docker
    }
}
