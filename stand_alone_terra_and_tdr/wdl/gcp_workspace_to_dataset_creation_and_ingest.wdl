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
            docker=docker_image
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
    }

    command <<<
        python /etc/terra_utils/gcp_workspace_to_dataset_creation_and_ingest.py \
        --billing_project  ~{billing_project} \
        --workspace_name  ~{workspace_name} \
        --phs_id  ~{phs_id} \
        ~{"--dataset_name " + dataset_name} \
        ~{"--update_strategy " + update_strategy} \
        ~{if bulk_mode then "--bulk_mode" else ""}

    >>>

    runtime {
		docker: docker_name
	}
}