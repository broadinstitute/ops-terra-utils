version 1.0


workflow CleanUpStagingWorkspace {
    input {
		String dataset_id
		String billing_project
		String workspace_name
		String? file_paths_to_ignore
		String output_file
		Boolean run_deletes
        String? docker
	}

	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])


	call CleanUpStagingWorkspaceTask {
		input:
			dataset_id=dataset_id,
            billing_project=billing_project,
            workspace_name=workspace_name,
            file_paths_to_ignore=file_paths_to_ignore,
            output_file=output_file,
            run_deletes=run_deletes,
            docker_name=docker_name
	}

    output {
        File delete_file = CleanUpStagingWorkspaceTask.delete_file
    }
}

task CleanUpStagingWorkspaceTask {
	input {
		String dataset_id
		String billing_project
		String workspace_name
		String? file_paths_to_ignore
		String output_file
		Boolean run_deletes
        String docker_name
	}

	command <<<
		python /etc/terra_utils/python/clean_up_staging_workspace.py \
		--dataset_id ~{dataset_id} \
		--billing_project ~{billing_project} \
		--workspace_name ~{workspace_name} \
        --output_file ~{output_file} \
		~{"--file_paths_to_ignore " + file_paths_to_ignore} \
		~{if run_deletes then "--run_delete" else ""}
	>>>

	runtime {
		docker: docker_name
	}

    output {
        File delete_file = output_file
    }
}
