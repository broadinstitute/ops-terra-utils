version 1.0


workflow DiffAndCleanUpWorkspace {
    input {
		String dataset_id
		String billing_project
		String workspace_name
		String? file_paths_to_ignore
		String cloud_directory
		String? delete_from_workspace
		String? google_project
		String? docker
	}

	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])


	call DiffAndCleanUpWorkspaceTask {
		input:
			dataset_id=dataset_id,
			billing_project=billing_project,
			workspace_name=workspace_name,
			file_paths_to_ignore=file_paths_to_ignore,
			cloud_directory=cloud_directory,
			delete_from_workspace=delete_from_workspace,
			google_project=google_project,
			docker_name=docker_name
	}
}

task DiffAndCleanUpWorkspaceTask {
	input {
		String dataset_id
		String billing_project
		String workspace_name
		String? file_paths_to_ignore
		String cloud_directory
		String? delete_from_workspace
		String? google_project
		String docker_name
	}

	command <<<
		python /etc/terra_utils/python/diff_and_clean_up_workspace.py \
		--dataset_id ~{dataset_id} \
		--billing_project ~{billing_project} \
		--workspace_name ~{workspace_name} \
		--cloud_directory ~{cloud_directory} \
		~{"--file_paths_to_ignore " + file_paths_to_ignore} \
		~{"--gcp_project " + google_project} \
		~{"--delete_from_workspace " + delete_from_workspace}
	>>>

	runtime {
		docker: docker_name
	}
}
