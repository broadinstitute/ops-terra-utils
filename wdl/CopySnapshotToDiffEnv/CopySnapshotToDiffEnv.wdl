version 1.0


workflow CopySnapshotToDiffEnv {
	input {
		String billing_project
		String workspace_name
		String dataset_id
		String orig_env
		Boolean continue_if_exists
		Boolean delete_temp_workspace
		Boolean verbose
		String? new_billing_profile
		File? service_account_json
		String? owner_emails
		String? docker
	}

	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

	call RunCopySnapshotToDiffEnv {
		input:
			billing_project=billing_project,
			workspace_name=workspace_name,
			dataset_id=dataset_id,
			orig_env=orig_env,
			new_billing_profile=new_billing_profile,
			continue_if_exists=continue_if_exists,
			docker_name=docker_name,
			delete_temp_workspace=delete_temp_workspace,
			verbose=verbose,
			owner_emails=owner_emails,
			service_account_json=service_account_json
	}
}

task RunCopySnapshotToDiffEnv {
	input {
		String billing_project
		String workspace_name
		String dataset_id
		String orig_env
		String? new_billing_profile
		Boolean continue_if_exists
		String docker_name
		Boolean delete_temp_workspace
		Boolean verbose
		String? owner_emails
		File? service_account_json
	}

	command <<<
	python3 /etc/terra_utils/python/copy_snapshot_to_diff_env.py \
		--billing_project ~{billing_project} \
		--workspace_name ~{workspace_name} \
		--dataset_id ~{dataset_id} \
		--orig_env ~{orig_env} \
		~{"--new_billing_profile " + new_billing_profile} \
		~{"--service_account_json " + service_account_json} \
		~{"--owner_emails " + owner_emails} \
		~{if continue_if_exists then "--continue_if_exists" else ""} \
		~{if delete_temp_workspace then '--delete_temp_workspace' else ''} \
		~{if verbose then '--verbose' else ''}
	>>>

	runtime {
		docker: docker_name
	}
}
