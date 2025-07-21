version 1.0


workflow CopySnapshotToDiffEnv {
	input {
		String temp_bucket
		String dataset_id
		String orig_env
		Boolean continue_if_exists
		Boolean verbose
		Boolean delete_intermediate_files
		String? new_billing_profile
		File? service_account_json
		String? owner_emails
		String? docker

	}

	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

	call RunCopySnapshotToDiffEnv {
		input:
			temp_bucket=temp_bucket,
			dataset_id=dataset_id,
			orig_env=orig_env,
			new_billing_profile=new_billing_profile,
			continue_if_exists=continue_if_exists,
			docker_name=docker_name,
			verbose=verbose,
			owner_emails=owner_emails,
			delete_intermediate_files=delete_intermediate_files,
			service_account_json=service_account_json
	}
}

task RunCopySnapshotToDiffEnv {
	input {
		String temp_bucket
		String dataset_id
		String orig_env
		String? new_billing_profile
		Boolean continue_if_exists
		String docker_name
		Boolean verbose
		Boolean delete_intermediate_files
		String? owner_emails
		File? service_account_json
	}

	command <<<
	python3 /etc/terra_utils/python/copy_snapshot_to_diff_env.py \
		--temp_bucket ~{temp_bucket} \
		--dataset_id ~{dataset_id} \
		--orig_env ~{orig_env} \
		~{"--new_billing_profile " + new_billing_profile} \
		~{"--service_account_json " + service_account_json} \
		~{"--owner_emails " + owner_emails} \
		~{if continue_if_exists then "--continue_if_exists" else ""} \
		~{if verbose then '--verbose' else ''} \
		~{if delete_intermediate_files then '--delete_intermediate_files' else ''}
	>>>

	runtime {
		docker: docker_name
	}
}
