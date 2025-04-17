version 1.0

workflow HardCloneWithExternalBucket {
    input {
		String source_billing_project
		String source_workspace_name
		String dest_billing_project
		String dest_workspace_name
		String external_bucket
		Boolean allow_already_created
		Boolean do_not_update_acls
		Int? workers
		String? extensions_to_ignore
		String? docker
		Int? memory_gb
		Int? batch_size
		Boolean check_and_wait_for_permissions
		Boolean skip_check_if_already_copied
		Int? max_permissions_wait_time
	}

	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])
	Int memory = select_first([memory_gb, 8])

	call HardCloneWithExternalBucketTask {
		input:
			source_billing_project=source_billing_project,
			source_workspace_name=source_workspace_name,
			dest_billing_project=dest_billing_project,
			dest_workspace_name=dest_workspace_name,
			allow_already_created=allow_already_created,
			external_bucket=external_bucket,
			workers=workers,
			extensions_to_ignore=extensions_to_ignore,
			docker_name=docker_name,
			memory_gb=memory,
			batch_size=batch_size,
			do_not_update_acls=do_not_update_acls,
			check_and_wait_for_permissions=check_and_wait_for_permissions,
			max_permissions_wait_time=max_permissions_wait_time,
			skip_check_if_already_copied=skip_check_if_already_copied
	}
}

task HardCloneWithExternalBucketTask {
	input {
		String source_billing_project
		String source_workspace_name
		String dest_billing_project
		String dest_workspace_name
		Boolean allow_already_created
		String external_bucket
		Int? workers
		String? extensions_to_ignore
		String docker_name
		Int memory_gb
		Boolean do_not_update_acls
		Boolean skip_check_if_already_copied
		Int? batch_size
		Boolean check_and_wait_for_permissions
		Int? max_permissions_wait_time
	}

	command <<<
		python /etc/terra_utils/python/hard_clone_workspace.py \
		--source_billing_project ~{source_billing_project} \
		--source_workspace_name "~{source_workspace_name}" \
		--dest_billing_project ~{dest_billing_project} \
		--dest_workspace_name "~{dest_workspace_name}" \
		--external_bucket ~{external_bucket} \
		~{if allow_already_created then "--allow_already_created" else ""} \
		~{"--workers " + workers} \
		~{"--extensions_to_ignore " + extensions_to_ignore} \
		~{"--batch_size " + batch_size} \
		~{if do_not_update_acls then "--do_not_update_acls" else ""} \
		~{if check_and_wait_for_permissions then "--check_and_wait_for_permissions" else ""} \
		~{"--max_permissions_wait_time " + max_permissions_wait_time} \
		~{if skip_check_if_already_copied then "--skip_check_if_already_copied" else ""}
	>>>

	runtime {
		docker: docker_name
		memory: "${memory_gb} GiB"
	}
}
