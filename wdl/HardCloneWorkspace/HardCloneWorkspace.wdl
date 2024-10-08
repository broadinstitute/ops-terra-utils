version 1.0

workflow HardCloneTerraWorkspace {
    input {
		String source_billing_project
		String source_workspace_name
		String dest_billing_project
        String dest_workspace_name
        Boolean allow_already_created
        Int? workers
        String? extensions_to_ignore
		String? docker_name
		Int? memory_gb
		Int? batch_size
	}

	String docker = select_first([docker_name, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])
	Int memory = select_first([memory_gb, 8])

	call HardCloneTerraWorkspaceTask {
		input:
			source_billing_project=source_billing_project,
			source_workspace_name=source_workspace_name,
			dest_billing_project=dest_billing_project,
			dest_workspace_name=dest_workspace_name,
			allow_already_created=allow_already_created,
			workers=workers,
			extensions_to_ignore=extensions_to_ignore,
			docker_name=docker,
			memory_gb=memory,
			batch_size=batch_size
	}
}

task HardCloneTerraWorkspaceTask {
	input {
		String source_billing_project
        String source_workspace_name
        String dest_billing_project
        String dest_workspace_name
        Boolean allow_already_created
        Int? workers
        String? extensions_to_ignore
        String docker_name
		Int memory_gb
		Int? batch_size
	}

	command <<<
		python /etc/terra_utils/hard_clone_workspace.py \
		--source_billing_project ~{source_billing_project} \
		--source_workspace_name ~{source_workspace_name} \
		--dest_billing_project ~{dest_billing_project} \
		--dest_workspace_name ~{dest_workspace_name} \
		~{if allow_already_created then "--allow_already_created" else ""} \
		~{"--workers " + workers} \
		~{"--extensions_to_ignore " + extensions_to_ignore} \
		~{"--batch_size " + batch_size}
	>>>

	runtime {
		docker: docker_name
		memory: "${memory_gb} GiB"
	}
}
