version 1.0

workflow hard_clone_terra_workspace {
    input {
		String source_billing_project
		String source_workspace_name
		String dest_billing_project
        String dest_workspace_name
        Boolean allow_already_created
        Int? workers
        String? extensions_to_ignore
		String? docker_name
	}

	String docker = select_first([docker_name, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

	call hard_clone_terra_workspace {
		input:
			source_billing_project=source_billing_project,
			source_workspace_name=source_workspace_name,
			dest_billing_project=dest_billing_project,
			dest_workspace_name=dest_workspace_name,
			allow_already_created=allow_already_created,
			workers=workers,
			extensions_to_ignore=extensions_to_ignore,
			docker_name=docker
	}
}

task hard_clone_terra_workspace {
	input {
		String source_billing_project
        String source_workspace_name
        String dest_billing_project
        String dest_workspace_name
        Boolean allow_already_created
        Int? workers
        String? extensions_to_ignore
        String docker_name
	}

	command <<<
		python /etc/terra_utils/hard_clone_workspace.py \
		--source_billing_project ~{source_billing_project} \
		--source_workspace_name ~{source_workspace_name} \
		--dest_billing_project ~{dest_billing_project} \
		--dest_workspace_name ~{dest_workspace_name} \
		~{if allow_already_created then "--allow_already_created" else ""} \
		~{"--workers " + workers} \
		~{"--extensions_to_ignore " + extensions_to_ignore}
	>>>

	runtime {
		docker: docker_name
	}
}
