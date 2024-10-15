version 1.0

import "../utils/GcpUtils.wdl" as gcp_utils

workflow HardCloneTerraWorkspace {
    input {
		String source_billing_project
		String source_workspace_name
		String dest_billing_project
		String dest_workspace_name
		Boolean allow_already_created
		Boolean rsync_workspace
		Int? workers
		String? extensions_to_ignore
		String? docker_name
		Int? memory_gb
		Int? batch_size
	}

	String docker = select_first([docker_name, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])
	# Ignore HardCloneTerraWorkspace submisisons files
	String rysnc_regex_exclude = "^.*/submissions/.*/HardCloneTerraWorkspace/.*$"
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
			batch_size=batch_size,
			metadata_only=rsync_workspace
	}

	if (rsync_workspace) {
		call gcp_utils.GcloudRsync {
			input:
				source=HardCloneTerraWorkspaceTask.src_bucket,
				destination=HardCloneTerraWorkspaceTask.dest_bucket,
				exclude_regex=rysnc_regex_exclude
		}
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
		Boolean metadata_only
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
		~{"--batch_size " + batch_size} \
		~{if metadata_only then "--metadata_only" else ""}
	>>>

	output {
		String dest_bucket = read_string("dest_workspace_bucket.txt")
		String src_bucket = read_string("source_workspace_bucket.txt")
	}

	runtime {
		docker: docker_name
		memory: "${memory_gb} GiB"
	}
}
