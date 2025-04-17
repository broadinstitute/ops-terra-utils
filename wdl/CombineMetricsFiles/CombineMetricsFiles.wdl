version 1.0

workflow CombineMetricsFiles {
	input {
		String billing_project
		String workspace_name
		String table_name
		String metrics_file_column
		String output_gcp_path
		String? identifier_column
		String? docker
	}

	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

	call CombineMetricsFilesTask {
		input:
			billing_project=billing_project,
			workspace_name=workspace_name,
			docker_name=docker_name,
			table_name=table_name,
			metrics_file_column=metrics_file_column,
			output_gcp_path=output_gcp_path,
			identifier_column=identifier_column
	}

	output {
		String combined_metrics = CombineMetricsFilesTask.combined_metrics_file
	}
}

task CombineMetricsFilesTask {
	input {
		String billing_project
		String workspace_name
		String table_name
		String metrics_file_column
		String output_gcp_path
		String? identifier_column
		String? docker_name
	}

	command <<<
		python /etc/terra_utils/python/combine_metrics_files.py \
			--billing_project ~{billing_project} \
			--workspace_name ~{workspace_name} \
			--table_name ~{table_name} \
			--metrics_file_column ~{metrics_file_column} \
			--output_gcp_path ~{output_gcp_path} \
			~{"--identifier_column " + identifier_column}
	>>>

	runtime {
		docker: docker_name
	}

	output {
		String combined_metrics_file = output_gcp_path
	}
}
