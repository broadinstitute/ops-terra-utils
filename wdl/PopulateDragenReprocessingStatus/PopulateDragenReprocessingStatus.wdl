version 1.0

workflow PopulateDragenReprocessingStatus {
    input {
		String billing_project
		String workspace_name
		String gcp_project
		String data_type
		String? min_start_date
		String? max_start_date
		String? docker
	}

	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

	call PopulateDragenReprocessingStatusTask {
		input:
			billing_project=billing_project,
			workspace_name=workspace_name,
			gcp_project=gcp_project,
			data_type=data_type,
			min_start_date=min_start_date,
			max_start_date=max_start_date,
			docker_name=docker_name
	}
}

task PopulateDragenReprocessingStatusTask {
	input {
		String billing_project
		String workspace_name
		String gcp_project
		String data_type
		String? min_start_date
		String? max_start_date
		String docker_name
	}

	command <<<
		python /etc/terra_utils/python/dragen/populate_dragen_reprocessing_status.py \
		--billing_project ~{billing_project} \
		--workspace_name ~{workspace_name} \
		--gcp_project ~{gcp_project} \
		--data_type ~{data_type} \
		~{"--min_start_date " + min_start_date} \
		~{"--max_start_date " + max_start_date}
	>>>

	runtime {
		docker: docker_name
	}
}
