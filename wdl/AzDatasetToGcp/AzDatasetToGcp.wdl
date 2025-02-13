version 1.0

workflow AzDatasetToGcp {
	input {
		File az_fofn
		Int width
		String gcp_destination
		String? docker
		Int? minutes_before_reload_token
	}

	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

	call CreateFofns {
		input:
			az_fofn=az_fofn,
			width=width,
			docker_name=docker_name,
			gcp_destination=gcp_destination
	}

	scatter (tsv in CreateFofns.output_tsvs) {
		call CopyAzToGcp {
			input:
				tsv=tsv,
				docker_name=docker_name,
				minutes_before_reload_token=minutes_before_reload_token
		}
	}
}

task CopyAzToGcp {
	input {
		File tsv
		String docker_name
		Int? minutes_before_reload_token
	}

	command <<<
		wget https://aka.ms/downloadazcopy-v10-linux
		tar -xvf downloadazcopy-v10-linux
		python /etc/terra_utils/python/run_az_copy_to_gcp.py --tsv ~{tsv} \
			~{"--minutes_before_reload_token " + minutes_before_reload_token}
	>>>

	runtime {
		docker: docker_name
	}
}

task CreateFofns {
	input {
		File az_fofn
		Int width
		String docker_name
		String gcp_destination
	}

	command <<<
		python /etc/terra_utils/python/create_az_copy_fofns.py \
		--az_fofn ~{az_fofn} \
		--width ~{width} \
		--gcp_destination ~{gcp_destination}
	>>>

	runtime {
		docker: docker_name
	}

	output {
		Array[File] output_tsvs = glob("split_*.tsv")
	}
}
