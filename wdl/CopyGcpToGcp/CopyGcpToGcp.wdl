version 1.0

workflow CopyGcpToGcp {
    input {
		String destination_path
		Boolean preserve_structure
		String? source_bucket
		String? source_fofn
        String? docker
	}

	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])


	call RunCopyGcpToGcp {
		input:
			destination_path=destination_path,
			preserve_structure=preserve_structure,
			source_bucket=source_bucket,
			source_fofn=source_fofn,
			docker_name=docker_name
	}
}

task RunCopyGcpToGcp {
	input {
		String destination_path
		String docker_name
		String? source_fofn
		String? source_bucket
		Boolean preserve_structure
	}

	command <<<
		python /etc/terra_utils/python/copy_gcp_to_gcp.py \
		--destination_path ~{destination_path} \
		~{"--source_fofn " + source_fofn} \
		~{"--source_bucket " + source_bucket} \
		~{if preserve_structure then "--preserve_structure" else ""}
	>>>

	runtime {
		docker: docker_name
	}
}
