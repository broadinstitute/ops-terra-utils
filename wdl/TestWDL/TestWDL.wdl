version 1.0

workflow TestWorkflow {
	input {
		String? docker
	}

	String docker_image = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

	call TestTask {
		input:
			docker_image = docker_image
	}
}

task TestTask {
	input {
		String docker_image
	}

	command <<<
		python /etc/terra_utils/python/emerge_single_sample_file_transfers.py
	>>>

	runtime {
		docker: docker_image
	}

	output {
		File mapping_csv = "AnVIL_eMERGE_eMERGEseq_source_bam_vcf_to_destination_mapping.csv"
		File unmapped_csv = "unmapped_files.csv"
	}
}
