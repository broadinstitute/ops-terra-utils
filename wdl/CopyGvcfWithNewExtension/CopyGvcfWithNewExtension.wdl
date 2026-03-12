version 1.0

workflow CopyGvcfWithNewExtension {
	input {
		Array[String] sample_names
		Array[String] gvcf_file_paths
		String sample_name_map
		String? docker
	}

	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

	call GenerateGvcfToSampleMapping {
		input:
			sample_names = sample_names,
			gvcf_file_paths = gvcf_file_paths,
			docker_name = docker_name
		}

	call CopyGvcfWithNewExtension {
		input:
			sample_mapping = GenerateGvcfToSampleMapping.sample_map,
			sample_name_map_output_location = sample_name_map + ".sample_map",
			docker_name = docker_name
	}
}

task GenerateGvcfToSampleMapping {
	input {
		Array[String] sample_names
		Array[String] gvcf_file_paths
		String docker_name
	}

	command <<<
		set -oe pipefail
		python << CODE

		file_paths = ['~{sep="','" gvcf_file_paths}']
		sample_names = ['~{sep="','" sample_names}']

		if len(file_paths) != len(sample_names):
			print("Number of gvcf paths does not equal the number of sample names. Please check your inputs.")
			exit(1)

		with open("sample_map_file.tsv", "w") as tsv_file:
			tsv_file.write("sample_name" + "\t" + "gvcf_file_path" + "\n")

			for i in range(len(file_paths)):
				tsv_file.write(sample_names[i] + "\t" + file_paths[i] + "\n")

		CODE
    >>>

	output {
        File sample_map = "sample_map_file.tsv"
    }

	runtime {
		docker: docker_name
	}

}

task CopyGvcfWithNewExtension {
	input {
		File sample_mapping
		String sample_name_map_output_location
		String docker_name
	}

	command <<<
		python /etc/terra_utils/python/convert_gvcf_extension_for_joint_calling.py \
			--original_gvcf_mapping ~{sample_mapping} \
			--output_sample_map ~{sample_name_map_output_location}
	>>>

	output {
		File output_sample_map = sample_name_map_output_location
	}

	runtime {
		docker: docker_name
	}
}
