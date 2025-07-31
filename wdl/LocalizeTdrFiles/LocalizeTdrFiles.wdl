version 1.0

workflow LocalizeTdrFiles {
    input {
		Array[String] primary_files  #what should i name these??
		Array[String] secondary_files
		Array[String] entity_ids
		String workspace_bucket
		String subdirectory_name
		Boolean update_data_tables = true
		String billing_project
		String workspace_name
		String data_table_name
		String new_primary_column_name
		String new_secondary_column_name

		String? docker
		Int? memory_gb
		Int? disk_size_gb
	}

	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])
	Int memory = select_first([memory_gb, 8])
	Int disk = select_first([disk_size_gb, 50])

	call LocalizeTdrFilesTask {
		input:
			primary_files = primary_files,
			secondary_files = secondary_files,
			entity_ids = entity_ids,
			workspace_bucket = workspace_bucket,
			subdirectory_name = subdirectory_name,
			update_data_tables = update_data_tables,
			billing_project = billing_project,
			workspace_name = workspace_name,
			data_table_name = data_table_name,
			new_primary_column_name = new_primary_column_name,
			new_secondary_column_name = new_secondary_column_name,
			docker_name = docker_name,
			memory = memory,
			disk = disk
	}
}

task LocalizeTdrFilesTask {
	input {
		Array[String] primary_files  #what should i name these??
		Array[String] secondary_files
		Array[String] entity_ids
		String workspace_bucket
		String subdirectory_name
		Boolean update_data_tables = true
		String billing_project
		String workspace_name
		String data_table_name
		String new_primary_column_name
		String new_secondary_column_name

		String docker_name
		String memory
		String disk
	}

	File primary_array_file = write_lines(primary_files)
	File secondary_array_file = write_lines(secondary_files)
	File entity_ids_array_file = write_lines(entity_ids)

	command <<<
		python /etc/terra_utils/python/localize_tdr_files.py \
		--billing_project ~{billing_project} \
		--workspace_name "~{workspace_name}" \
		--first_file_list ~{primary_array_file} \
		--second_file_list ~{secondary_array_file} \
		--entity_ids ~{entity_ids_array_file} \
		--subdir ~{subdirectory_name} \
		--first_column_name ~{new_primary_column_name} \
		--second_column_name ~{new_secondary_column_name} \
		--table_name ~{data_table_name} \
		~{if update_data_tables then "--upload-tsv" else ""} \
	>>>

	runtime {
		docker: docker_name
		memory: "${memory} GiB"
		disks: "local-disk ${disk} HDD"
	}

	output {
		File output_tsv = "~{data_table_name}_localized_files.tsv"
	}
}
