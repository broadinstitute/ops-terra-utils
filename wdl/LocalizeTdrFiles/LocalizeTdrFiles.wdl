version 1.0

workflow LocalizeTdrFiles {
    input {
		Array[String] input_data  #what should i name these??
		Array[String] input_data_index
		Array[String] input_entity_id
		String workspace_bucket
		String subdirectory_name
		Boolean update_data_tables = true
		String billing_project
		String workspace_name
		String data_table_name
		String new_data_column_name
		String new_data_index_column_name

		String? docker
		Int? memory_gb
		Int? disk_size_gb
	}

	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])
	Int memory = select_first([memory_gb, 8])
	Int disk = select_first([disk_size_gb, 50])

	call LocalizeTdrFilesTask {
		input:
			data = input_data,
			data_index = input_data_index,
			entity_id = input_entity_id,
			workspace_bucket = workspace_bucket,
			subdirectory_name = subdirectory_name,
			update_data_tables = update_data_tables,
			billing_project = billing_project,
			workspace_name = workspace_name,
			data_table_name = data_table_name,
			new_data_column_name = new_data_column_name,
			new_data_index_column_name = new_data_index_column_name,
			docker_name = docker_name,
			memory = memory,
			disk = disk
	}
}

task LocalizeTdrFilesTask {
	input {
		Array[String] data  #what should i name these??
		Array[String] data_index
		Array[String] entity_id
		String workspace_bucket
		String subdirectory_name
		Boolean update_data_tables = true
		String billing_project
		String workspace_name
		String data_table_name
		String new_data_column_name
		String new_data_index_column_name

		String docker_name
		String memory
		String disk
	}

	File data_array_file = write_lines(data)
	File data_index_array_file = write_lines(data_index)
	File entity_ids_array_file = write_lines(entity_id)

	command <<<
		python /etc/terra_utils/python/localize_tdr_files.py \
		--billing_project ~{billing_project} \
		--workspace_name "~{workspace_name}" \
		--first_file_list ~{data_array_file} \
		--second_file_list ~{data_index_array_file} \
		--entity_ids ~{entity_ids_array_file} \
		--subdir ~{subdirectory_name} \
		--first_column_name ~{new_data_column_name} \
		--second_column_name ~{new_data_index_column_name} \
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
