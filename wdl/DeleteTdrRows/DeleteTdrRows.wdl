version 1.0

workflow DeleteTdrRows {
	input {
		String dataset_id
		String tdr_table_name
		Array[String] ids_to_delete
		String id_column_name
		Boolean delete_files
		String? docker
	}

	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

	call DeleteTdrRowsTask {
		input:
			dataset_id=dataset_id,
			table=tdr_table_name,
			id_column_name=id_column_name,
			delete_files=delete_files,
			ids_to_delete=ids_to_delete,
			docker_name=docker_name
	}
}

task DeleteTdrRowsTask {
	input {
		String dataset_id
		String table
		Array[String] ids_to_delete
		String id_column_name
		Boolean delete_files
		String docker_name
	}

	File ids_to_delete = write_lines(ids_to_delete)

	command <<<
		python /etc/terra_utils/python/delete_tdr_rows.py \
		--dataset_id ~{dataset_id} \
		--table ~{table} \
		--ids_to_delete_file ~{ids_to_delete} \
		--id_column_name ~{id_column_name} \
		~{if delete_files then "--delete_files" else ""}
	>>>

	runtime {
		docker: docker_name
	}
}
