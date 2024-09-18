version 1.0

workflow FileExport{
		input {
		String export_type
		String target_id
		String bucket_id
	}
	call run_export {
		input: export_type=export_type,
				target_id=target_id,
				bucket_id=bucket_id
	}
}

task run_export{
	input {
		String export_type
		String target_id
		String bucket_id
	}

	command <<<
		python /etc/terra_utils/azure_tdr_to_gcp_file_transfer.py \
		--export_type ~{export_type} \
		--target_id ~{target_id} \
		--bucket_id ~{bucket_id}
	>>>

	runtime {
		docker: "johnscira/test_docker_repo:latest"
	}
}
