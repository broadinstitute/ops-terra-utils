version 1.0

workflow FileExport{
  input {
        String export_type
        String target_id
        String bucket_id
        String? bucket_output_path
        Boolean? retain_path_structure
    }

    call run_export {
        input: export_type=export_type,
                target_id=target_id,
                bucket_id=bucket_id,
                bucket_output_path=bucket_output_path,
                retain_path_structure=retain_path_structure
    }
}

task run_export{
    input {
        String export_type
        String target_id
        String bucket_id
        String? bucket_output_path
        Boolean? retain_path_structure
    }

    command <<<
        python /etc/terra_utils/azure_tdr_to_gcp_file_transfer.py \
        --export_type ~{export_type} \
        --target_id ~{target_id} \
        --bucket_id ~{bucket_id} \
        ~{"--bucket_ouput_path" + bucket_output_path} \
        ~{if retain_path_structure then "--retain_path_structure" else ""}
    >>>

    runtime {
        docker: "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"
  }

    output{
        File copy_logs='copy_manifest.csv'
    }

}
