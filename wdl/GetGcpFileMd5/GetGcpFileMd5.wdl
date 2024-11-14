version 1.0

workflow GetGcpFileMd5 {
    input {
        String gcp_file_path
        Boolean create_cloud_md5_file
        String? md5_format
        String? docker
        Int? memory_gb
    }

    String docker_image = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    call GetFileMd5 {
        input:
            gcp_file_path = gcp_file_path,
            create_cloud_md5_file = create_cloud_md5_file,
            docker_image = docker_image,
            md5_format = md5_format,
            memory_gb = memory_gb
    }

    output {
        String md5_hash = GetFileMd5.md5_hash
    }
}

task GetFileMd5 {
    input {
        String gcp_file_path
        Boolean create_cloud_md5_file
        String docker_image
        String? md5_format
        Int? memory_gb
    }

    command <<<
        python /etc/terra_utils/python/get_file_md5.py \
        --gcp_file_path ~{gcp_file_path} \
        --output_file object_md5.txt \
        ~{if create_cloud_md5_file then "--create_cloud_md5_file" else ""} \
        ~{"--md5_format " + md5_format}
    >>>

    runtime {
        docker: docker_image
        memory: select_first([memory_gb, 4]) + " GB"
    }

    output {
        String md5_hash = read_string("object_md5.txt")
    }
}
