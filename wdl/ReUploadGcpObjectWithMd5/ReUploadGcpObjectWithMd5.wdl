version 1.0

workflow ReUploadGcpObjectWithMd5 {
    input {
        String gcp_file_path
        String? requester_pays_project
        Int? memory_gb
        Int? disk_size
        String? docker
    }

    String docker_image = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    call ReUploadGcpObject {
        input:
            gcp_file_path = gcp_file_path,
            requester_pays_project = requester_pays_project,
            disk_size = disk_size,
            memory_gb = memory_gb,
            docker_image = docker_image
    }
}

task ReUploadGcpObject {
    input {
        String gcp_file_path
        String? requester_pays_project
        Int? disk_size
        String? md5_format
        Int? memory_gb
        String docker_image
    }

    command <<<
        python /etc/terra_utils/python/reupload_gcp_file_with_md5.py \
        --gcp_file_path ~{gcp_file_path} \
        ~{if requester_pays_project then "--requester_pays_project " + requester_pays_project else ""}
    >>>

    runtime {
        docker: docker_image
        disks: "local-disk " + select_first([disk_size, 2]) + " SSD"
        memory: select_first([memory_gb, 2]) + " GB"
    }
}
