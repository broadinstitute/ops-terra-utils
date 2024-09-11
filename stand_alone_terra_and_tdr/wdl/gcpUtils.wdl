version 1.0

task CopyGCPFile {
    input {
        String source_file_path
        String destination_file_path
    }

    command <<<
        set -euo pipefail

        gsutil cp source_file_path destination_file_path

    >>>

    runtime {
        docker: "gcr.io/gcp-runtimes/ubuntu_16_0_4:latest"
    }

}