version 1.0

workflow AzDatasetToGcp {
	input {
		File az_fofn
		Int width
		String log_dir
		Int max_gb_per_disk
		Boolean skip_too_large_files
		String? docker
		Int? minutes_before_reload_token
	}

	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

	call CreateFofns {
		input:
			az_fofn=az_fofn,
			width=width,
			docker_name=docker_name,
			max_gb_per_disk=max_gb_per_disk,
			skip_too_large_files=skip_too_large_files
	}

    scatter (index in range(length(CreateFofns.output_tsvs))) {
		Int disk_size = read_int(CreateFofns.disk_sizes[index])
        call CopyAzToGcp {
            input:
                tsv=CreateFofns.output_tsvs[index],
                docker_name=docker_name,
                minutes_before_reload_token=minutes_before_reload_token,
                disk_size_gb=disk_size,  # Using corresponding disk size
                log_dir=log_dir
        }
    }
}

task CopyAzToGcp {
	input {
		File tsv
		String docker_name
		String log_dir
		Int? minutes_before_reload_token
		Int disk_size_gb
	}

	Int vm_disk_size = disk_size_gb + 40  # Add 20 GB to disk size

	command <<<
		wget https://aka.ms/downloadazcopy-v10-linux
		tar -xvf downloadazcopy-v10-linux

		python /etc/terra_utils/python/run_az_copy_to_gcp.py \
			~{"--time_before_reload " + minutes_before_reload_token} \
			--tsv ~{tsv} \
			--az_path azcopy_linux_amd64_10.28.0/azcopy \
			--log_dir ~{log_dir}
	>>>

	runtime {
		docker: docker_name
		disks: "local-disk " + vm_disk_size + " HDD"
	}
}

task CreateFofns {
	input {
		File az_fofn
		Int width
		String docker_name
		Int max_gb_per_disk
		Boolean skip_too_large_files
	}

	command <<<
		python /etc/terra_utils/python/create_az_copy_fofns.py \
		--full_az_tsv ~{az_fofn} \
		--width ~{width} \
		--max_gb_per_disk ~{max_gb_per_disk} \
		~{if skip_too_large_files then "--skip_too_large_files" else ""}
	>>>

	runtime {
		docker: docker_name
	}

	output {
		Array[File] output_tsvs = glob("split_*.tsv")
		Array[File] disk_sizes = glob("disk_size_split_*.txt")
	}
}
