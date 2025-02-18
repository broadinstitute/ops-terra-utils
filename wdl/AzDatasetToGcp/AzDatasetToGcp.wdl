version 1.0

workflow AzDatasetToGcp {
	input {
		File az_fofn
		Int width
		String? docker
		Int? minutes_before_reload_token
		Int? disk_size_gb
	}

	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

	call CreateFofns {
		input:
			az_fofn=az_fofn,
			width=width,
			docker_name=docker_name
	}

	call DownloadAz {
		input:
			docker_name=docker_name
	}

	scatter (tsv in CreateFofns.output_tsvs) {
		call CopyAzToGcp {
			input:
				tsv=tsv,
				docker_name=docker_name,
				minutes_before_reload_token=minutes_before_reload_token,
				disk_size_gb=disk_size_gb#,
				#az_path=DownloadAz.az_file,
				#az_notice=DownloadAz.az_notice
		}
	}
}

task CopyAzToGcp {
	input {
		File tsv
		String docker_name
		#File az_path
		# Needed to also be there for permission?
		#File az_notice
		Int? minutes_before_reload_token
		Int? disk_size_gb
	}

	command <<<
		wget https://aka.ms/downloadazcopy-v10-linux
		tar -xvf downloadazcopy-v10-linux

		python /etc/terra_utils/python/run_az_copy_to_gcp.py \
			~{"--time_before_reload " + minutes_before_reload_token} \
			--tsv ~{tsv} \
			--az_path azcopy_linux_amd64_10.28.0/azcopy
	>>>

	runtime {
		docker: docker_name
		disks: "local-disk " + select_first([disk_size_gb, 50]) + " HDD"
	}
}

task DownloadAz {
	input {
		String docker_name
	}

	command <<<
		wget https://aka.ms/downloadazcopy-v10-linux
		tar -xvf downloadazcopy-v10-linux
		chmod +x azcopy_linux_amd64_10.28.0/azcopy
	>>>

	runtime {
		docker: docker_name
	}

	#output {
#		File az_file = "azcopy_linux_amd64_10.28.0/azcopy"
#		File az_notice = "azcopy_linux_amd64_10.28.0/NOTICE.txt"
#	}
}

task CreateFofns {
	input {
		File az_fofn
		Int width
		String docker_name
	}

	command <<<
		python /etc/terra_utils/python/create_az_copy_fofns.py \
		--full_az_tsv ~{az_fofn} \
		--width ~{width}
	>>>

	runtime {
		docker: docker_name
	}

	output {
		Array[File] output_tsvs = glob("split_*.tsv")
	}
}
