version 1.0

# Workflow for reheadering a BAM file.
# The workflow takes an input BAM file, a reference genome, and changes the sample name in the BAM header.
workflow ReaheaderBam {
    input {
        File input_bam          # Input BAM file to be reheadered
        String old_sample       # Original sample name in the BAM file
        String new_sample       # New sample name to replace the original
    }

    # Main task call to perform reheadering
    call ReheaderFile {
        input:
            input_bam = input_bam,
            old_sample = old_sample,
            new_sample = new_sample,
    }

    # Output of the workflow includes the new BAM file, its index, and MD5 checksum.
    output {
        File reheadered_bam_path = ReheaderFile.new_bam
        File reheadered_bai_path = ReheaderFile.new_bai
        File reheadered_bam_md5_path = ReheaderFile.new_md5
    }
}

# Task definition for reheadering a BAM file.
task ReheaderFile {
    input {
        File input_bam          # Input BAM file to be reheadered
        String old_sample       # Original sample name in the BAM file
        String new_sample       # New sample name to replace the original
    }

    # Naming convention for new files
    String new_bam = new_sample + ".bam"
    String new_bai = new_sample + ".bai"
    String new_md5 = new_sample + ".bam.md5"

    # Calculate the required disk size based on input file sizes
    Int disk_size = ceil((2 * size(input_bam, "GiB"))) + 20

    command {
        set -e

        echo "Generating new header file"
        # Extract header from BAM, modify sample name, and remove @PG lines
        samtools view -H ~{input_bam} > header
        sed --expression='s/SM:~{old_sample}/SM:~{new_sample}/g' header > remapped_header
        grep -v '^@PG' remapped_header > updated_header

        echo "Reheadering cram file"
        # Reheader the BAM file with updated header
        samtools reheader -P updated_header ~{input_bam} > ~{new_bam}

        echo "Generating md5 file"
        # Generate MD5 checksum for the new BAM file
        md5sum ~{new_bam} > ~{new_md5}

        echo "Generating index file"
        # Index the new BAM file
        samtools index ~{new_bam} > ~{new_bai}
    }

    # Runtime parameters including docker image, memory, CPU, disk, and retries
    runtime {
        docker: "us.gcr.io/broad-gotc-prod/samtools:1.0.0-1.11-1624651616"
        preemptible: 3
        memory: "7 GiB"
        cpu: "1"
        disks: "local-disk " + disk_size + " HDD"
    }

    # Output files from the task
    output {
        File new_bam = "~{new_bam}"
        File new_bai = "~{new_bai}"
        File new_md5 = "~{new_md5}"
    }
}
