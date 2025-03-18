version 1.0


workflow ReheaderArraysVcfFile {
    input {
        File input_vcf
        String new_sample_alias
        String chipwell_barcode
    }

    call ReplaceSampleAlias {
        input:
            input_vcf = input_vcf,
            new_sample_alias = new_sample_alias,
            chipwell_barcode = chipwell_barcode
    }

    call ReheaderVCF {
        input:
            vcf = ReplaceSampleAlias.reheadered_vcf,
            chipwell_barcode = chipwell_barcode,
            new_sample_alias = new_sample_alias
    }

    output {
        File reheadered_vcf = ReheaderVCF.reheadered_vcf
        File reheadered_vcf_index = ReheaderVCF.reheadered_vcf_index
    }

}

task ReplaceSampleAlias {
    input {
        File input_vcf
        String new_sample_alias
        String chipwell_barcode

        Int cpu = 1
        Int memory_mb = 16000
        Int disk_size_gb = ceil(3 * size(input_vcf, "GiB")) + 60
    }

    String reheadered_vcf = chipwell_barcode + ".vcf.gz"

    command {
        set -e

        echo "Getting original sample alias from vcf"
        original_sample_alias=$(zgrep 'sampleAlias' ~{input_vcf} | cut -d'=' -f2)

        echo "Updating VCF to replace the original sample alias with the new sample alias"
        gunzip -c  ~{input_vcf} | sed 's/$original_sample_alias/~{new_sample_alias}/g' | gzip > ~{reheadered_vcf}

    }

    runtime {
        docker: "us.gcr.io/broad-gotc-prod/picard-cloud:2.26.11"
        cpu: cpu
        memory: "~{memory_mb} MiB"
        disks: "local-disk ~{disk_size_gb} HDD"

    }

    output {
        File reheadered_vcf = "~{reheadered_vcf}"
    }
}


task ReheaderVCF {
    input {
        File vcf
        String chipwell_barcode
        String new_sample_alias

        Int cpu = 1
        Int memory_mb = 16000
        Int disk_size_gb = ceil(3 * size(vcf, "GiB")) + 60
    }

    String reheadered_vcf = chipwell_barcode + ".vcf.gz"
    String reheadered_vcf_index = reheadered_vcf + ".tbi"

    command <<<
        java -jar /usr/picard/picard.jar RenameSampleInVcf \
        INPUT=~{vcf} \
        OUTPUT=~{reheadered_vcf} \
        NEW_SAMPLE_NAME=~{new_sample_alias} \
        CREATE_INDEX=true
    >>>

    output {
        File reheadered_vcf = "~{reheadered_vcf}"
        File reheadered_vcf_index = "~{reheadered_vcf_index}"
    }

    runtime {
        docker: "us.gcr.io/broad-gotc-prod/picard-cloud:2.26.11"
        cpu: cpu
        memory: "~{memory_mb} MiB"
        disks: "local-disk ~{disk_size_gb} HDD"
    }

}
