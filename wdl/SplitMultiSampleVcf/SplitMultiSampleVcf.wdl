version 1.0

# WORKFLOW DEFINITION
workflow SplitVcfWorkflow {
  input {
    File   multi_sample_vcf
    File   group_specs_file
    Boolean copy_outputs_to_final_destination
    String bcftoolsDocker   = "us.gcr.io/broad-gotc-prod/imputation-bcf-vcf:1.0.7-1.10.2-0.1.16-1669908889"
  }

  String pipeline_version = "0.0.0"

  Array[Object] group_specs = read_json(group_specs_file)

  scatter (group_spec in group_specs) {
    call ExtractGroup {
      input:
          multi_sample_vcf = multi_sample_vcf,
          prefix_          = group_spec.prefix,
          group_samples    = group_spec.samples,
          docker           = bcftoolsDocker
    }
    if (copy_outputs_to_final_destination) {
      call CopyVcfToFinalDestination {
        input:
            output_vcf = ExtractGroup.group_vcf,
            output_vcf_index = ExtractGroup.group_tbi,
            destination_workspace_bucket = group_spec.destination_workspace_bucket
      }
    }
  }

  output {
    Array[File] group_vcfs = ExtractGroup.group_vcf
    Array[File] group_tbis = ExtractGroup.group_tbi
  }
}

# TASK DEFINITIONS
task ExtractGroup {
  input {
    File          multi_sample_vcf
    String        prefix_
    Array[String] group_samples = group_samples
    String        docker
  }

  String output_vcf = prefix_ + "__" + basename(multi_sample_vcf)
  String output_tbi = output_vcf + ".tbi"

  command <<<
    set -o errexit
    set -o nounset
    set -o pipefail

    # -----------------------------------------------------------------

    SAMPLEIDS=~{write_lines(group_samples)}

    # -----------------------------------------------------------------

    bcftools                          \
        view                          \
        --samples-file "${SAMPLEIDS}" \
        --output-file '~{output_vcf}' \
        --output-type z               \
        '~{multi_sample_vcf}'

    # -----------------------------------------------------------------
    bcftools index --force --tbi '~{output_vcf}'

    >>>

  output {
    File group_vcf = output_vcf
    File group_tbi = output_tbi
  }

  runtime {
    docker      : docker
    cpu         : 4
    cpuPlatform : "Intel Ice Lake"
    memory      : "16 GB"
    disks       : "local-disk 800 SSD"
  }
}

task CopyVcfToFinalDestination {
  input {
    File output_vcf
    File output_vcf_index
    String destination_workspace_bucket
  }

  command <<<
    DESTINATION_OUTPUT_DIRECTORY="gs://~{destination_workspace_bucket}/vcf_parsing_output/"

    gsutil cp ~{output_vcf} $DESTINATION_OUTPUT_DIRECTORY
    gsutil cp ~{output_vcf_index} $DESTINATION_OUTPUT_DIRECTORY

    echo "Copied ~{output_vcf} and ~{output_vcf_index} to $DESTINATION_OUTPUT_DIRECTORY"
  >>>

  runtime {
    docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
  }
}
