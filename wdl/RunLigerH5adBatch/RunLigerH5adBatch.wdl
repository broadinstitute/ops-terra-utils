version 1.0

## Runs LIGER/src/run_liger_h5ad_batch.py (see run/run_liger_docker.sh for the
## equivalent local docker invocation) against a batch of .h5ad files, using
## the image pushed to:
##   us-central1-docker.pkg.dev/operations-portal-427515/liger/geneset-extractors-liger:md_liger

workflow run_liger_h5ad_batch {
  input {
    Array[File] h5ad_files
    String dataset_column = "donor_id"
    String cell_type_column = "cell_type__kp"
    String organism = "human"
    String genome_build = "hg38"
    Int max_cells_total = 50000
    Int liger_top_n_genes = 250
    Int extractor_top_k = 250
    String liger_k_grid = "10,12,14,16,18,20,22,24"
    Int liger_n_reps = 5
    Boolean overwrite = false
    Int mem_gb = 16
    Int disk_gb = 50
    String docker_image = "us-central1-docker.pkg.dev/operations-portal-427515/liger/geneset-extractors-liger:md_liger"
  }

  call liger_batch {
    input:
      h5ad_files = h5ad_files,
      dataset_column = dataset_column,
      cell_type_column = cell_type_column,
      organism = organism,
      genome_build = genome_build,
      max_cells_total = max_cells_total,
      liger_top_n_genes = liger_top_n_genes,
      extractor_top_k = extractor_top_k,
      liger_k_grid = liger_k_grid,
      liger_n_reps = liger_n_reps,
      overwrite = overwrite,
      docker_image = docker_image,
      mem_gb = mem_gb,
      disk_gb = disk_gb
  }

  output {
    File outputs_tar = liger_batch.outputs_tar
    File manifest = liger_batch.manifest
  }
}

task liger_batch {
  input {
    Array[File] h5ad_files
    String dataset_column
    String cell_type_column
    String organism
    String genome_build
    Int max_cells_total
    Int liger_top_n_genes
    Int extractor_top_k
    String liger_k_grid
    Int liger_n_reps
    Boolean overwrite
    String docker_image
    Int mem_gb
    Int disk_gb
  }

  command <<<
    set -euo pipefail

    mkdir -p input_root out_root
    for f in ~{sep=" " h5ad_files}; do
      ln -s "$f" "input_root/$(basename "$f")"
    done

    python /opt/dig-gene-set-extractors/LIGER/src/run_liger_h5ad_batch.py \
      --input_root input_root \
      --out_root out_root \
      --dataset_column "~{dataset_column}" \
      --cell_type_column "~{cell_type_column}" \
      --organism "~{organism}" \
      --genome_build "~{genome_build}" \
      --max_cells_total ~{max_cells_total} \
      --liger_top_n_genes ~{liger_top_n_genes} \
      --extractor_top_k ~{extractor_top_k} \
      --liger_k_grid "~{liger_k_grid}" \
      --liger_n_reps ~{liger_n_reps} \
      ~{if overwrite then "--overwrite" else ""}

    tar -czf liger_outputs.tar.gz -C out_root .
  >>>

  output {
    File outputs_tar = "liger_outputs.tar.gz"
    File manifest = "out_root/run_manifest.json"
  }

  runtime {
    docker: docker_image
    memory: "~{mem_gb} GB"
    disks: "local-disk ~{disk_gb} HDD"
  }
}