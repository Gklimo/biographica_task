from etl.extract import download_gff
from etl.transform import parse_gff
from etl.load import load_to_delta_lake
import os

def run_etl(ensembl_version, organism_name, output_dir, delta_path):
    gff_file = download_gff(ensembl_version, organism_name, output_dir)
    gene_data = parse_gff(gff_file)
    load_to_delta_lake(gene_data, delta_path)

if __name__ == "__main__":
    ensembl_version = 52
    organism_name = "arabidopsis_thaliana"
    output_dir = "./output"
    delta_path = "/mnt/datalake/raw_genes_data"
    os.makedirs(output_dir, exist_ok=True)
    run_etl(ensembl_version, organism_name, output_dir, delta_path)
