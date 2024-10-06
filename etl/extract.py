import requests
from urllib.parse import urljoin
import os

def download_gff(ensembl_version: int, organism_name: str, chromosome: str, output_dir: str) -> str:
    # Ensure the output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Directory '{output_dir}' created.")

    base_url = f"https://ftp.ensemblgenomes.ebi.ac.uk/pub/plants/release-{ensembl_version}/gff3/{organism_name}/"
    gff_file = f"{organism_name.capitalize()}.TAIR10.{ensembl_version}.{chromosome}.gff3.gz"
    full_url = urljoin(base_url, gff_file)
    local_path = os.path.join(output_dir, gff_file)

    # Download the file
    response = requests.get(full_url)
    if response.status_code == 200:
        with open(local_path, 'wb') as f:
            f.write(response.content)
        return local_path
    else:
        raise Exception(f"Failed to download GFF file for {organism_name} {chromosome} (status code: {response.status_code})")
