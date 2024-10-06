import pandas as pd
import gzip

def parse_attributes(attributes):
    attributes_dict = dict(attr.split('=') for attr in attributes.split(';') if '=' in attr)
    return attributes_dict.get('ID'), attributes_dict.get('Name')

def parse_gff(file_path: str) -> pd.DataFrame:
    with gzip.open(file_path, 'rt') as f:
        # Load the GFF data into a DataFrame
        gff_data = pd.read_csv(f, sep='\t', comment='#', header=None, names=[
            'seqid', 'source', 'feature', 'start', 'end', 'score', 'strand', 'phase', 'attributes'
        ])

    # Make a copy to avoid warnings
    gff_data = gff_data.copy()

    # Parse attributes to extract gene_id and gene_name
    gff_data[['gene_id', 'gene_name']] = gff_data['attributes'].apply(lambda x: pd.Series(parse_attributes(x)))

    # Convert columns to string before applying string methods
    gff_data['seqid'] = gff_data['seqid'].astype(str).str.lower().str.strip()
    gff_data['source'] = gff_data['source'].astype(str).str.lower().str.strip()
    gff_data['feature'] = gff_data['feature'].astype(str).str.lower().str.strip()
    gff_data['gene_id'] = gff_data['gene_id'].astype(str).str.lower().str.strip()

    # Replace NaN in gene_name explicitly with 'unknown'
    gff_data['gene_name'] = gff_data['gene_name'].fillna('unknown').astype(str).str.lower().str.strip()

    # Handle the score column properly: convert '.' to 0 and ensure numeric type
    gff_data['score'] = gff_data['score'].replace('.', '0')
    gff_data['score'] = pd.to_numeric(gff_data['score'], errors='coerce').fillna(0)

    # Return the full cleaned DataFrame with all columns
    return gff_data