import pandas as pd
import gzip

# TODO: add error handling for missing attributes
def parse_gff(file_path: str) -> pd.DataFrame:
    with gzip.open(file_path, 'rt') as f:
        gff_data = pd.read_csv(f, sep='\t', comment='#', header=None, names=[
            'seqid', 'source', 'feature', 'start', 'end', 'score', 'strand', 'phase', 'attributes'
        ])

    genes = gff_data[gff_data['feature'] == 'gene']

    def parse_attributes(attributes):
        attributes_dict = dict(attr.split('=') for attr in attributes.split(';'))
        return attributes_dict.get('ID'), attributes_dict.get('Name')

    genes[['gene_id', 'gene_name']] = genes['attributes'].apply(lambda x: pd.Series(parse_attributes(x)))

    return genes[['seqid', 'start', 'end', 'gene_id', 'gene_name', 'strand']]