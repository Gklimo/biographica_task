import pytest
import pandas as pd
import gzip
from io import BytesIO
from etl.transform import parse_gff

@pytest.fixture
def sample_gff_content():
    content = (
        "##gff-version 3\n"
        "Chr1\tTAIR10\tgene\t3631\t5899\t.\t+\t.\tID=AT1G01010;Name=NAC001\n"
        "Chr1\tTAIR10\tmRNA\t3631\t5899\t.\t+\t.\tID=AT1G01010.1;Parent=AT1G01010;Name=NAC001.1\n"
        "Chr1\tTAIR10\tgene\t6790\t8737\t.\t-\t.\tID=AT1G01020;Name=ARV1\n"
    )
    return BytesIO(content.encode())

def test_parse_gff(sample_gff_content, tmp_path):
    # Create a temporary gzip file
    temp_file = tmp_path / "test.gff3.gz"
    with gzip.open(temp_file, 'wb') as f:
        f.write(sample_gff_content.getvalue())

    # Parse the GFF file
    result = parse_gff(str(temp_file))

    # Check the result
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2  # We expect 2 genes
    assert list(result.columns) == ['seqid', 'start', 'end', 'gene_id', 'gene_name', 'strand']

    # Check the content of the first row
    first_gene = result.iloc[0]
    assert first_gene['seqid'] == 'Chr1'
    assert first_gene['start'] == 3631
    assert first_gene['end'] == 5899
    assert first_gene['gene_id'] == 'AT1G01010'
    assert first_gene['gene_name'] == 'NAC001'
    assert first_gene['strand'] == '+'