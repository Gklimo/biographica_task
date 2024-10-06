import pytest
import pandas as pd
import gzip
from io import BytesIO
from etl.transform import parse_gff

@pytest.fixture
def sample_gff_content():
    """Fixture providing sample GFF content with 2 genes."""
    content = (
        "##gff-version 3\n"
        "Chr1\tTAIR10\tgene\t3631\t5899\t.\t+\t.\tID=AT1G01010;Name=NAC001\n"
        "Chr1\tTAIR10\tgene\t6790\t8737\t.\t-\t.\tID=AT1G01020\n"  # Missing gene name
    )
    return BytesIO(content.encode())

def test_parse_gff(sample_gff_content, tmp_path):
    """Test the parse_gff function with 2 genes."""
    # Create a temporary gzip file for testing
    temp_file = tmp_path / "test.gff3.gz"
    with gzip.open(temp_file, 'wb') as f:
        f.write(sample_gff_content.getvalue())

    # Parse the GFF file
    result = parse_gff(str(temp_file))

    # Basic DataFrame checks
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2  # We expect 2 genes
    assert not result.empty

    # Check column presence
    expected_columns = [
        'seqid', 'source', 'feature', 'start', 'end',
        'score', 'strand', 'phase', 'attributes',
        'gene_id', 'gene_name'
    ]
    assert all(col in result.columns for col in expected_columns)

    # Test first gene (normal case)
    first_gene = result.iloc[0]
    assert first_gene['seqid'] == 'chr1'
    assert first_gene['gene_id'] == 'at1g01010'
    assert first_gene['gene_name'] == 'nac001'
    assert first_gene['strand'] == '+'
    assert first_gene['score'] == 0  # '.' should be converted to 0

    # Test second gene (missing gene name)
    second_gene = result.iloc[1]
    assert second_gene['gene_id'] == 'at1g01020'
    assert second_gene['gene_name'] == 'unknown'  # Missing name should be 'unknown'
    assert second_gene['score'] == 0
