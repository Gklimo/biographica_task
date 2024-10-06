import unittest
from unittest.mock import patch, MagicMock
import os
from etl.extract import download_gff  # Adjust the import based on your module structure

class TestDownloadGFF(unittest.TestCase):

    @patch('requests.get')
    def test_download_gff_success(self, mock_get):
        # Mocking a successful download
        mock_get.return_value = MagicMock(status_code=200, content=b'fake_gff_content')

        # Define test inputs
        ensembl_version = 59
        organism_name = "arabidopsis_thaliana"
        chromosome = "1"
        output_dir = "./output"

        # Run the function
        result = download_gff(ensembl_version, organism_name, chromosome, output_dir)

        # Assert the file path is returned correctly
        expected_file_path = os.path.join(output_dir, f"{organism_name.capitalize()}.TAIR10.{ensembl_version}.{chromosome}.gff3.gz")
        self.assertEqual(result, expected_file_path)

    @patch('requests.get')
    def test_download_gff_failure(self, mock_get):
        # Mocking a failed download
        mock_get.return_value = MagicMock(status_code=404)

        # Define test inputs
        ensembl_version = 52
        organism_name = "invalid_organism"
        chromosome = "1"
        output_dir = "./output"

        # Assert an exception is raised for a failed download
        with self.assertRaises(Exception):
            download_gff(ensembl_version, organism_name, chromosome, output_dir)

if __name__ == '__main__':
    unittest.main()
