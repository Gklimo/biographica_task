# Ensembl ETL Pipeline

A Python-based ETL pipeline for downloading, transforming, and loading genome data from Ensembl into PostgreSQL.

## Overview

This ETL pipeline processes genetic data from the Ensembl database, specifically downloading and parsing GFF (General Feature Format) files for a given organism. The pipeline is designed to extract, transform, and load genomic data into a PostgreSQL database, making it ready for further analysis or integration into data workflows. It runs on an hourly schedule by default.

## Project Structure

- `main.py` - Pipeline orchestration and scheduling
- `etl/`
  - `extract.py` - Downloads GFF files from Ensembl FTP
  - `transform.py` - Parses and cleanses GFF data
  - `load.py` - Handles PostgreSQL database operations
- `tests/`
  - `test_extract.py` - Unit test for extract module
  - `test_transform.py` - Unit test for transform module

## Setup

1. Create a virtual environment and install dependencies:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

2. Run PostgreSQL locally and create database using port 5432:
```sql
CREATE DATABASE ensemblgenomes;
```

3. Configurre parameters in `main.py`:
```python
config = {
    "ensembl_version": 59,
    "organism_name": "arabidopsis_thaliana",
    "output_dir": "./output",
    "log_folder": "./logs",
    "db_url": "postgresql://username:password@localhost:5432/ensemblgenomes",
    "table_name": "genes_data"
}
```

## Running the Pipeline

### One-off run:
Uncomment the following line in `main.py`:
```python
# run_scheduled_pipeline(config=config, pipeline_name="Genome_ETL")
```

### Scheduled run:
```bash
python main.py
```
The pipeline will run every hour by default.

## Database Schema

The pipeline creates a table with the following structure:
```sql
CREATE TABLE genes_data (
    seqid TEXT,
    source TEXT,
    feature TEXT,
    start INT,
    end INT,
    score FLOAT,
    strand TEXT,
    phase TEXT,
    attributes TEXT,
    gene_id TEXT,
    gene_name TEXT
);
```

## Testing

Run unit tests:
```bash
pytest tests/ -v

```


### TODO:
1. **Implement Upserts**:
   - Modify the loading step to perform upserts to avoid full table refresh.

2. **DBT Integration**:
   - Use dbt for managing transformations, validation, and generating data lineage.

3. **Add Metadata Logging**:
   - Track each ETL run with details such as records processed and any errors encountered.

4. **Airflow/Dagster orchestration**:
   - For advanced scheduling, monitoring, and error handling.

5. **Expand Testing**:
   - Implement more unit tests for data load and create integration tests.

6. **Environmental Variables**:
   - To make the pipeline more configurable and flexible.

7. **Cloud Deployment**:
   - Explore deploying the pipeline on cloud platforms like AWS to enable automation.
