import schedule
import time
import os
from etl.extract import download_gff
from etl.transform import parse_gff
from etl.load import load_to_postgres
from pipeline_logging import PipelineLogging

def etl_pipeline(config, logger):
    try:
        logger.info("Starting Genome ETL pipeline.")

        # Extract
        logger.info(f"Downloading GFF file for organism {config['organism_name']}, version {config['ensembl_version']}.")
        gff_file_path = download_gff(config['ensembl_version'], config['organism_name'], "chromosome.1", config['output_dir'])
        logger.info(f"GFF file downloaded successfully to {gff_file_path}.")

        # Transform
        logger.info("Starting the transformation of the GFF data.")
        genes_df = parse_gff(gff_file_path)
        logger.info(f"Transformation complete. Parsed {len(genes_df)} gene records.")

        # Load
        db_url = config['db_url']
        table_name = config['table_name']
        logger.info(f"Loading data to PostgreSQL table: {table_name}")
        load_to_postgres(genes_df, db_url, table_name)
        logger.info("Data loaded successfully to PostgreSQL.")

    except Exception as e:
        logger.error(f"Error during pipeline execution: {e}")

# Set up the scheduler
def run_scheduled_pipeline(config, pipeline_name):
    os.makedirs(config["log_folder"], exist_ok=True)  # Ensure the log folder exists
    pipeline_logger = PipelineLogging(pipeline_name=pipeline_name, log_folder_path=config["log_folder"])
    logger = pipeline_logger.logger
    etl_pipeline(config=config, logger=logger)
    pipeline_logger.logger.handlers.clear()

if __name__ == "__main__":
    config = {
        "ensembl_version": 59,
        "organism_name": "arabidopsis_thaliana",
        "output_dir": "./output",
        "log_folder": "./logs",
        "db_url": "postgresql://postgres:password@localhost:5432/ensemblgenomes",  # Make sure the database name is included
        "table_name": "genes_data"
    }

    # # One-off run of the pipeline
    run_scheduled_pipeline(config=config, pipeline_name="Genome_ETL")

    # # Schedule the pipeline to run every hour
    # schedule.every().hour.do(run_scheduled_pipeline, config=config, pipeline_name="Genome_ETL")

    # # Keep the scheduler running with polling every 5 minutes
    # while True:
    #     schedule.run_pending()
    #     time.sleep(300)  # Sleep for 300 seconds (5 minutes)
