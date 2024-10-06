from sqlalchemy import create_engine, text
import pandas as pd

# TODO: Sync data with upserts
def create_table_if_not_exists(engine, table_name):
    """Create a table for the GFF data if it doesn't exist."""
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        seqid TEXT,
        source TEXT,
        feature TEXT,
        start INT,
        "end" INT,    -- Need to escape the end keyword
        score FLOAT,  -- Allow decimal scores
        strand TEXT,
        phase TEXT,
        attributes TEXT,
        gene_id TEXT,
        gene_name TEXT
    );
    """
    with engine.connect() as connection:
        connection.execute(text(create_table_query))
        print(f"Table {table_name} created.")

def load_to_postgres(dataframe: pd.DataFrame, db_url: str, table_name: str):
    """Load the transformed DataFrame into a PostgreSQL table."""

    # Step 1: Create an engine to connect to PostgreSQL
    engine = create_engine(db_url, echo=True)

    # Step 2: Create the table if it doesn't exist
    create_table_if_not_exists(engine, table_name)

    # Step 3: Load the data into the PostgreSQL table
    dataframe.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"Data loaded successfully to table '{table_name}'.")
