import pandas as pd
from sqlalchemy import create_engine
from extraction import extract_lat_lon_for_data
from cleaning import clean_data
from staging import stage_data_to_mysql
from create_dim_facts import create_dimensions_and_facts
from tqdm import tqdm
from logger import setup_logger

logger = setup_logger()

def run_pipeline():
    logger.info("Starting ETL pipeline")

    # Load the data
    input_csv = r'Data/Processed/combined_data.csv'
    df = pd.read_csv(input_csv)
    logger.info(f"Loaded data from {input_csv}")

    # Perform extraction
    df = extract_lat_lon_for_data(df)

    # Perform cleaning
    df = clean_data(df)

    # Save the cleaned and enriched data locally
    output_csv = r'Data/Processed/enriched_cleaned_data.csv'
    df.to_csv(output_csv, index=False)
    logger.info(f"Data enrichment and cleaning completed and saved to {output_csv}")

    # Stage the cleaned data into MySQL
    db_credentials = {
        'username': 'root',
        'password': '2616',
        'host': 'localhost',
        'port': '3306',
        'database': 'zomato_case_study'
    }
    logger.info(f"Staging data into MySQL table 'staging_table'")
    stage_data_to_mysql(output_csv, db_credentials, 'staging_table')
    logger.info("Staging process completed")

    # Create dimensions and facts
    engine = create_engine(f"mysql+mysqlconnector://{db_credentials['username']}:{db_credentials['password']}@{db_credentials['host']}:{db_credentials['port']}/{db_credentials['database']}")
    
    logger.info("Creating dimensions and facts")
    create_dimensions_and_facts(engine)
    logger.info("Dimensions and Facts created successfully")
    

if __name__ == "__main__":
    run_pipeline()
    logger.info("Pipeline execution completed")
