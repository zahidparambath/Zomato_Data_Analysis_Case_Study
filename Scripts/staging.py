import pandas as pd
from sqlalchemy import create_engine
import logging

# Configure logging
logging.basicConfig(
    filename='log/etl_process.log',  
    level=logging.INFO,  
    format='%(asctime)s - %(levelname)s - %(message)s',  
    datefmt='%Y-%m-%d %H:%M:%S'  
)

# Log the start of the ETL process
logging.info('Starting the ETL process: Loading CSV to MySQL staging table.')

try:
    # Load the CSV file
    csv_file = 'Data\Processed\cleaned_enriched_data.csv'
    logging.info(f'Loading CSV file: {csv_file}')
    df = pd.read_csv(csv_file)
    logging.info(f'CSV file loaded successfully. Number of rows: {len(df)}')

    # Database connection parameters
    db_username = 'root'
    db_password = '2616'
    db_host = 'localhost'
    db_port = '3306'
    db_name = 'zomato_case_study'

    # Create database engine
    logging.info('Creating database engine.')
    engine = create_engine(f'mysql+mysqlconnector://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

    # Load DataFrame into the MySQL staging table
    logging.info('Loading DataFrame into MySQL staging table.')
    df.to_sql('staging_table', con=engine, if_exists='replace', index=False)
    logging.info('Data loaded successfully into the staging table!')

except Exception as e:
    logging.error(f'An error occurred: {e}')
finally:
    logging.info('ETL process completed.')
