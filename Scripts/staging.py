import pandas as pd
from sqlalchemy import create_engine
from logger import setup_logger

logger = setup_logger()

def load_to_mysql(df, table_name, engine):
    """Load data into a MySQL table."""
    logger.info(f'Starting data load to MySQL table: {table_name}')
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logger.info(f'Data loaded successfully into the {table_name} table')
    except Exception as e:
        logger.error(f'Error loading data into MySQL: {e}')

def stage_data_to_mysql(csv_file, db_credentials, table_name):
    """Stage the cleaned data into the MySQL database."""
    # Load the CSV data
    logger.info(f'Loading data from {csv_file}')
    df = pd.read_csv(csv_file)

    # Extract database connection details
    db_username = db_credentials['username']
    db_password = db_credentials['password']
    db_host = db_credentials['host']
    db_port = db_credentials['port']
    db_name = db_credentials['database']

    # Create a MySQL engine
    engine = create_engine(f'mysql+mysqlconnector://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')
    
    # Load the data into MySQL
    load_to_mysql(df, table_name, engine)
