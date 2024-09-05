import pandas as pd
from sqlalchemy import create_engine
import logging

def setup_logger(log_file='Logging/etl_process.log', log_level=logging.INFO):
    logging.basicConfig(filename=log_file, 
                        level=log_level, 
                        format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger()

logger = setup_logger()
def clean_data(df):
    logger.info('Starting the data cleaning process')

    logger.info('Cleaning the rating and votes column')
    df['RATING'] = df['RATING'].replace('NEW', None)
    df['VOTES'] = df['VOTES'].replace('NEW', None)

    df['RATING'] = pd.to_numeric(df['RATING'], errors='coerce').fillna(0).astype(int)
    df['VOTES'] = pd.to_numeric(df['VOTES'], errors='coerce').fillna(0).astype(int)

    logger.info('Cleaned the rating and votes column')

    df.drop_duplicates(inplace=True)
    logger.info('Duplicate rows removed')

    logger.info('Data cleaning process completed')
    return df

def load_to_mysql(df, table_name, engine):
    logger.info(f'Starting data load to MySQL table : {table_name}')

    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logger.info(f'Data loaded successfully into the {table_name} table')

    except Exception as e:
        logger.info(f'Error loading data into MySQL : {e}')

csv_file = r'Data\Processed\enriched_combined_data.csv'
logger.info(f'Loading data from {csv_file}')
df= pd.read_csv(csv_file)

df = clean_data(df)

db_username = 'root'
db_password = '2616'
db_host= 'localhost'
db_port = '3306'
db_name = 'zomato_case_study'

engine = create_engine(f'mysql+mysqlconnector://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')
load_to_mysql(df, 'staging_table', engine)
