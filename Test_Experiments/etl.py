import pandas as pd
from sqlalchemy import create_engine
import logging

def setup_logger(log_file='Logging/etl_process.log', log_level=logging.INFO):
    logging.basicConfig(filename=log_file, 
                        level=log_level, 
                        format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger()

logger = setup_logger()

# Dictionary for manual translation of non-English rating types
def translate_rating_type(value):
    """Translate non-English words in 'RATING_TYPE' to English."""
    translation_dict = {
        'Ä°yi': 'Good',
        'Åšrednio': 'Average',
        'Biasa': 'Average',
        'Baik': 'Good',
        'Bom': 'Good',
        'Bueno': 'Good',
        'Buono': 'Good',
        'Çok iyi': 'Very Good',
        'Dobrze': 'Good',
        'Dobré': 'Good',
        'Eccellente': 'Excellent',
        'Excelente': 'Excellent',
        'Media': 'Average',
        'MÃ©dia': 'Average',
        'Muito bom': 'Good',
        'Muy Bueno': 'Good',
        'Nedostatek hlasÅ¯': 'Poor',
        'Not rated': 'Not rated',
        'Ortalama': 'Average',
        'Ottimo': 'Excellent',
        'PrÅ¯mÄ›r': 'Average',
        'Priemer': 'Average',
        'Promedio': 'Average',
        'SkvÄ›lÃ¡ volba': 'Excellent',
        'SkvÄ›lÃ©': 'Very Good',
        'Sangat Baik': 'Very Good',
        'Terbaik': 'Very Good',
        'VeÄ¾mi dobrÃ©': 'Very Good',
        'Ve?mi dobré': 'Very Good',
        'Velmi dobrÃ©': 'Very Good',
        'Wybitnie': 'Excellent',
        'İyi': 'Good',
        'Média': 'Average',
        'Muito Bom': 'Good',
        'Nedostatek hlasů': 'Poor',
        'Průměr': 'Average',
        'Skvělá volba': 'Excellent',
        'Skvělé': 'Very Good',
        'Średnio': 'Average',
        'Veľmi dobré': 'Very Good',
        'Velmi dobré': 'Very Good',
        'Vynikajúce': 'Excellent',
        'Bardzo dobrze': 'Very Good'
    }
    return translation_dict.get(value, value)

def clean_data(df):
    logger.info('Starting the data cleaning process')

    # Cleaning RATING and VOTES columns
    logger.info('Cleaning the RATING and VOTES columns')
    df['RATING'] = df['RATING'].replace('NEW', None)
    df['VOTES'] = df['VOTES'].replace('NEW', None)

    df['RATING'] = pd.to_numeric(df['RATING'], errors='coerce').fillna(0).astype(int)
    df['VOTES'] = pd.to_numeric(df['VOTES'], errors='coerce').fillna(0).astype(int)
    logger.info('Cleaned the RATING and VOTES columns successfully')

    # Translating non-English words in RATING_TYPE
    logger.info('Translating non-English words in the RATING_TYPE column')
    df['RATING_TYPE'] = df['RATING_TYPE'].apply(lambda x: translate_rating_type(x))
    logger.info('Translation of non-English words in RATING_TYPE completed')

    # Removing duplicates
    logger.info('Removing duplicate rows')
    df.drop_duplicates(inplace=True)
    logger.info('Duplicate rows removed')

    logger.info('Data cleaning process completed')
    return df

def load_to_mysql(df, table_name, engine):
    logger.info(f'Starting data load to MySQL table: {table_name}')
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logger.info(f'Data loaded successfully into the {table_name} table')
    except Exception as e:
        logger.error(f'Error loading data into MySQL: {e}')

csv_file = r'Data\Processed\enriched_combined_data.csv'
logger.info(f'Loading data from {csv_file}')
df = pd.read_csv(csv_file)

df = clean_data(df)

db_username = 'root'
db_password = '2616'
db_host= 'localhost'
db_port = '3306'
db_name = 'zomato_case_study'

engine = create_engine(f'mysql+mysqlconnector://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')
load_to_mysql(df, 'staging_table', engine)
