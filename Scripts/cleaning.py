import pandas as pd
from logger import setup_logger

logger = setup_logger()

# Dictionary for manual translation of non-English rating types
def translate_rating_type(value):
    """Translate non-English words in 'RATING_TYPE' to English."""
    translation_dict = {
        'Ä°yi': 'Good', 'Åšrednio': 'Average', 'Biasa': 'Average', 'Baik': 'Good',
        'Bom': 'Good', 'Bueno': 'Good', 'Buono': 'Good', 'Çok iyi': 'Very Good',
        'Dobrze': 'Good', 'Dobré': 'Good', 'Eccellente': 'Excellent', 'Excelente': 'Excellent',
        'Media': 'Average', 'MÃ©dia': 'Average', 'Muito bom': 'Good', 'Muy Bueno': 'Good',
        'Nedostatek hlasÅ¯': 'Poor', 'Not rated': 'Not rated', 'Ortalama': 'Average',
        'Ottimo': 'Excellent', 'PrÅ¯mÄ›r': 'Average', 'Priemer': 'Average', 'Promedio': 'Average',
        'SkvÄ›lÃ¡ volba': 'Excellent', 'SkvÄ›lÃ©': 'Very Good', 'Sangat Baik': 'Very Good',
        'Terbaik': 'Very Good', 'VeÄ¾mi dobrÃ©': 'Very Good', 'Ve?mi dobré': 'Very Good',
        'Velmi dobrÃ©': 'Very Good', 'Wybitnie': 'Excellent', 'İyi': 'Good',
        'Média': 'Average', 'Muito Bom': 'Good', 'Nedostatek hlasů': 'Poor',
        'Průměr': 'Average', 'Skvělá volba': 'Excellent', 'Skvělé': 'Very Good',
        'Średnio': 'Average', 'Veľmi dobré': 'Very Good', 'Velmi dobré': 'Very Good',
        'Vynikajúce': 'Excellent', 'Bardzo dobrze': 'Very Good'
    }
    return translation_dict.get(value, value)

def clean_data(df):
    """Clean and prepare the data."""
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

    logger.info('Removing duplicate rows based on NAME, CITY, and REGION')
    initial_count = len(df)
    df.drop_duplicates(subset=['NAME', 'CITY', 'REGION'], inplace=True)  # Remove duplicates based on these three columns
    removed_count = initial_count - len(df)
    logger.info(f'Removed {removed_count} duplicate rows')

    logger.info('Data cleaning process completed')
    return df

