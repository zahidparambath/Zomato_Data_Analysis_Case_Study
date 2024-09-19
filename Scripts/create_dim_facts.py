import pandas as pd
from sqlalchemy import create_engine
from logger import setup_logger

logger = setup_logger()

def create_dimensions_and_facts(engine):
    logger.info('Starting the process of creating dimensions and facts')

    logger.info('Loading data from staging table')
    query = "SELECT * FROM staging_table"
    df = pd.read_sql(query, engine)
    print(df.columns)

    logger.info('Creating dim_restaurant dimension')

    # Drop duplicates based on relevant columns
    dim_restaurant = df[['NAME', 'CITY', 'REGION', 'CUSINE_CATEGORY', 'CUSINE TYPE', 'latitude', 'longitude']].drop_duplicates().reset_index(drop=True)

    # Generate an auto-incrementing RESTAURANT_ID
    dim_restaurant['RESTAURANT_ID'] = range(1, len(dim_restaurant) + 1)

    # Reorder the columns to put RESTAURANT_ID first
    dim_restaurant = dim_restaurant[['RESTAURANT_ID', 'NAME', 'CITY', 'REGION', 'CUSINE_CATEGORY', 'CUSINE TYPE', 'latitude', 'longitude']]

    # Save the dimension to the database
    dim_restaurant.to_sql('dim_restaurant', con=engine, if_exists='replace', index=False)

    logger.info('dim_restaurant dimension created successfully')
    logger.info('Creating dim_rating_type dimension')
    dim_rating_type = df[['RATING_TYPE']].drop_duplicates().reset_index(drop=True)
    dim_rating_type['RATING_TYPE_ID'] = range(1, len(dim_rating_type) + 1)
    dim_rating_type = dim_rating_type[['RATING_TYPE_ID', 'RATING_TYPE']]
    dim_rating_type.to_sql('dim_rating_type', con=engine, if_exists='replace', index=False)
    logger.info('dim_rating_type dimension created successfully')

    logger.info('Creating dim_time dimension')
    dim_time = df[['TIMING']].drop_duplicates().reset_index(drop=True)
    dim_time['TIMING_ID'] = range(1, len(dim_time) + 1)
    dim_time = dim_time[['TIMING_ID', 'TIMING']]
    dim_time.to_sql('dim_time', con=engine, if_exists='replace', index=False)
    logger.info('dim_time dimension created successfully')

    logger.info('Creating fact_restaurant fact table')
    # Map the IDs for rating_type and timing
    rating_type_mapping = dict(zip(dim_rating_type['RATING_TYPE'], dim_rating_type['RATING_TYPE_ID']))
    timing_mapping = dict(zip(dim_time['TIMING'], dim_time['TIMING_ID']))
    dim_restaurant_mapping = dict(zip(dim_restaurant['NAME'], dim_restaurant['RESTAURANT_ID']))

    df['RATING_TYPE_ID'] = df['RATING_TYPE'].map(rating_type_mapping)
    df['TIMING_ID'] = df['TIMING'].map(timing_mapping)
    df['RESTAURANT_ID'] = df['NAME'].map(dim_restaurant_mapping)

    # Create fact table with necessary fields
    fact_restaurant = df[['PRICE', 'RATING', 'VOTES', 'RATING_TYPE_ID', 'TIMING_ID', 'RESTAURANT_ID']]
    fact_restaurant = fact_restaurant.dropna(subset=['RATING_TYPE_ID', 'TIMING_ID', 'RESTAURANT_ID'])
    fact_restaurant.to_sql('fact_restaurant', con=engine, if_exists='replace', index=False)
    logger.info('fact_restaurant fact table created successfully')

    logger.info('Process completed successfully')

if __name__ == "__main__":
    db_username = 'root'
    db_password = '2616'
    db_host = 'localhost'
    db_port = '3306'
    db_name = 'zomato_case_study'

    engine = create_engine(f'mysql+mysqlconnector://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

    create_dimensions_and_facts(engine)
