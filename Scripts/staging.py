import pandas as pd
from sqlalchemy import create_engine

csv_file = 'Data\Processed\cleaned_enriched_data.csv'
df = pd.read_csv(csv_file)

# Database connection parameters
db_username = 'root'
db_password = '2616'
db_host = 'localhost'  
db_port = '3306'       
db_name = 'zomato_case_study'

engine = create_engine(f'mysql+mysqlconnector://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

# Load DataFrame into the MySQL staging table
df.to_sql('staging_table', con=engine, if_exists='replace', index=False)
print("Data loaded successfully into the staging table!")
