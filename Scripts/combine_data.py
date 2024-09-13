import os
import pandas as pd
from tqdm import tqdm

def load_and_combine_data(data_dir):
    combined_data = []

    city_dirs = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]
    for city in tqdm(city_dirs, desc='Processing cities'):
        city_path = os.path.join(data_dir, city)
        for csv_file in tqdm([f for f in os.listdir(city_path) if f.endswith('.csv')],
                             desc=f'Processing files in {city}', leave=False):
            csv_path = os.path.join(city_path, csv_file)
            city_df = pd.read_csv(csv_path, delimiter='|')
            combined_data.append(city_df)

    combined_df = pd.concat(combined_data, ignore_index=True)
    return combined_df

data_dir = r'Data\Raw\Zomato_Dataset'
combined_df = load_and_combine_data(data_dir)
print(combined_df.head())
combined_df.to_csv(r'Data\Processed\combined_data.csv', index=False)
