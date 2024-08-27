import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import certifi
import urllib3
import time
import random

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Function to extract latitude and longitude from the URL
def extract_lat_lon(url, retries=5):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36'
    }
    attempt = 0
    while attempt < retries:
        try:
            response = requests.get(url, headers=headers, verify=False)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                script_content = soup.find('script', string=lambda text: text and 'window.__PRELOADED_STATE__' in text)

                if script_content:
                    script_text = script_content.string

                    # Extract latitude
                    latitude_start_index = script_text.find(r'"latitude\":\"') + len(r'"latitude\":\"')
                    latitude_end_index = script_text.find('",', latitude_start_index)
                    latitude = script_text[latitude_start_index:latitude_end_index]

                    # Extract longitude
                    longitude_start_index = script_text.find(r'"longitude\":\"') + len(r'"longitude\":\"')
                    longitude_end_index = script_text.find('",', longitude_start_index)
                    longitude = script_text[longitude_start_index:longitude_end_index]

                    return latitude.replace('\\', ''), longitude.replace('\\', '')

            elif response.status_code == 429:
                # Handle rate limiting by pausing and retrying
                wait_time = 2 ** attempt + random.uniform(0, 1)
                print(f"Rate limit exceeded. Retrying after {wait_time:.2f} seconds...")
                time.sleep(wait_time)
                attempt += 1
            else:
                print(f"Failed to retrieve page: {url} - Status code: {response.status_code}")
                return None, None
        except requests.exceptions.RequestException as e:
            print(f"Request error for URL {url}: {e}")
            return None, None
    print(f"Failed to retrieve page after {retries} attempts: {url}")
    return None, None

# Function to process each row
def process_row(index, row):
    url = row['URL']
    latitude, longitude = extract_lat_lon(url)
    return index, latitude, longitude

# Load the CSV file
df = pd.read_csv('Data/Processed/combined_data.csv')

# Add new columns for latitude and longitude in the original dataframe
df['latitude'] = None
df['longitude'] = None

# Sample 5,000 random rows and include all rows where city="Kochi"
sampled_df = pd.concat([df[df['CITY'] == 'Kochi'], df[df['CITY'] != 'Kochi'].sample(n=10000, random_state=42)])

# Use ThreadPoolExecutor to parallelize the requests
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(process_row, index, row) for index, row in sampled_df.iterrows()]
    for future in tqdm(as_completed(futures), total=len(futures), desc="Processing URLs"):
        index, latitude, longitude = future.result()
        if latitude and longitude:
            df.at[index, 'latitude'] = latitude
            df.at[index, 'longitude'] = longitude

# Save the enriched DataFrame to a new CSV file
df.to_csv('Data/Processed/enriched_combined_data_test10_2.csv', index=False)
print("Data enrichment completed!")
