import pandas as pd
df = pd.read_csv('Data\Processed\enriched_combined_data.csv')

# Replace "NEW" with None (or you can replace with 0 if preferred)
df['RATING'] = df['RATING'].replace('NEW', None)
df['VOTES'] = df['VOTES'].replace('NEW', None)
# Convert the RATING column to numeric (integer), handling any remaining non-numeric entries
df['RATING'] = pd.to_numeric(df['RATING'], errors='coerce').fillna(0).astype(int)
df['VOTES'] = pd.to_numeric(df['VOTES'], errors='coerce').fillna(0).astype(int)

df.to_csv('Data\Processed\cleaned_enriched_data.csv', index=False)
print("Rating column cleaned and saved to 'cleaned_enriched_data.csv'")
