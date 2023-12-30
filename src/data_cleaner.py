# src/data_cleaner.py
from data_loader import load_data  # Import the load_data function from data_loader.py

def clean_data(data):
    # Implement data cleaning and transformation logic here

    # Handling missing data in columns 
    
    if 'location_street' in data.columns:
        data['location_street'].fillna('', inplace=True)
    if 'location_city' in data.columns:
        data['location_city'].fillna('', inplace=True)
    if 'location_state' in data.columns:
        data['location_state'].fillna('', inplace=True)
    if 'location_postcode' in data.columns:
        data['location_postcode'].fillna('', inplace=True)

    # Add a country_code column based on the 'nat' column
    if 'nat' in data.columns:
        data['country_code'] = data['nat']

    print("Loaded clean data successfully!")
    return data

# Load 'wwc' data
wwc_loaded_data = load_data('wwc')

# Clean the loaded 'wwc' data
cleaned_wwc_data = clean_data(wwc_loaded_data)
print(cleaned_wwc_data.head())

# Load 'hb' data
hb_loaded_data = load_data('hb')

# Clean the loaded 'hb' data
cleaned_hb_data = clean_data(hb_loaded_data)
print(cleaned_hb_data.head())
