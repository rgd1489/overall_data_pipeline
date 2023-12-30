# src/data_enricher.py
from data_loader import load_data  # Import the load_data function from data_loader.py
from data_cleaner import clean_data  # Import the load_data function from data_loader.py

def read_country_mapping(file_path):
    country_mapping = {}
    with open(file_path, 'r') as file:
        for line_number, line in enumerate(file, start=1):
            try:
                code, name = line.strip().split(',')
                country_mapping[code] = name
            except ValueError:
                print(f"Error in line {line_number}: {line}")
    return country_mapping


def enrich_data(data, mapping_file_path='src/country_mapping.txt'):
    # Implement data enrichment logic here
    # Read country mapping from the text file
    
    country_mapping = read_country_mapping(mapping_file_path)

    # Enrich data with location details based on the 'nat' column
    data['country_code'] = data['nat']
    data['country_name'] = data['nat'].map(country_mapping)

    # You can add more enrichment logic based on your specific requirements
    print("mapping is done successfully")
    return data


# Load 'wwc' data
wwc_loaded_data = load_data('wwc')

# Clean the loaded 'wwc' data
cleaned_wwc_data = clean_data(wwc_loaded_data)

# Enrich the cleaned 'wwc' data
enriched_wwc_data = enrich_data(cleaned_wwc_data)
print(enriched_wwc_data.columns)

# Load 'hb' data
hb_loaded_data = load_data('hb')

# Clean the loaded 'hb' data
cleaned_hb_data = clean_data(hb_loaded_data)

# Enrich the cleaned 'hb' data
enriched_hb_data = enrich_data(cleaned_hb_data)
print(enriched_hb_data.columns)
