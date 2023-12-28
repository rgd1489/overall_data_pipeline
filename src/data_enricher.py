# src/data_enricher.py
def read_country_mapping(file_path):
    country_mapping = {}
    with open(file_path, 'r') as file:
        for line in file:
            code, name = line.strip().split(',')
            country_mapping[code] = name
    return country_mapping

def enrich_data(data, mapping_file_path='src/country_mapping.txt'):
    # Implement data enrichment logic here

    # Read country mapping from the text file
    country_mapping = read_country_mapping(mapping_file_path)

    # Enrich data with location details based on the 'nat' column
    data['country_code'] = data['nat']
    data['country_name'] = data['nat'].map(country_mapping)

    # You can add more enrichment logic based on your specific requirements

    return data
