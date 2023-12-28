# src/data_cleaner.py
def clean_data(data):
    # Implement data cleaning and transformation logic here

    # Handle missing location data
    data['location_street'].fillna('', inplace=True)
    data['location_city'].fillna('', inplace=True)
    data['location_state'].fillna('', inplace=True)
    data['location_postcode'].fillna('', inplace=True)

    # Add a country_code column based on the 'nat' column
    data['country_code'] = data['nat']

    # You can add more cleaning and transformation steps based on your specific requirements

    return data
