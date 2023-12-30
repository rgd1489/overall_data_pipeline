import pandas as pd
import os
import json
import csv
from pandas import json_normalize

def load_data(game_name):
    data_path = f"data/{game_name}"

    if not os.path.exists(data_path):
        raise ValueError(f"Invalid game_name: {game_name}")

    if game_name == 'wwc':
        json_files = [os.path.join(root, file) for root, _, files in os.walk(data_path) for file in files if file.endswith(".json")]

        wwc_data = []
        for json_file in json_files:
            with open(json_file, 'r', encoding='utf-8') as file:
                lines = file.readlines()
                for line in lines:
                    wwc_data.append(json.loads(line)) 

        wwc_data = json_normalize(wwc_data, sep='')

        # Rename specific columns
        wwc_data = wwc_data.rename(columns={
            'namefirst': 'first_name',
            'namelast': 'last_name',
            'locationcity': 'location_city',
            'locationstate': 'location_state',
            'locationpostcode': 'location_postcode',
            'picturelarge': 'picture_large',
            'picturemedium': 'picture_medium',
            'picturethumbnail': 'picture_thumbnail'
        })

        selected_columns = ['gender', 'first_name', 'last_name', 'location_city', 'location_state', 'location_postcode', 'picture_large', 'picture_medium', 'picture_thumbnail',
                            'email', 'dob', 'nat', 'registered', 'phone', 'cell']
        wwc_data = wwc_data[selected_columns]

        wwc_data['game_name'] = game_name
        return wwc_data

    elif game_name == 'hb':
        # Load CSV files for 'hb' game
        csv_files = []
        columns_to_load = ['first_name', 'last_name', 'email', 'gender','dob']  # Specify the columns you want

        for root, dirs, files in os.walk(data_path):
            for file in files:
                if file.endswith(".csv"):
                    csv_files.append(os.path.join(root, file))

        hb_data = pd.concat([pd.read_csv(csv_file) for csv_file in csv_files], ignore_index=True)

        # Add 'game_name' column
        hb_data['game_name'] = game_name

        # Add additional columns with null values
        additional_columns = ['location_city', 'location_state', 'location_postcode', 'registered',
                              'phone', 'cell', 'picture_large', 'picture_medium', 'picture_thumbnail', 'nat']
        for col in additional_columns:
            for col in additional_columns:
             hb_data[col] = None
            
        return hb_data

    else:
        raise ValueError(f"Unsupported game_name: {game_name}")

# Loading 'wwc' and 'hb' data into separate DataFrames
wwc_data = load_data('wwc')
hb_data = load_data('hb')

# Assigning 'game_name' to differentiate records
wwc_data['game_name'] = 'wwc'
hb_data['game_name'] = 'hb'

# Merging based on specified columns and keeping all records
merged_data = pd.concat([wwc_data, hb_data], ignore_index=True)

# Printing first 50 rows for both game names
print("First 50 rows for 'wwc' game:")
print(merged_data[merged_data['game_name'] == 'wwc'].head(50))

print("\nFirst 50 rows for 'hb' game:")
print(merged_data[merged_data['game_name'] == 'hb'].head(50))



