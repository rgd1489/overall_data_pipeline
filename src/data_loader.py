# src/data_loader.py
import pandas as pd
import os

def load_data(game_name):
    data_path = f"data/{game_name}"

    if game_name == 'wwc':
        # Load JSON files for 'wwc' game
        json_files = []
        for root, dirs, files in os.walk(data_path):
            for file in files:
                if file.endswith(".json"):
                    json_files.append(os.path.join(root, file))

        wwc_data = pd.concat([pd.read_json(json_file) for json_file in json_files], ignore_index=True)
        return wwc_data

    elif game_name == 'hb':
        # Load CSV files for 'hb' game
        csv_files = []
        for root, dirs, files in os.walk(data_path):
            for file in files:
                if file.endswith(".csv"):
                    csv_files.append(os.path.join(root, file))

        hb_data = pd.concat([pd.read_csv(csv_file) for csv_file in csv_files], ignore_index=True)
        return hb_data

    else:
        raise ValueError(f"Unsupported game_name: {game_name}")
