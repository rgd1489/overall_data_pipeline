# src/main.py
import sys
import os
import pandas as pd
import psycopg2
from data_loader import load_data
from data_cleaner import clean_data
from data_enricher import enrich_data
from argparse import ArgumentParser

def create_connection():
    return psycopg2.connect(
        host=os.environ.get('DB_HOST', 'localhost'),
        port=os.environ.get('DB_PORT', '5432'),
        user=os.environ.get('DB_USER', 'ovecell_user'),
        password=os.environ.get('DB_PASSWORD', 'ovecell_password'),
        database=os.environ.get('DB_NAME', 'ovecell_db')
    )

def setup_database():
    conn = create_connection()
    with conn.cursor() as cursor:
        with open('data/database_schema.sql', 'r') as setup_file:
            cursor.execute(setup_file.read())
    conn.commit()
    conn.close()

def load_and_transform_data(game_name):
    # Load data
    raw_data = load_data(game_name)

    # Transform data based on game name
    cleaned_data = clean_data(raw_data)

    # Enrich data with locations
    enriched_data = enrich_data(cleaned_data)

    return enriched_data

def append_county_location_to_csv_data(csv_data, enriched_data):
    # Append county value and location to CSV data based on common fields
    merged_data = pd.merge(csv_data, enriched_data, how='left',
                           on=['first_name','last_name', 'dob', 'email'], suffixes=('', '_enriched'))
    return merged_data


def insert_data_into_database(data):
    conn = create_connection()
    with conn.cursor() as cursor:
        for _, row in data.iterrows():
            # Extract CSV-specific values
            csv_specific_columns = ['first_name', 'last_name', 'email', 'gender', 'ip_address', 'dob', 'location_street',
                                    'location_city', 'location_state', 'location_postcode','login_username', 'login_password',
                                    'login_salt', 'login_md5', 'login_sha1', 'login_sha256', 'registered', 'phone', 'cell',
                                    'id', 'value', 'picture_large', 'picture_medium', 'picture_thumbnail', 'nat']
            csv_specific_values = [row[col] if col in row.index else None for col in csv_specific_columns]

            # Extract JSON-specific values with aliasing
            json_specific_columns = [
                'gender',
                'name_title as title', 'name_first as first_name', 'name_last as last_name',
                'location_street', 'location_city', 'location_state', 'location_postcode',
                'email',
                'login_username', 'login_password', 'login_salt', 'login_md5', 'login_sha1', 'login_sha256',
                'dob',
                'registered',
                'phone',
                'cell',
                'id_name as id', 'id_value as value',
                'picture_large', 'picture_medium', 'picture_thumbnail',
                'nat'
            ]
            json_specific_values = [
                row['gender'],
                row['name_title'], row['name_first'], row['name_last'],
                row['location_street'], row['location_city'], row['location_state'], row['location_postcode'],
                row['email'],
                row['login_username'], row['login_password'], row['login_salt'], row['login_md5'], row['login_sha1'], row['login_sha256'],
                row['dob'],
                row['registered'],
                row['phone'],
                row['cell'],
                row['id_name'], row['id_value'],
                row['picture_large'], row['picture_medium'], row['picture_thumbnail'],
                row['nat']
            ]

            # Handle missing values
            json_specific_values = [value if value is not None else None for value in json_specific_values]

            cursor.execute("""
                INSERT INTO user_accounts (game_name, user_id, gender, first_name, last_name, email,
                                          location_street, location_city, location_state, location_postcode,
                                          dob, registered, phone, cell, country_code, picture_large,
                                          picture_medium, picture_thumbnail, country_code, country_name, location)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['game_name'], row['user_id'], row['gender'], row['first_name'], row['last_name'],
                row['email'], row['location_street'], row['location_city'],
                row['location_state'], row['location_postcode'], pd.to_datetime(row['dob']),
                pd.to_datetime(row['registered']), row['phone'], row['cell'], row['country_code'],
                row['picture_large'], row['picture_medium'], row['picture_thumbnail'],
                row['country_name'], row['location']
            ))
    conn.commit()
    conn.close()


def main():
    parser = ArgumentParser(description="Ovecell Data Pipeline")
    parser.add_argument("game_name", choices=['wwc', 'hb'], help="Game name ('wwc' or 'hb')")
    parser.add_argument("date", help="Date in format 'YYYY-MM-DD'")
    ##parser.add_argument("csv_file_path", help="Path to the CSV file")

    args = parser.parse_args()

    # Database setup
    setup_database()

    # Load and transform data
    enriched_data = load_and_transform_data(args.game_name, args.date)

    # Load CSV data
    csv_data = pd.read_csv(args.csv_file_path)

    # Append county and location to CSV data
    merged_data = append_county_location_to_csv_data(csv_data, enriched_data)

    # Insert data into the database
    insert_data_into_database(merged_data)

if __name__ == "__main__":
    main()
