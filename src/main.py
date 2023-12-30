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
        user=os.environ.get('DB_USER', 'postgres'),
        password=os.environ.get('DB_PASSWORD', 'postgres'),
        database=os.environ.get('DB_NAME', 'postgres')
    )

def setup_database():
    conn = create_connection()
    with conn.cursor() as cursor:
        with open('data/database_schema.sql', 'r') as setup_file:
            cursor.execute(setup_file.read())
    conn.commit()
    conn.close()

def load_and_transform_data():
    # Load data for 'wwc' game
    raw_wwc_data = load_data('wwc')
    cleaned_wwc_data = clean_data(raw_wwc_data)
    enriched_wwc_data = enrich_data(cleaned_wwc_data)
    enriched_wwc_data['game_name']= 'wwc'

    # Load data for 'hb' game
    raw_hb_data = load_data('hb')
    cleaned_hb_data = clean_data(raw_hb_data)
    enriched_hb_data = enrich_data(cleaned_hb_data)
    enriched_hb_data['game_name']= 'hb'

    # Combine the enriched data of both games
    combined_data = pd.concat([enriched_wwc_data, enriched_hb_data], ignore_index=True)
    
    return combined_data



def insert_data_into_database(data):
    conn = create_connection()
    with conn.cursor() as cursor:
        for _, row in data.iterrows():
            cursor.execute("""
                INSERT INTO user_accounts (game_name, gender, first_name, last_name, email,
                                          location_city, location_state, location_postcode,
                                          dob, registered, phone, cell, picture_large,
                                          picture_medium, picture_thumbnail, country_code, country_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['game_name'], row['gender'], row['first_name'], row['last_name'],
                row['email'], row['location_city'],
                row['location_state'], row['location_postcode'], pd.to_datetime(row['dob']),
                pd.to_datetime(row['registered']), row['phone'], row['cell'],
                row['picture_large'], row['picture_medium'], row['picture_thumbnail'], row['country_code'],
                row['country_name']
            ))
    conn.commit()
    conn.close()

def main():
    parser = ArgumentParser(description="Ovecell Data Pipeline")
    parser.add_argument("game_name", choices=['wwc', 'hb'], help="Game name ('wwc' or 'hb')")
    args = parser.parse_args()

    
    # Database setup
    setup_database()

    # Load and transform data
    enriched_data = load_and_transform_data(args.game_name)


    # Insert data into the database
    insert_data_into_database(enriched_data)

if __name__ == "__main__":
    main()


