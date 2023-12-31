-- data/database_schema.sql
CREATE TABLE IF NOT EXISTS user_accounts (
    id SERIAL PRIMARY KEY,
    game_name VARCHAR(10),
    gender VARCHAR(10),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    location_city VARCHAR(100),
    location_state VARCHAR(100),
    location_postcode VARCHAR(20),
    dob TIMESTAMP, 
    registered TIMESTAMP,
    phone VARCHAR(20),
    cell VARCHAR(20),
    country_code VARCHAR(10),
    picture_large VARCHAR(255),
    picture_medium VARCHAR(255),
    picture_thumbnail VARCHAR(255),
    country_name VARCHAR(50)    
);
