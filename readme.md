Project Structure

data/: Contains game data in JSON and CSV formats.

src/: Contains Python code's and country mapping txt file.

Dockerfile: Docker configuration for the application.

docker-compose.yml: Docker configuration for the PostgreSQL database.

requirements.txt: Python dependencies.

Docker Setup

..bash

docker-compose build

docker-compose up -d

Running Data Pipeline

docker-compose run app python main.py game_name

docker-compose run app python src/main.py wwc  
docker-compose run app python src/main.py hb
