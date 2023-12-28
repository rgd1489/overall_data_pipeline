import psycopg2
from datetime import date

class Database:
    def __init__(self, host, port, dbname, user, password):
        self.connection = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        self.cursor = self.connection.cursor()

    def gender_ratio_per_game(self):
        # Query to find the gender ratio in each game
        query = """
        SELECT game_name, COUNT(*) AS total_players,
               COUNT(CASE WHEN gender = 'Male' THEN 1 END) AS male_players,
               COUNT(CASE WHEN gender = 'Female' THEN 1 END) AS female_players
        FROM users
        GROUP BY game_name;
        """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        return result

    def youngest_oldest_players_per_country(self):
        # Query to find the youngest and oldest players per country
        query = """
        SELECT location,
               MIN(registration_date) AS youngest_player,
               MAX(registration_date) AS oldest_player
        FROM users
        GROUP BY location;
        """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        return result

# Example usage
if __name__ == "__main__":
    db = Database(host='localhost', port=5432, dbname='ovecell_data', user='ovecell_user', password='ovecell_password')

    # Gender ratio in each game
    gender_ratio_result = db.gender_ratio_per_game()
    print("Gender Ratio in Each Game:")
    for row in gender_ratio_result:
        game_name, total_players, male_players, female_players = row
        print(f"{game_name}: Male - {male_players}/{total_players}, Female - {female_players}/{total_players}")

    # Youngest and oldest players per country
    youngest_oldest_result = db.youngest_oldest_players_per_country()
    print("\nYoungest and Oldest Players per Country:")
    for row in youngest_oldest_result:
        location, youngest_player, oldest_player = row
        print(f"{location}: Youngest - {youngest_player}, Oldest - {oldest_player}")

    
