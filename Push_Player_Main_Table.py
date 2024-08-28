import psycopg2
from psycopg2 import sql, Error
import logging

class PushToNBAPlayersTable:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def push_profile_data_to_nba_players_table(self, profile_data_list):
        try:
            connection = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            cursor = connection.cursor()

            for profile_data in profile_data_list:
                player_name = profile_data['name']
                first_name = profile_data['first_name']
                last_name = profile_data['last_name']
                href = profile_data['href']
                img_src = profile_data['img_src']
                player_id = profile_data['player_id']

                # Check if the player already exists
                check_query = """
                    SELECT player_id FROM nba_players WHERE player_id = %s
                """
                cursor.execute(check_query, (player_id,))
                result = cursor.fetchone()

                if result:
                    logging.info(f"Player: {player_name} already exists in 'nba_players' table.")
                    continue

                # Insert new player data
                insert_query = """
                    INSERT INTO nba_players (full_name, first_name, last_name, href, img_src, player_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                values = (player_name, first_name, last_name, href, img_src, player_id)

                cursor.execute(insert_query, values)
                logging.info(f"Player: {player_name} - Added to 'nba_players' table.")

            connection.commit()  # Commit all the changes once after the loop

        except (Exception, psycopg2.Error) as error:
            logging.error(f"Error pushing profile data: {error}")

        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

# Set up logging
logging.basicConfig(level=logging.INFO)

