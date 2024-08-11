import mysql.connector
from mysql.connector import Error
import logging

class PushProfileToMySQL:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def push_profile_data_to_mysql(self, profile_data_list):
        try:
            with mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            ) as connection:
                cursor = connection.cursor()

                self.check_table_columns_existence(cursor)

                for profile_data in profile_data_list:
                    player_name = profile_data['name']
                    first_name = profile_data['first_name']
                    last_name = profile_data['last_name']
                    href = profile_data['href']
                    img_src = profile_data['img_src']
                    ppg = profile_data['ppg']
                    rpg = profile_data['rpg']
                    apg = profile_data['apg']
                    pie = profile_data['pie']

                    insert_query = """
                        INSERT INTO nba_players (full_name, first_name, last_name, href, img_src, ppg, rpg, apg, pie)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            ppg = VALUES(ppg),
                            rpg = VALUES(rpg),
                            apg = VALUES(apg),
                            pie = VALUES(pie)
                    """
                    values = (profile_data['name'], profile_data['first_name'], profile_data['last_name'],
                              profile_data['href'], profile_data['img_src'], ppg, rpg, apg, pie)

                    cursor.execute(insert_query, values)
                    connection.commit()

                    logging.info(f"Player: {player_name} first_name: {first_name} last_name: {last_name} href: {href} img_src: {img_src} - PPG: {ppg}, RPG: {rpg}, APG: {apg}, PIE: {pie} - Updated in MySQL")

        except mysql.connector.Error as error:
            logging.error(f"Error pushing profile data: {error}")

    def check_table_columns_existence(self, cursor):
        try:
            cursor.execute("DESCRIBE nba_players")
            columns = cursor.fetchall()

            required_columns = {'ppg', 'rpg', 'apg', 'pie'}
            existing_columns = {column[0] for column in columns}

            if not required_columns.issubset(existing_columns):
                logging.warning("Required columns are missing in the table.")
                self.add_columns_if_not_exist(cursor)

        except mysql.connector.Error as error:
            logging.error(f"Error checking columns existence: {error}")

    def add_columns_if_not_exist(self, cursor):
        try:
            cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'nba_players'")
            existing_columns = [column[0] for column in cursor.fetchall()]

            columns_to_add = ['ppg', 'rpg', 'apg', 'pie']

            for column in columns_to_add:
                if column not in existing_columns:
                    alter_query = f"ALTER TABLE nba_players ADD COLUMN {column} FLOAT"
                    cursor.execute(alter_query)
                    logging.info(f"Column '{column}' added successfully to 'nba_players' table.")

        except mysql.connector.Error as error:
            logging.error(f"Error adding columns: {error}")

# Set up logging
logging.basicConfig(level=logging.INFO)
