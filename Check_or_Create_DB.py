import mysql.connector
from mysql.connector import Error
import logging

class Check_or_Create_DB:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def create_database(self):
        try:
            connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password
            )
            cursor = connection.cursor()

            # Create the database
            cursor.execute(f"CREATE DATABASE {self.database}")
            logging.info(f"Database '{self.database}' created successfully.")

        except mysql.connector.Error as error:
            logging.error(f"Error creating database: {error}")

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

    def create_table(self):
        try:
            with mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            ) as connection:
                cursor = connection.cursor()

                create_table_query = """
                    CREATE TABLE IF NOT EXISTS nba_players (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        full_name VARCHAR(255) NOT NULL,
                        first_name VARCHAR(255) NOT NULL,
                        last_name VARCHAR(255) NOT NULL,
                        href VARCHAR(255) UNIQUE NOT NULL COLLATE utf8_general_ci,
                        img_src VARCHAR(255) NOT NULL,
                        ppg FLOAT,
                        rpg FLOAT,
                        apg FLOAT,
                        pie FLOAT
                    )
                """
                cursor.execute(create_table_query)
                connection.commit()
                logging.info("Table 'nba_players' created successfully or already exists.")

        except mysql.connector.Error as error:
            logging.error(f"Error creating table: {error}")


# Set up logging
logging.basicConfig(level=logging.INFO)
