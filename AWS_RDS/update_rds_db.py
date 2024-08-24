import psycopg2
import logging
import os

class UpdateAWSRDS:
    def __init__(self, local_db_config, rds_db_config):
        self.local_db_config = local_db_config
        self.rds_db_config = rds_db_config

    def fetch_local_data(self):
        """Fetch data from the local PostgreSQL database."""
        try:
            local_conn = psycopg2.connect(**self.local_db_config)
            local_cursor = local_conn.cursor()
            local_cursor.execute("SELECT * FROM nba_players")
            data = local_cursor.fetchall()
            local_cursor.close()
            local_conn.close()
            return data
        except Exception as e:
            logging.error(f"Error fetching data from local database: {e}")
            return []

    def update_rds_data(self, data):
        """Update the AWS RDS PostgreSQL database with the local data."""
        try:
            rds_conn = psycopg2.connect(**self.rds_db_config)
            rds_cursor = rds_conn.cursor()

            # Create table if it doesn't exist
            create_table_query = """
            CREATE TABLE IF NOT EXISTS player_data_rds_schema.player_data_rds (
                id SERIAL PRIMARY KEY,
                full_name VARCHAR(255) NOT NULL,
                first_name VARCHAR(255) NOT NULL,
                last_name VARCHAR(255) NOT NULL,
                href VARCHAR(255) UNIQUE NOT NULL,
                img_src VARCHAR(255) NOT NULL,
                ppg FLOAT,
                rpg FLOAT,
                apg FLOAT,
                pie FLOAT
            );
            """
            rds_cursor.execute(create_table_query)
            logging.info("Table 'player_data_rds' created successfully.")

            # Insert or update data
            insert_query = """
                INSERT INTO player_data_rds_schema.player_data_rds (id, full_name, first_name, last_name, href, img_src, ppg, rpg, apg, pie)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    full_name = EXCLUDED.full_name,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    href = EXCLUDED.href,
                    img_src = EXCLUDED.img_src,
                    ppg = EXCLUDED.ppg,
                    rpg = EXCLUDED.rpg,
                    apg = EXCLUDED.apg,
                    pie = EXCLUDED.pie;
            """

            for row in data:
                logging.info(f"Inserting/Updating row: {row}")
                try:
                    rds_cursor.execute(insert_query, row)
                except psycopg2.Error as e:
                    logging.error(f"Error executing query: {e}")
                    logging.error(f"Query: {insert_query}")
                    logging.error(f"Data: {row}")

            rds_conn.commit()
            rds_cursor.close()
            rds_conn.close()
            logging.info("AWS RDS database updated successfully.")
        except Exception as e:
            logging.error(f"Error updating AWS RDS database: {e}")

if __name__ == "__main__":
    # Local PostgreSQL database configuration
    local_db_config = {
        'dbname': os.getenv('POSTGRES_DATABASE'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('POSTGRES_HOST'),
        'port': '5432'
    }

    # AWS RDS PostgreSQL database configuration
    rds_db_config = {
        'dbname': os.getenv('RDS_DB_NAME'),
        'user': os.getenv('RDS_USERNAME'),
        'password': os.getenv('RDS_PASSWORD'),
        'host': os.getenv('RDS_HOST'),
        'port': '5432'
    }

    # Set up logging
    logging.basicConfig(level=logging.DEBUG)

    # Initialize the updater
    updater = UpdateAWSRDS(local_db_config, rds_db_config)

    # Fetch data from local database
    logging.info("Fetching data from local database...")
    local_data = updater.fetch_local_data()

    # Update AWS RDS database with the fetched data
    logging.info("Updating AWS RDS database...")
    updater.update_rds_data(local_data)
