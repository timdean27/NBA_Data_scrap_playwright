import psycopg2
import logging
import os

def fetch_local_data(local_db_config):
    """Fetch data from the local PostgreSQL database."""
    try:
        # Connect to the local PostgreSQL database
        local_conn = psycopg2.connect(**local_db_config)
        local_cursor = local_conn.cursor()
        
        # Execute query to fetch data
        local_cursor.execute("SELECT id, full_name, first_name, last_name, href, img_src, ppg, rpg, apg, pie FROM nba_players")
        
        # Fetch all rows from the executed query
        data = local_cursor.fetchall()
        
        # Close the cursor and connection
        local_cursor.close()
        local_conn.close()
        
        return data
    except Exception as e:
        logging.error(f"Error fetching data from local database: {e}")
        return []

if __name__ == "__main__":
    # Local PostgreSQL database configuration
    local_db_config = {
        'dbname': os.getenv('POSTGRES_DATABASE'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('POSTGRES_HOST'),
        'port': '5432'
    }

    # Set up logging
    logging.basicConfig(level=logging.DEBUG)

    # Fetch data from local database
    logging.info("Fetching data from local database...")
    local_data = fetch_local_data(local_db_config)
    
    # Print the fetched data
    print("Data fetched from local database:")
    if local_data:
        for row in local_data:
            print(row)
    else:
        print("No data fetched or an error occurred.")
