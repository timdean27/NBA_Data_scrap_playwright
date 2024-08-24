import psycopg2
import logging
import os

def create_table_and_insert_data(db_config):
    """Create a table and insert test data into the AWS RDS PostgreSQL database."""
    try:
        # Connect to the RDS PostgreSQL database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Create the 'test' table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS public.test (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            value DOUBLE PRECISION NOT NULL
        );
        """
        cursor.execute(create_table_query)
        logging.info("Table 'test' created successfully.")

        # Insert test data
        insert_data_query = """
        INSERT INTO public.test (name, value) VALUES (%s, %s)
        ON CONFLICT (id) DO NOTHING;
        """
        test_data = [
            ('Sample Item 1', 123.45),
            ('Sample Item 2', 678.90)
        ]
        
        for data in test_data:
            cursor.execute(insert_data_query, data)
            logging.info(f"Inserted test data: {data}")

        # Commit changes and close connection
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Test data inserted successfully.")

    except psycopg2.Error as e:
        logging.error(f"Database error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    # AWS RDS PostgreSQL database configuration
    db_config = {
        'dbname': os.getenv('RDS_DB_NAME'),
        'user': os.getenv('RDS_USERNAME'),
        'password': os.getenv('RDS_PASSWORD'),
        'host': os.getenv('RDS_HOST'),
        'port': '5432'
    }

    # Set up logging
    logging.basicConfig(level=logging.DEBUG)

    # Create table and insert data
    create_table_and_insert_data(db_config)
