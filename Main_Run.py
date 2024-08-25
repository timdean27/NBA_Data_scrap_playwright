import os
import logging
from Check_or_Create_DB_Postgres import Check_or_Create_DB
from Collect_Last_Five_Games import Collect_Last_Five_Games_Class
from Collect_Player_Season_Stats import Player_Season_Data_Scraper
from NBA_Scrape import FetchNBA_Names_HREF
from Push_Player_Main_Table import Push_To_nba_players_table
from Push_Player_Season_Stats import Push_Season_data_to_POSTGRES
from Push_Last_Five_Games_ToPostgres import Push_Last_5_games_data_to_POSTGRES

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    # Retrieve PostgreSQL connection details from environment variables
    postgres_host = os.environ.get('POSTGRES_HOST')
    postgres_user = os.environ.get('POSTGRES_USER')
    postgres_password = os.environ.get('POSTGRES_PASSWORD')
    postgres_database = os.environ.get('POSTGRES_DATABASE')

    # Check if all required environment variables are set
    if not all([postgres_host, postgres_user, postgres_password, postgres_database]):
        logging.error("Missing one or more environment variables.")
        exit(1)

    try:
        # Initialize the database handler
        logging.info("Initializing database handler...")
        creatDB = Check_or_Create_DB(postgres_host, postgres_user, postgres_password, postgres_database)
        
        # Check and create the database and necessary tables
        logging.info("Checking and creating database and table if needed...")
        creatDB.create_database()
        creatDB.create_tables()

        # Fetch NBA player data
        logging.info("Creating FetchNBA_Names_HREF instance...")
        nba_fetcher = FetchNBA_Names_HREF()
        
        logging.info("Fetching NBA player data...")
        page_source = nba_fetcher.get_all_players_page_source()
        player_data_from_nba_scrape = nba_fetcher.get_player_data(page_source, limit_players_for_test=10)
        logging.info(f"Found {len(player_data_from_nba_scrape)} players.")
        print(player_data_from_nba_scrape)
        # Push player data to the main table
        logging.info("Creating Push_To_nba_players_table instance...")
        main_table_pusher = Push_To_nba_players_table(postgres_host, postgres_user, postgres_password, postgres_database)
        
        logging.info("Pushing player data to the main table...")
        main_table_pusher.push_profile_data_to_nba_players_table(player_data_from_nba_scrape)
        logging.info("Player data successfully pushed to the main table.")

        # Scrape player profiles
        logging.info("Creating Player_Season_Data_Scraper instance...")
        profile_season_data_scraper = Player_Season_Data_Scraper()

        logging.info("Scraping player profiles...")
        profile_season_data = profile_season_data_scraper.scrape_player_seaon_data(player_data_from_nba_scrape)
        logging.info(f"Scraped profiles for {len(profile_season_data)} players.")

        # Push profile data to PostgreSQL
        logging.info("Creating PushProfileToPostgres instance...")
        profile_pusher = Push_Season_data_to_POSTGRES(postgres_host, postgres_user, postgres_password, postgres_database)

        logging.info("Pushing profile data to PostgreSQL...")
        profile_pusher.push_season_data_to_nba_player_season_stats_table(profile_season_data)
        logging.info("Profile data successfully pushed to PostgreSQL.")

        # Scrape last 5 games data
        logging.info("Creating Collect_Last_Five_Games_Class instance...")
        last_five_games_collector = Collect_Last_Five_Games_Class()

        logging.info("Scraping last 5 games for each player...")
        player_last_five_games = last_five_games_collector.scrape_player_last_five_games(profile_season_data)
        logging.info("Last 5 games data successfully scraped.")

        # Push last 5 games data to PostgreSQL
        logging.info("Creating Push_Last_5_games_data_to_POSTGRES instance...")
        last_five_games_pusher = Push_Last_5_games_data_to_POSTGRES(postgres_host, postgres_user, postgres_password, postgres_database)

        logging.info("Pushing last 5 games data to PostgreSQL...")
        last_five_games_pusher.push_last_five_games_to_db(page_source, profile_season_data)
        logging.info("Last 5 games data successfully pushed to PostgreSQL.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
