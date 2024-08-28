import os
import logging
from Check_or_Create_DB_Postgres import CheckOrCreateDB
from Collect_Last_Five_Games import Collect_Last_Five_Games_Class
from Collect_Player_Season_Stats import PlayerSeasonDataScraper
from NBA_Scrape import FetchNBA_Names_HREF
from Push_Player_Main_Table import PushToNBAPlayersTable
from Push_Player_Season_Stats import PushSeasonDataToPostgres
from Push_Last_Five_Games_ToPostgres import PushLast5GamesDataToPostgres

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
        creatDB = CheckOrCreateDB(postgres_host, postgres_user, postgres_password, postgres_database)
        
        # Check and create the database and necessary tables
        logging.info("Checking and creating database and table if needed...")
        creatDB.create_database_if_not_exists()
        creatDB.create_tables()

        # Fetch NBA player data
        logging.info("Creating FetchNBA_Names_HREF instance...")
        nba_fetcher = FetchNBA_Names_HREF()
        
        logging.info("Fetching NBA player data...")
        page_source = nba_fetcher.get_all_players_page_source()
        player_data_from_nba_scrape = nba_fetcher.get_player_data(page_source, limit_players_for_test=7)
        logging.info(f"Found {len(player_data_from_nba_scrape)} players.")
        print(player_data_from_nba_scrape)
        # Push player data to the main table
        logging.info("Creating PushToNBAPlayersTable instance...")
        main_table_pusher = PushToNBAPlayersTable(postgres_host, postgres_user, postgres_password, postgres_database)
        
        logging.info("Pushing player data to the main table...")
        main_table_pusher.push_profile_data_to_nba_players_table(player_data_from_nba_scrape)
        logging.info("Player data successfully pushed to the main table.")

        # Scrape player profiles
        logging.info("Creating PlayerSeasonDataScraper instance...")
        profile_season_data_scraper = PlayerSeasonDataScraper()

        logging.info("Scraping player profiles...")
        profile_season_data = profile_season_data_scraper.scrape_player_season_data(player_data_from_nba_scrape)
        logging.info(f"Scraped profiles for {len(profile_season_data)} players.")

        # Push profile data to PostgreSQL
        logging.info("Creating PushProfileToPostgres instance...")
        profile_season_pusher = PushSeasonDataToPostgres(postgres_host, postgres_user, postgres_password, postgres_database)

        logging.info("Pushing profile data to PostgreSQL...")
        profile_season_pusher.push_season_data_nba_player_season_stats_table(profile_season_data)
        logging.info("Profile data successfully pushed to PostgreSQL.")

        # Scrape last 5 games data
        logging.info("Creating Collect_Last_Five_Games_Class instance...")
        last_five_games_collector = Collect_Last_Five_Games_Class()

        logging.info("Scraping last 5 games for each player...")
        player_last_five_games = last_five_games_collector.scrape_player_last_five_games(player_data_from_nba_scrape)
        logging.info("Last 5 games data successfully scraped.")

        # Push last 5 games data to PostgreSQL
        logging.info("Creating PushLast5GamesDataToPostgres instance...")
        last_five_games_pusher = PushLast5GamesDataToPostgres(postgres_host, postgres_user, postgres_password, postgres_database)

        logging.info("Pushing last 5 games data to PostgreSQL...")
        last_five_games_pusher.push_last_five_games_to_db(player_last_five_games)
        logging.info("Last 5 games data successfully pushed to PostgreSQL.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
