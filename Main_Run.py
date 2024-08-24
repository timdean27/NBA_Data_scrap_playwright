import os
from NBA_Scrape import FetchNBA_Names_HREF
from PlayerProfileScraper import Player_Profile_Scraper
from Push_Profile_to_Postgres import Push_Profile_ToPostgres
from Check_or_Create_DB_Postgres import Check_or_Create_DB
from Collect_Last_Five_Games import Collect_Last_Five_Games_Class

if __name__ == "__main__":
    postgres_host = os.environ.get('POSTGRES_HOST')
    postgres_user = os.environ.get('POSTGRES_USER')
    postgres_password = os.environ.get('POSTGRES_PASSWORD')
    postgres_database = os.environ.get('POSTGRES_DATABASE')

    if not all([postgres_host, postgres_user, postgres_password, postgres_database]):
        print("Missing one or more environment variables.")
        exit(1)

    try:
        # Initialize database handler
        creatDB = Check_or_Create_DB(postgres_host, postgres_user, postgres_password, postgres_database)
        
        # Check existence of DB and tables and create if needed
        print("Checking and creating database and table if needed...")
        creatDB.create_database()
        creatDB.create_table()

        # Create an instance of the FetchNBA_Names_HREF class in NBA_Scrape file
        print("Creating FetchNBA_Names_HREF instance...")
        nba_fetcher = FetchNBA_Names_HREF()

        # Fetch player data
        print("Fetching NBA player data...")
        page_source = nba_fetcher.get_all_players_page_source()
        player_data_from_nba_scrape = nba_fetcher.get_player_data(page_source, limit_players_for_test=10)
        print(f"Found {len(player_data_from_nba_scrape)} players.")

        # Create an instance of the PlayerProfileScraper class
        print("Creating PlayerProfileScraper instance...")
        profile_scraper = Player_Profile_Scraper()

        # Scrape player profiles using the obtained player data
        print("Scraping player profiles...")
        profile_data = profile_scraper.scrape_player_profiles(player_data_from_nba_scrape)
        print(f"Scraped profiles for {len(profile_data)} players.")

        # Create an instance of the PushProfileToPostgres class
        print("Creating PushProfileToPostgres instance...")
        profile_pusher = Push_Profile_ToPostgres(postgres_host, postgres_user, postgres_password, postgres_database)

        # Push profile data to PostgreSQL
        print("Pushing profile data to PostgreSQL...")
        profile_pusher.push_profile_data_to_postgres(profile_data)
        print("Profile data successfully pushed to PostgreSQL.")

        # Create an instance of the Collect_Last_Five_Games_Class class
        print("Creating Collect_Last_Five_Games_Class instance...")
        scrape_last_5_games_instance = Collect_Last_Five_Games_Class()

        # Scrape last 5 games from players' profiles
        print("Scraping last 5 games for each player...")
        player_last_five_games = scrape_last_5_games_instance.scrape_player_last_five_games(profile_data)
        print("Last 5 games data successfully scraped.")

    except Exception as e:
        print(f"An error occurred: {e}")
