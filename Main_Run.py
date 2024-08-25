import os
from Check_or_Create_DB_Postgres import Check_or_Create_DB
from Collect_Last_Five_Games import Collect_Last_Five_Games_Class  # For scraping last 5 games data
from Collect_Player_Season_Stats import Player_Profile_Scraper  # For scraping player season stats
from NBA_Scrape import FetchNBA_Names_HREF  # For scraping NBA player names and HREFs
from Push_Player_Main_Table import Push_To_nba_players_table  # For pushing player data to the main table
from Push_Player_Season_Stats import Push_Season_data_to_POSTGRES  # For pushing season stats to the database
from Push_Last_Five_Games_ToPostgres import Push_Last_5_games_data_to_POSTGRES  # For pushing last 5 games data to the database

if __name__ == "__main__":
    # Retrieve PostgreSQL connection details from environment variables
    postgres_host = os.environ.get('POSTGRES_HOST')
    postgres_user = os.environ.get('POSTGRES_USER')
    postgres_password = os.environ.get('POSTGRES_PASSWORD')
    postgres_database = os.environ.get('POSTGRES_DATABASE')

    # Check if all required environment variables are set
    if not all([postgres_host, postgres_user, postgres_password, postgres_database]):
        print("Missing one or more environment variables.")
        exit(1)

    try:
        # Step 1: Initialize the database handler
        print("Initializing database handler...")
        creatDB = Check_or_Create_DB(postgres_host, postgres_user, postgres_password, postgres_database)
        
        # Step 2: Check and create the database and necessary tables
        print("Checking and creating database and table if needed...")
        creatDB.create_database()  # Ensure the database exists
        creatDB.create_tables()  # Ensure the necessary table(s) exist

        # Step 3: Fetch NBA player data
        print("Creating FetchNBA_Names_HREF instance...")
        nba_fetcher = FetchNBA_Names_HREF()
        
        print("Fetching NBA player data...")
        page_source = nba_fetcher.get_all_players_page_source()
        player_data_from_nba_scrape = nba_fetcher.get_player_data(page_source, limit_players_for_test=10)
        print(f"Found {len(player_data_from_nba_scrape)} players.")

        # Step 4: Push player data to the main table
        print("Creating Push_To_nba_players_table instance...")
        main_table_pusher = Push_To_nba_players_table(postgres_host, postgres_user, postgres_password, postgres_database)
        
        print("Pushing player data to the main table...")
        main_table_pusher.push_profile_data_to_nba_players_table(player_data_from_nba_scrape)
        print("Player data successfully pushed to the main table.")

        # Step 5: Scrape player profiles
        print("Creating PlayerProfileScraper instance...")
        profile_scraper = Player_Profile_Scraper()

        print("Scraping player profiles...")
        profile_data = profile_scraper.scrape_player_profiles(player_data_from_nba_scrape)
        print(f"Scraped profiles for {len(profile_data)} players.")

        # Step 6: Push profile data to PostgreSQL
        print("Creating PushProfileToPostgres instance...")
        profile_pusher = Push_Season_data_to_POSTGRES(postgres_host, postgres_user, postgres_password, postgres_database)

        print("Pushing profile data to PostgreSQL...")
        profile_pusher.push_season_data_to_nba_player_season_stats_table(profile_data)
        print("Profile data successfully pushed to PostgreSQL.")

        # Step 7: Scrape last 5 games data
        print("Creating Collect_Last_Five_Games_Class instance...")
        last_five_games_collector = Collect_Last_Five_Games_Class()

        print("Scraping last 5 games for each player...")
        player_last_five_games = last_five_games_collector.scrape_player_last_five_games(profile_data)
        print("Last 5 games data successfully scraped.")

        # Step 8: Push last 5 games data to PostgreSQL
        print("Creating Push_Last_5_games_data_to_POSTGRES instance...")
        last_five_games_pusher = Push_Last_5_games_data_to_POSTGRES(postgres_host, postgres_user, postgres_password, postgres_database)

        print("Pushing last 5 games data to PostgreSQL...")
        last_five_games_pusher.push_last_five_games_to_db(player_last_five_games)
        print("Last 5 games data successfully pushed to PostgreSQL.")

    except Exception as e:
        print(f"An error occurred: {e}")
