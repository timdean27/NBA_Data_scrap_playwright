import os
from NBA_Scrape import FetchNBA_Names_HREF
from PlayerProfileScraper import Player_Profile_Scraper
from Push_Profile_to_Mysql import Push_Profile_ToMySQL
from Check_or_Create_DB import Check_or_Create_DB

if __name__ == "__main__":
    # Retrieve environment variables
    mysql_host = os.getenv('MYSQL_HOST')
    mysql_user = os.getenv('MYSQL_USER')
    mysql_password = os.getenv('MYSQL_PASSWORD')
    mysql_database = os.getenv('MYSQL_DATABASE')

    if not all([mysql_host, mysql_user, mysql_password, mysql_database]):
        print("Missing one or more environment variables.")
        exit(1)

    try:
        # Initialize database handler
        creatDB = Check_or_Create_DB(mysql_host, mysql_user, mysql_password, mysql_database)
        
        # Check existence of DB and tables and create if needed
        print("Checking and creating database and table if needed...")
        creatDB.create_database()
        creatDB.create_table()

        # Create an instance of the FetchNBA_Names_HREF class
        print("Creating FetchNBA_Names_HREF instance...")
        nba_fetcher = FetchNBA_Names_HREF()

        # Fetch player data
        print("Fetching NBA player data...")
        page_source = nba_fetcher.get_all_players_page_source()
        player_data_from_nba_scrape = nba_fetcher.get_player_data(page_source ,limit_players_for_test=10)
        print(f"Found {len(player_data_from_nba_scrape)} players.")

        # Create an instance of the PlayerProfileScraper class
        print("Creating PlayerProfileScraper instance...")
        profile_scraper = Player_Profile_Scraper()

        # Scrape player profiles using the obtained player data
        print("Scraping player profiles...")
        profile_data = profile_scraper.scrape_player_profiles(player_data_from_nba_scrape)
        print(f"Scraped profiles for {len(profile_data)} players.")

        # Create an instance of the PushProfileToMySQL class
        print("Creating PushProfileToMySQL instance...")
        profile_pusher = Push_Profile_ToMySQL(mysql_host, mysql_user, mysql_password, mysql_database)

        # Push profile data to MySQL
        print("Pushing profile data to MySQL...")
        profile_pusher.push_profile_data_to_mysql(profile_data)
        print("Profile data successfully pushed to MySQL.")

    except Exception as e:
        print(f"An error occurred: {e}")
