# NBA Player Profile Scraper

This project is a web scraper that retrieves and stores NBA player profiles, including statistical data, into a MySQL database. It uses Playwright for web scraping and BeautifulSoup for HTML parsing.

## Project Structure

- **`nba_scrape.py`**: Contains the `FetchNBA_Names_HREF` class for fetching NBA player data.
- **`PlayerProfileScraper.py`**: Contains the `PlayerProfileScraper` class for scraping detailed player profiles.
- **`push_profile_to_mysql.py`**: Contains the `PushProfileToMySQL` class for inserting player profile data into MySQL.
- **`Check_or_Create_DB.py`**: Contains the `Check_or_Create_DB` class for creating the database and table if they do not exist.
- **`main.py`**: Orchestrates the workflow by using the above classes to fetch, scrape, and insert data.

## Setup

### Dependencies

1. **Python**: Ensure you have Python 3.x installed.
2. **Install Required Packages**:
    ```bash
    pip install mysql-connector-python playwright beautifulsoup4
    playwright install
    ```

### Environment Variables

Set up the following environment variables with your MySQL configuration:

- `MYSQL_HOST`
- `MYSQL_USER`
- `MYSQL_PASSWORD`
- `MYSQL_DATABASE`

You can set these in your terminal session or include them in a `.env` file (and use a library like `python-dotenv` to load them).

### Database Setup

The `Check_or_Create_DB` class will create the database and table if they do not exist. It assumes that the environment variables are properly set.

## Usage

1. **Run the Main Script**: This will execute the entire workflow.
    ```bash
    python main.py
    ```

2. **Scripts Overview**:

    - **`nba_scrape.py`**:
        - **`FetchNBA_Names_HREF`**: 
            - `get_all_players_page_source()`: Fetches the page source of all NBA players.
            - `get_player_data(page_source)`: Extracts player data (name, href, image source) from the page source.

    - **`PlayerProfileScraper.py`**:
        - **`PlayerProfileScraper`**:
            - `scrape_player_profiles(player_data_from_nba_scrape)`: Scrapes detailed profile data (PPG, RPG, APG, PIE) for each player.
            - `extract_stat(soup, stat_label)`: Extracts a specific statistical value from the player's profile page.

    - **`push_profile_to_mysql.py`**:
        - **`PushProfileToMySQL`**:
            - `push_profile_data_to_mysql(profile_data_list)`: Inserts player profile data into MySQL. It checks if columns exist and adds them if needed.

    - **`Check_or_Create_DB.py`**:
        - **`Check_or_Create_DB`**:
            - `create_database()`: Creates the database.
            - `create_table()`: Creates the table if it does not exist.

## Logging

The project uses Python's built-in logging module to record events. Logs will be displayed in the console. Adjust the logging level as needed in the script.

## Notes

- Ensure that the `page.wait_for_timeout()` times are appropriate for your network conditions and page load times.
- Be mindful of the website's `robots.txt` and scraping policies. Ensure your usage complies with their terms.

## Troubleshooting

- If you encounter issues with Playwright, ensure the browser binaries are installed correctly (`playwright install`).
- Check your MySQL connection details and make sure the database server is accessible.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

