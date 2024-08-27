from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import os
import json
import logging

class Collect_Last_Five_Games_Class:
    def __init__(self):
        self.base_url = "https://www.nba.com"
        self.output_dir = "last5GamesData"
        self.json_file_path = os.path.join(self.output_dir, "player_last_five_games.json")

        # Set up logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def scrape_player_last_five_games(self, player_data_from_nba_scrape, profile_season_data):
        logging.info("Running scrape_player_last_five_games method in Collect_Last_Five_Games_Class class")
        player_last_five_games_dict = {}

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            try:
                page = browser.new_page()

                for player in player_data_from_nba_scrape:
                    player_name = player['name']
                    player_href = player['href']
                    player_profile_url = f"{self.base_url}{player_href}/profile"

                    player_last_five_games_dict[player_name] = {f"game{i+1}": {} for i in range(5)}

                    if profile_season_data['games_played'] > 0:
                        try:
                            page.goto(player_profile_url, timeout=40000)
                            page.wait_for_load_state('networkidle')

                            # Locate the table and extract its HTML content
                            table_locator = page.locator('div.MockStatsTable_statsTable__V_Skx >> table')
                            table_html = table_locator.inner_html()

                            # Parse the HTML content with BeautifulSoup
                            soup = BeautifulSoup(table_html, 'html.parser')
                            tbody = soup.find('tbody')

                            if tbody:
                                rows = tbody.find_all('tr')
                                for i, row in enumerate(rows[:5]):  # Get only the last 5 games
                                    columns = row.find_all('td')
                                    if len(columns) == 23:  # Expected number of columns
                                        row_data = [col.get_text(strip=True) for col in columns]
                                        player_last_five_games_dict[player_name][f"game{i+1}"] = {
                                            "Game Date": row_data[0],
                                            "Matchup": row_data[1],
                                            "W/L": row_data[2],
                                            "MIN": row_data[3],
                                            "PTS": row_data[4],
                                            "FGM": row_data[5],
                                            "FGA": row_data[6],
                                            "FG%": row_data[7],
                                            "3PM": row_data[8],
                                            "3PA": row_data[9],
                                            "3P%": row_data[10],
                                            "FTM": row_data[11],
                                            "FTA": row_data[12],
                                            "FT%": row_data[13],
                                            "OREB": row_data[14],
                                            "DREB": row_data[15],
                                            "REB": row_data[16],
                                            "AST": row_data[17],
                                            "STL": row_data[18],
                                            "BLK": row_data[19],
                                            "TOV": row_data[20],
                                            "PF": row_data[21],
                                            "+/-": row_data[22]
                                        }
                                    else:
                                        logging.warning(f"Unexpected number of columns ({len(columns)}) in row for player: {player_name}")
                            else:
                                logging.warning(f"No tbody found in table for player: {player_profile_url}")

                        except Exception as e:
                            logging.error(f"Error scraping player profile for player: {player_name} - {str(e)}")
                            logging.error(f"Player URL: {player_profile_url}")

            finally:
                browser.close()

        # Ensure the "last5GamesData" directory exists
        os.makedirs(self.output_dir, exist_ok=True)

        # Save data to a JSON file
        with open(self.json_file_path, 'w') as json_file:
            json.dump(player_last_five_games_dict, json_file, indent=4)

        return player_last_five_games_dict


# Test data
# # Sample player profile data as a list of dictionaries
# player_data_from_nba_scrape = [
#     {'name': 'Precious Achiuwa', 'first_name': 'Precious', 'last_name': 'Achiuwa', 'href': '/player/1630173/precious-achiuwa/', 'img_src': 'https://cdn.nba.com/headshots/nba/latest/260x190/1630173.png', 'ppg': 7.6, 'rpg': 6.6, 'apg': 1.3, 'pie': 9.5},
#     {'name': 'Steven Adams', 'first_name': 'Steven', 'last_name': 'Adams', 'href': '/player/203500/steven-adams/', 'img_src': 'https://cdn.nba.com/headshots/nba/latest/260x190/203500.png', 'ppg': 8.6, 'rpg': 11.5, 'apg': 2.3, 'pie': 11.2},
#     {'name': 'Bam Adebayo', 'first_name': 'Bam', 'last_name': 'Adebayo', 'href': '/player/1628389/bam-adebayo/', 'img_src': 'https://cdn.nba.com/headshots/nba/latest/260x190/1628389.png', 'ppg': 19.3, 'rpg': 10.4, 'apg': 3.9, 'pie': 15.2},
#     {'name': 'Ochai Agbaji', 'first_name': 'Ochai', 'last_name': 'Agbaji', 'href': '/player/1630534/ochai-agbaji/', 'img_src': 'https://cdn.nba.com/headshots/nba/latest/260x190/1630534.png', 'ppg': 5.8, 'rpg': 2.8, 'apg': 1.1, 'pie': 4.6},
#     {'name': 'Melvin Ajinca', 'first_name': 'Melvin', 'last_name': 'Ajinca', 'href': '/player/1642351/melvin-ajinca/', 'img_src': 'https://cdn.nba.com/headshots/nba/latest/260x190/1642351.png', 'ppg': 0.0, 'rpg': 0.0, 'apg': 0.0, 'pie': 0.0},
#     {'name': 'Santi Aldama', 'first_name': 'Santi', 'last_name': 'Aldama', 'href': '/player/1630583/santi-aldama/', 'img_src': 'https://cdn.nba.com/headshots/nba/latest/260x190/1630583.png', 'ppg': 10.7, 'rpg': 5.8, 'apg': 2.3, 'pie': 10.0},
#     {'name': 'Trey Alexander', 'first_name': 'Trey', 'last_name': 'Alexander', 'href': '/player/1641725/trey-alexander/', 'img_src': 'https://cdn.nba.com/headshots/nba/latest/260x190/1641725.png', 'ppg': 0.0, 'rpg': 0.0, 'apg': 0.0, 'pie': 0.0},
#     {'name': 'Nickeil Alexander-Walker', 'first_name': 'Nickeil', 'last_name': 'Alexander-Walker', 'href': '/player/1629638/nickeil-alexander-walker/', 'img_src': 'https://cdn.nba.com/headshots/nba/latest/260x190/1629638.png', 'ppg': 8.0, 'rpg': 2.0, 'apg': 2.5, 'pie': 7.1},
#     {'name': 'Grayson Allen', 'first_name': 'Grayson', 'last_name': 'Allen', 'href': '/player/1627821/grayson-allen/', 'img_src': 'https://cdn.nba.com/headshots/nba/latest/260x190/1627821.png', 'ppg': 11.3, 'rpg': 3.6, 'apg': 2.8, 'pie': 9.4}
# ]

# # Initialize the class and call the method
# collector = Collect_Last_Five_Games_Class()
# player_last_five_games = collector.scrape_player_last_five_games(player_data_from_nba_scrape)
# print(player_last_five_games)
