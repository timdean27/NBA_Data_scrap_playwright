from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import os
import json
import logging

class PlayerSeasonDataScraper:
    def __init__(self):
        self.base_url = "https://www.nba.com"
        self.output_dir = "PlayerSeasonData"
        self.json_file_path = os.path.join(self.output_dir, "player_data_from_nba_scrape.json")
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def scrape_player_season_data(self, player_data_from_nba_scrape):
        logging.info("Running scrape_player_season_data method in PlayerSeasonDataScraper class")
        profile_data_list = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)  # Run in headless mode
            try:
                page = browser.new_page()

                for player in player_data_from_nba_scrape:
                    player_name = player.get('name', 'Unknown Player')
                    player_id = player['player_id']
                    player_profile_url = f"https://www.nba.com/stats/player/{player_id}"

                    logging.info(f"Processing player: {player_name}, player_id: {player_id}")

                    try:
                        page.goto(player_profile_url, timeout=60000)  # Increased timeout for page loading

                        # Check if the player is a rookie
                        experience_elements = page.locator('div.PlayerSummary_playerInfo__om2G4 >> p.PlayerSummary_playerInfoValue__JS8_v')
                        experience_texts = [elem.inner_text() for elem in experience_elements.all()]

                        # Filter and check if any of the texts contain 'Rookie'
                        if any('Rookie' in text for text in experience_texts):
                            logging.info(f"Player {player_name} is a rookie. Adding row with zeros.")
                            # Add a row with zeros
                            profile_data_list.append({
                                "player_name": player_name,
                                "player_id": player_id,
                                "game_data": {
                                    "season_year": "0",
                                    "team": "0",
                                    "games_played": "0",
                                    "minutes_per_game": "0",
                                    "points_per_game": "0",
                                    "field_goals_made": "0",
                                    "field_goals_attempted": "0",
                                    "field_goal_percentage": "0",
                                    "three_points_made": "0",
                                    "three_points_attempted": "0",
                                    "three_point_percentage": "0",
                                    "free_throws_made": "0",
                                    "free_throws_attempted": "0",
                                    "free_throw_percentage": "0",
                                    "offensive_rebounds": "0",
                                    "defensive_rebounds": "0",
                                    "total_rebounds": "0",
                                    "assists": "0",
                                    "turnovers": "0",
                                    "steals": "0",
                                    "blocks": "0",
                                    "personal_fouls": "0",
                                    "fantasy_points": "0",
                                    "double_doubles": "0",
                                    "triple_doubles": "0",
                                    "plus_minus": "0"
                                }
                            })
                        else:
                            # Locate the first Crom_body__UYOcU on the page
                            logging.info(f"Locating the first Crom_body__UYOcU table for player_id: {player_id}")
                            page.wait_for_selector('tbody.Crom_body__UYOcU', timeout=10000)
                            first_table_locator = page.locator('tbody.Crom_body__UYOcU').first
                            first_row_locator = first_table_locator.locator('tr').first
                            first_row_html = first_row_locator.inner_html()
                            soup = BeautifulSoup(first_row_html, 'html.parser')

                            # Extract the 26 tds from the row
                            columns = soup.find_all('td')
                            if len(columns) == 26:  # Ensure we have exactly 26 columns
                                row_data = [col.get_text(strip=True) for col in columns]
                                game_data = {
                                    "season_year": row_data[0],
                                    "team": row_data[1],
                                    "games_played": row_data[2],
                                    "minutes_per_game": row_data[3],
                                    "points_per_game": row_data[4],
                                    "field_goals_made": row_data[5],
                                    "field_goals_attempted": row_data[6],
                                    "field_goal_percentage": row_data[7],
                                    "three_points_made": row_data[8],
                                    "three_points_attempted": row_data[9],
                                    "three_point_percentage": row_data[10],
                                    "free_throws_made": row_data[11],
                                    "free_throws_attempted": row_data[12],
                                    "free_throw_percentage": row_data[13],
                                    "offensive_rebounds": row_data[14],
                                    "defensive_rebounds": row_data[15],
                                    "total_rebounds": row_data[16],
                                    "assists": row_data[17],
                                    "turnovers": row_data[18],
                                    "steals": row_data[19],
                                    "blocks": row_data[20],
                                    "personal_fouls": row_data[21],
                                    "fantasy_points": row_data[22],
                                    "double_doubles": row_data[23],
                                    "triple_doubles": row_data[24],
                                    "plus_minus": row_data[25]
                                }
                                profile_data_list.append({
                                    "player_name": player_name,
                                    "player_id": player_id,
                                    "game_data": game_data
                                })
                                logging.info(f"Added data for the first season of player_id: {player_id}")
                            else:
                                logging.warning(f"Unexpected number of columns in first row: {len(columns)} for player_id: {player_id}")

                    except Exception as e:
                        logging.error(f"Error scraping player profile for player_id: {player_id} - {e}")

            finally:
                browser.close()

        # Ensure the "PlayerSeasonData" directory exists
        os.makedirs(self.output_dir, exist_ok=True)

        # Save data to a JSON file
        with open(self.json_file_path, 'w') as json_file:
            json.dump(profile_data_list, json_file, indent=4)

        return profile_data_list


# # Test data
# player_data_from_nba_scrape = [
#     {'name': 'Precious Achiuwa', 'first_name': 'Precious', 'last_name': 'Achiuwa', 'href': '/player/1630173/precious-achiuwa/', 'img_src': 'https://cdn.nba.com/headshots/nba/latest/260x190/1630173.png', 'player_id': '1630173'},
#     {'name': 'Steven Adams', 'first_name': 'Steven', 'last_name': 'Adams', 'href': '/player/203500/steven-adams/', 'img_src': 'https://cdn.nba.com/headshots/nba/latest/260x190/203500.png', 'player_id': '203500'},
#     {'name': 'Bam Adebayo', 'first_name': 'Bam', 'last_name': 'Adebayo', 'href': '/player/1628389/bam-adebayo/', 'img_src': 'https://cdn.nba.com/headshots/nba/latest/260x190/1628389.png', 'player_id': '1628389'},
#     {'name': 'Ochai Agbaji', 'first_name': 'Ochai', 'last_name': 'Agbaji', 'href': '/player/1630534/ochai-agbaji/', 'img_src': 'https://cdn.nba.com/headshots/nba/latest/260x190/1630534.png', 'player_id': '1630534'},
#     {'name': 'Melvin Ajinca', 'first_name': 'Melvin', 'last_name': 'Ajinca', 'href': '/player/1642351/melvin-ajinca/', 'img_src': 'https://cdn.nba.com/headshots/nba/latest/260x190/1642351.png', 'player_id': '1642351'},
#     {'name': 'Santi Aldama', 'first_name': 'Santi', 'last_name': 'Aldama', 'href': '/player/1630583/santi-aldama/', 'img_src': 'https://cdn.nba.com/headshots/nba/latest/260x190/1630583.png', 'player_id': '1630583'},
#     {'name': 'Trey Alexander', 'first_name': 'Trey', 'last_name': 'Alexander', 'href': '/player/1641725/trey-alexander/', 'img_src': 'https://cdn.nba.com/headshots/nba/latest/260x190/1641725.png', 'player_id': '1641725'},
#     {'name': 'Johnny Juzang', 'first_name': 'Johnny', 'last_name': 'Juzang', 'href': '/player/1630548/johnny-juzang/', 'img_src': 'https://cdn.nba.com/headshots/nba/latest/260x190/1630200.png', 'player_id': '1630548'},
#     {'name': 'E.J. Liddell', 'first_name': 'E.J.', 'last_name': 'Liddell', 'href': '/player/1630604/ej-liddell/', 'img_src': 'https://cdn.nba.com/headshots/nba/latest/260x190/1630617.png', 'player_id': '1630604'},
#     {'name': 'Kris Murray', 'first_name': 'Kris', 'last_name': 'Murray', 'href': '/player/1631200/kris-murray/', 'img_src': 'https://cdn.nba.com/headshots/nba/latest/260x190/1630718.png', 'player_id': '1631200'},
#     {'name': 'Jalen Wilson', 'first_name': 'Jalen', 'last_name': 'Wilson', 'href': '/player/1630592/jalen-wilson/', 'img_src': 'https://cdn.nba.com/headshots/nba/latest/260x190/1630830.png', 'player_id': '1630592'}
# ]

# scraper = PlayerSeasonDataScraper()
# scraper.scrape_player_season_data(player_data_from_nba_scrape)
