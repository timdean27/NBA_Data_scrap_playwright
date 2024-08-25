from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import logging

class Player_Profile_Scraper:
    def __init__(self):
        self.base_url = "https://www.nba.com"

    def scrape_player_profiles(self, player_data_from_nba_scrape):
        logging.info("Running scrape_player_profiles method in PlayerProfileScraper class")
        profile_data_list = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()

            for player in player_data_from_nba_scrape:
                player_name = player.get('name', 'Unknown Player')
                player_id = player['player_id']
                player_profile_url = f"{self.base_url}/stats/player/{player_id}"

                try:
                    page.goto(player_profile_url)
                    page.wait_for_timeout(2000)

                    # Locate the first row (tr) inside the div with class Crom_body__UYOcU
                    first_row_locator = page.locator('tbody.Crom_body__UYOcU >> tr').first
                    first_row_html = first_row_locator.inner_html()
                    soup = BeautifulSoup(first_row_html, 'html.parser')

                    # Extract the 26 tds from the first row
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
                    else:
                        logging.warning(f"Unexpected number of columns in row: {len(columns)}")

                except Exception as e:
                    logging.error(f"Error scraping player profile: {e}")

            browser.close()

        return profile_data_list

# Set up logging
logging.basicConfig(level=logging.INFO)


