from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

class Player_Profile_Scraper:
    def __init__(self):
        self.base_url = "https://www.nba.com"

    def scrape_player_profiles(self, player_data_from_nba_scrape):
        print("Running scrape_player_profiles method in PlayerProfileScraper class")
        profile_data_list = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()

            for player in player_data_from_nba_scrape:
                player_href = player['href']
                player_profile_url = f"{self.base_url}{player_href}profile"

                try:
                    page.goto(player_profile_url)
                    page.wait_for_timeout(2000)

                    profile_page_source = page.content()
                    profile_soup = BeautifulSoup(profile_page_source, "html.parser")

                    points_per_game_value = self.extract_stat(profile_soup, "PPG")
                    rebounds_per_game_value = self.extract_stat(profile_soup, "RPG")
                    assists_per_game_value = self.extract_stat(profile_soup, "APG")
                    pie_value = self.extract_stat(profile_soup, "PIE")

                    player_profile_data = {
                        'name': player['name'],
                        'first_name': player['first_name'],
                        'last_name': player['last_name'],
                        'href': player['href'],
                        'img_src': player['img_src'],
                        'ppg': points_per_game_value,
                        'rpg': rebounds_per_game_value,
                        'apg': assists_per_game_value,
                        'pie': pie_value
                    }

                    print("player_profile_data created", player_profile_data)
                    profile_data_list.append(player_profile_data)

                except Exception as e:
                    print(f"Error scraping player profile: {e}")

            browser.close()

        return profile_data_list

    def extract_stat(self, soup, stat_label):
        stat = soup.find("p", class_="PlayerSummary_playerStatLabel__I3TO3", string=stat_label)
        if stat:
            value = stat.find_next("p", class_="PlayerSummary_playerStatValue___EDg_").text
            return 0.0 if value == '--' else float(value)
        return 0.0


