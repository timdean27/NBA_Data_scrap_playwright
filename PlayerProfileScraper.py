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
                player_profile_url = f"{self.base_url}{player_href}/profile"

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

# Example usage:
if __name__ == "__main__":
    nba_fetcher = FetchNBA_Names_HREF()
    page_source = nba_fetcher.get_all_players_page_source()
    player_data = nba_fetcher.get_player_data(page_source)

    profile_scraper = PlayerProfileScraper()
    result = profile_scraper.scrape_player_profiles(player_data)

    if result:
        print(f"Scraped profiles for {len(result)} players.")
        for player_profile in result:
            print(player_profile)
    else:
        print("No player profiles scraped.")
