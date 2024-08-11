from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

class FetchNBA_Names_HREF:
    def __init__(self):
        self.url = "https://www.nba.com/players"

    def get_all_players_page_source(self):
        print("Running get_all_players_page_source method in FetchNBA_Names_HREF class")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(self.url)

            try:
                page.wait_for_timeout(2000)

                # Find and interact with the correct dropdown
                dropdown = page.locator("select[title='Page Number Selection Drown Down List']")
                dropdown.select_option("All")

                # Wait for the page to load after selecting "All"
                page.wait_for_timeout(2000)

                # Get the page source after the dropdown selection
                page_source = page.content()

                print("Successfully fetched page source after selecting 'All'.")
                return page_source

            except Exception as e:
                print(f"An error occurred: {e}")
                return None

            finally:
                browser.close()

    def get_player_data(self, page_source):
        print("Running get_player_data method in FetchNBA_Names_HREF class")
        try:
            soup = BeautifulSoup(page_source, "html.parser")
            player_list = soup.find("table", class_="players-list")

            player_data = []
            limit_players_for_test = 10
            for idx, row in enumerate(player_list.find_all("td", class_="primary text RosterRow_primaryCol__1lto4"), 1):
                player_name_container = row.find("div", class_="RosterRow_playerName__G28lg")
                player_first_name = player_name_container.find("p", class_="RosterRow_playerFirstName__NYm50").text
                player_last_name = player_name_container.find_all("p")[1].text
                player_page_link = row.find("a", class_="Anchor_anchor__cSc3P RosterRow_playerLink__qw1vG")
                player_image = row.find("img", class_="PlayerImage_image__wH_YX PlayerImage_round__bIjPr")

                if player_name_container and player_first_name and player_page_link and player_image:
                    full_name = f"{player_first_name} {player_last_name}"
                    player_href = player_page_link.get("href")
                    player_img_src = player_image.get("src")
                    player_data.append({"name": full_name, "first_name": player_first_name, "last_name": player_last_name, "href": player_href, "img_src": player_img_src})

                    print(f"Collected data for Player {idx}: {full_name}, Link: {player_href}")

                if len(player_data) >= limit_players_for_test:
                    break

            return player_data

        except Exception as e:
            print(f"Error in get_player_data: {e}")
            return None

# Example usage:
if __name__ == "__main__":
    nba_fetcher = FetchNBA_Names_HREF()
    page_source = nba_fetcher.get_all_players_page_source()

    if page_source:
        player_data = nba_fetcher.get_player_data(page_source)
        if player_data:
            print(f"Scraped profiles for {len(player_data)} players.")
            for player in player_data:
                print(player)
        else:
            print("No player data fetched.")
    else:
        print("Failed to fetch page source.")
