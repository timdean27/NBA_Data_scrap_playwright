
from playwright.sync_api import sync_playwright 
from bs4 import BeautifulSoup


class FetchNBA_Names_HREF:
    def __init__(self):
        # set initail url
        self.url = "https://www.nba.com/players"

    # Method to get the page source of all players after selecting "All" in the dropdown
    def get_all_players_page_source(self):
        print("Running get_all_players_page_source method in FetchNBA_Names_HREF class")

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=False)  # Launch Chromium browser (headless=False opens the browser window)
            page = browser.new_page()  # Open a new browser page
            page.goto(self.url)  # Navigate to the NBA players page

            try:
                # (2 seconds delay)
                page.wait_for_timeout(2000)

                # Locate the dropdown to select the number of players to display
                dropdown = page.locator("select[title='Page Number Selection Drown Down List']")
                dropdown.select_option("All")  # Select the "All" option to display all players

                # (2 seconds delay)
                page.wait_for_timeout(2000)

                # Get the page source after the dropdown selection
                page_source = page.content()

                print("Successfully fetched page source after selecting 'All'.")
                # print(page_source)
                return page_source  # Return the ALLLLL of the HTML content of the page

            except Exception as error:
                # Handle any errors that occur during the process
                print(f"An error occurred: {error}")
                return None  # Return None if an error occurs

            finally:
                # Close the browser after the process is complete
                browser.close()

    # Method to extract player data from the page source
    def get_player_data(self, page_source, limit_players_for_test):
        print("Running get_player_data method in FetchNBA_Names_HREF class")
        try:
            # Parse the HTML content using BeautifulSoup
            soup = BeautifulSoup(page_source, "html.parser")
            player_list = soup.find("table", class_="players-list")  # Locate the table containing player information

            player_data = []  # Initialize an empty list to store player data

            # Loop through each player row in the table
            for idx, row in enumerate(player_list.find_all("td", class_="primary text RosterRow_primaryCol__1lto4"), 1):
                # Extract player name information
                player_name_container = row.find("div", class_="RosterRow_playerName__G28lg")
                player_first_name = player_name_container.find("p", class_="RosterRow_playerFirstName__NYm50").text
                player_last_name = player_name_container.find_all("p")[1].text
                
                # Extract the player's profile link and image
                player_page_link = row.find("a", class_="Anchor_anchor__cSc3P RosterRow_playerLink__qw1vG")
                player_image = row.find("img", class_="PlayerImage_image__wH_YX PlayerImage_round__bIjPr")

                # Check if all necessary information is available
                if player_name_container and player_first_name and player_page_link and player_image:
                    full_name = f"{player_first_name} {player_last_name}"  # Combine first and last names
                    player_href = player_page_link.get("href")  # Get the player's profile URL
                    player_img_src = player_image.get("src")  # Get the player's image URL

                    # Append the player data to the list
                    player_data.append({"name": full_name, "first_name": player_first_name, "last_name": player_last_name, "href": player_href, "img_src": player_img_src})

                    # Print the collected data for each player
                    print(f"Collected data for Player {idx}: {full_name}, Link: {player_href}")

                # Limit the number of players collected for testing purposes
                if len(player_data) >= limit_players_for_test:
                    break  # Stop collecting data after reaching the limit

            return player_data  # Return the list of player data

        except Exception as error:
            # Handle any errors that occur during the data extraction process
            print(f"Error in get_player_data: {error}")
            return None  # Return None if an error occurs
