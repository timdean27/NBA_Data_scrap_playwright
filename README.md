
# NBA Player Profile Scraper

The NBA Player Profile Scraper is a tool designed to automatically collect and store detailed data about NBA players. It gathers player information such as statistics and profile images directly from the web and stores it in a database. The collected data can also be used to power a REST API, making it accessible for developers or applications that need live NBA data. This project is fully supported on Amazon RDS (Relational Database Service) for scalable and reliable database management.

## How It Works

This project uses web scraping techniques to gather player data from official NBA sources. The scraper automatically navigates web pages, retrieves player profiles (like their name, stats, and images), and stores this information in a database. This data can then be accessed via a REST API for use in apps, websites, or data analysis tools.

The data collected includes:
- Player names
- Profile pictures
- Points per game (PPG)
- Rebounds per game (RPG)
- Assists per game (APG)
- Player Impact Estimate (PIE), which measures a player's overall contribution

## Why This Project is Useful

The NBA Player Profile Scraper is useful for:
- Sports analysts or fans who need easy access to player data.
- Fantasy sports enthusiasts who want up-to-date player stats for tracking or drafting.
- Developers looking to integrate NBA player stats into applications, websites, or visualizations.
- Building REST APIs to provide NBA data to other systems or users.

## Key Features

- **Automated Data Collection**: Gathers player stats without manual effort.
- **Data Storage**: Saves the collected data in a relational database, making it easy to access for future use.
- **REST API Support**: The data can be integrated into a REST API, allowing developers to easily retrieve player profiles through API requests.
- **Amazon RDS**: The tool is fully compatible with Amazon RDS, providing reliable cloud-based storage and scalability for the database.

## How to Use It

1. **Install**: The tool requires a few simple installations to get started (Python and some additional software).
2. **Run the Tool**: Once set up, running the main program will begin the data collection process, gathering NBA player profiles from the web.
3. **Access the Data**: The collected data is saved in a database, which can be accessed directly or through a REST API.
4. **Cloud Storage**: If using Amazon RDS, the scraper can easily store the data in the cloud for scalable, secure access.

## Behind the Scenes

Here’s a simplified explanation of how the tool works:
- It visits the NBA's website and scans player profiles.
- The tool collects key stats, profile images, and other relevant information.
- This data is then stored in a database, either locally or in the cloud (via Amazon RDS), and can be accessed via a REST API for further use.

### Environment Variables

Set up the following environment variables with your MySQL configuration:

    postgres_host = os.environ.get('POSTGRES_HOST')
    postgres_user = os.environ.get('POSTGRES_USER')
    postgres_password = os.environ.get('POSTGRES_PASSWORD')
    postgres_database = os.environ.get('POSTGRES_DATABASE')

You can set these in your terminal session or include them in a `.env` file (and use a library like `python-dotenv` to load them).

### Database Setup

The `Check_or_Create_DB` class will create the database and table if they do not exist. It assumes that the environment variables are properly set.

## Usage

1. **Run the Main Script**: This will execute the entire workflow.
    ```bash
    python Main_Run.py
    ```

## Troubleshooting

If you experience issues while setting up or running the scraper, check your internet connection, browser setup, or database connection settings. If using Amazon RDS, ensure that the database configuration is correct and that permissions are properly set up.


