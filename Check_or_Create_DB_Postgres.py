import psycopg2
from psycopg2 import sql
import logging

class Check_or_Create_DB:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def create_database(self):
        try:
            connection = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password
            )
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                    sql.Identifier(self.database)))
                logging.info(f"Database '{self.database}' created successfully.")

        except (Exception, psycopg2.Error) as error:
            logging.error(f"Error creating database: {error}")

        finally:
            if connection:
                connection.close()

    def create_tables(self):
        try:
            with psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            ) as connection:
                with connection.cursor() as cursor:
                    # Create nba_players table
                    create_players_table_query = """
                        CREATE TABLE IF NOT EXISTS nba_players (
                            id SERIAL PRIMARY KEY,
                            full_name VARCHAR(255) NOT NULL,
                            first_name VARCHAR(255) NOT NULL,
                            last_name VARCHAR(255) NOT NULL,
                            href VARCHAR(255) UNIQUE NOT NULL,
                            img_src VARCHAR(255) NOT NULL,
                            player_id VARCHAR(255) NOT NULL
                        )
                    """
                    cursor.execute(create_players_table_query)

                    # Create nba_player_season_stats table
                    create_season_stats_table_query = """
                        CREATE TABLE IF NOT EXISTS nba_player_season_stats (
                            id SERIAL PRIMARY KEY,
                            player_id INTEGER REFERENCES nba_players(id) ON DELETE CASCADE,
                            season_year INTEGER NOT NULL,
                            games_played INTEGER NOT NULL,
                            minutes_per_game FLOAT NOT NULL,
                            points_per_game FLOAT NOT NULL,
                            field_goals_made FLOAT NOT NULL,
                            field_goals_attempted FLOAT NOT NULL,
                            field_goal_percentage FLOAT NOT NULL,
                            three_points_made FLOAT NOT NULL,
                            three_points_attempted FLOAT NOT NULL,
                            three_point_percentage FLOAT NOT NULL,
                            free_throws_made FLOAT NOT NULL,
                            free_throws_attempted FLOAT NOT NULL,
                            free_throw_percentage FLOAT NOT NULL,
                            offensive_rebounds FLOAT NOT NULL,
                            defensive_rebounds FLOAT NOT NULL,
                            total_rebounds FLOAT NOT NULL,
                            assists FLOAT NOT NULL,
                            turnovers FLOAT NOT NULL,
                            steals FLOAT NOT NULL,
                            blocks FLOAT NOT NULL,
                            personal_fouls FLOAT NOT NULL,
                            fantasy_points FLOAT NOT NULL,
                            double_doubles FLOAT NOT NULL,
                            triple_doubles FLOAT NOT NULL,
                            plus_minus FLOAT NOT NULL,
                            date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            date_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """
                    cursor.execute(create_season_stats_table_query)

                    # Create nba_players_last_5_games table
                    create_last_5_games_table_query = """
                        CREATE TABLE IF NOT EXISTS nba_players_last_5_games (
                            id SERIAL PRIMARY KEY,
                            player_id INTEGER REFERENCES nba_players(id) ON DELETE CASCADE,
                            game_date DATE NOT NULL,
                            matchup VARCHAR(100) NOT NULL,
                            win_loss CHAR(1) NOT NULL,
                            minutes INTEGER NOT NULL,
                            points INTEGER NOT NULL,
                            field_goals_made INTEGER NOT NULL,
                            field_goals_attempted INTEGER NOT NULL,
                            field_goal_percentage FLOAT NOT NULL,
                            three_points_made INTEGER NOT NULL,
                            three_points_attempted INTEGER NOT NULL,
                            three_point_percentage FLOAT NOT NULL,
                            free_throws_made INTEGER NOT NULL,
                            free_throws_attempted INTEGER NOT NULL,
                            free_throw_percentage FLOAT NOT NULL,
                            offensive_rebounds INTEGER NOT NULL,
                            defensive_rebounds INTEGER NOT NULL,
                            total_rebounds INTEGER NOT NULL,
                            assists INTEGER NOT NULL,
                            steals INTEGER NOT NULL,
                            blocks INTEGER NOT NULL,
                            turnovers INTEGER NOT NULL,
                            personal_fouls INTEGER NOT NULL,
                            plus_minus INTEGER NOT NULL,
                            date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            date_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """
                    cursor.execute(create_last_5_games_table_query)

                    # Create triggers and functions to automatically update the `date_updated` column
                    create_trigger_function_query = """
                        CREATE OR REPLACE FUNCTION update_date_updated()
                        RETURNS TRIGGER AS $$
                        BEGIN
                            NEW.date_updated = CURRENT_TIMESTAMP;
                            RETURN NEW;
                        END;
                        $$ LANGUAGE plpgsql;
                    """
                    cursor.execute(create_trigger_function_query)

                    create_trigger_query = """
                        CREATE TRIGGER update_date_updated_trigger
                        BEFORE UPDATE ON nba_player_season_stats
                        FOR EACH ROW
                        EXECUTE FUNCTION update_date_updated();
                    """
                    cursor.execute(create_trigger_query)

                    create_trigger_query = """
                        CREATE TRIGGER update_date_updated_trigger_last_5_games
                        BEFORE UPDATE ON nba_players_last_5_games
                        FOR EACH ROW
                        EXECUTE FUNCTION update_date_updated();
                    """
                    cursor.execute(create_trigger_query)

                    connection.commit()
                    logging.info("Tables 'nba_players', 'nba_player_season_stats', and 'nba_players_last_5_games' created successfully or already exist.")

        except (Exception, psycopg2.Error) as error:
            logging.error(f"Error creating tables: {error}")
