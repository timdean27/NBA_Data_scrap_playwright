import psycopg2
from psycopg2 import sql
import logging

class CheckOrCreateDB:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def create_database_if_not_exists(self):
        connection = None
        try:
            connection = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                # need database and port for rds
                # database=self.database,
                # port=5432
            )
            connection.autocommit = True

            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s"), [self.database])
                if cursor.fetchone() is None:
                    cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                        sql.Identifier(self.database)))
                    logging.info(f"Database '{self.database}' created successfully.")
                else:
                    logging.info(f"Database '{self.database}' already exists.")
                    
        except (Exception, psycopg2.Error) as error:
            logging.error(f"Error creating database: {error}")

        finally:
            if connection:
                connection.close()

    def table_exists(self, cursor, table_name):
        query = sql.SQL("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s);")
        cursor.execute(query, [table_name])
        return cursor.fetchone()[0]

    def create_tables(self):
        try:
            with psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            ) as connection:
                with connection.cursor() as cursor:
                    # Check and create nba_players table
                    if not self.table_exists(cursor, 'nba_players'):
                        create_players_table_query = """
                            CREATE TABLE nba_players (
                                id SERIAL PRIMARY KEY,
                                full_name VARCHAR(255) NOT NULL,
                                first_name VARCHAR(255) NOT NULL,
                                last_name VARCHAR(255) NOT NULL,
                                href VARCHAR(255) UNIQUE NOT NULL,
                                img_src VARCHAR(255) NOT NULL,
                                player_id VARCHAR(255) UNIQUE NOT NULL
                            )
                        """
                        logging.info("Creating table: nba_players")
                        cursor.execute(create_players_table_query)
                    else:
                        logging.info("Table 'nba_players' already exists.")

                    # Check and create nba_player_season_stats table
                    if not self.table_exists(cursor, 'nba_player_season_stats'):
                        create_season_stats_table_query = """
                            CREATE TABLE nba_player_season_stats (
                                id SERIAL PRIMARY KEY,
                                player_id VARCHAR(255) REFERENCES nba_players(player_id) ON DELETE CASCADE,
                                season_year VARCHAR(50) NOT NULL,
                                team VARCHAR(50) NOT NULL,
                                games_played FLOAT NOT NULL,
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
                        logging.info("Creating table: nba_player_season_stats")
                        cursor.execute(create_season_stats_table_query)
                    else:
                        logging.info("Table 'nba_player_season_stats' already exists.")

                    # Check and create nba_players_last_5_games table
                    if not self.table_exists(cursor, 'nba_players_last_5_games'):
                        create_last_5_games_table_query = """
                            CREATE TABLE nba_players_last_5_games (
                                id SERIAL PRIMARY KEY,
                                player_id VARCHAR(255) REFERENCES nba_players(player_id) ON DELETE CASCADE,
                                game_date DATE,
                                matchup VARCHAR(100),
                                win_loss CHAR(1),
                                minutes FLOAT,
                                points FLOAT,
                                field_goals_made FLOAT,
                                field_goals_attempted FLOAT,
                                field_goal_percentage FLOAT,
                                three_points_made FLOAT,
                                three_points_attempted FLOAT,
                                three_point_percentage FLOAT,
                                free_throws_made FLOAT,
                                free_throws_attempted FLOAT,
                                free_throw_percentage FLOAT,
                                offensive_rebounds FLOAT,
                                defensive_rebounds FLOAT,
                                total_rebounds FLOAT,
                                assists FLOAT,
                                steals FLOAT,
                                blocks FLOAT,
                                turnovers FLOAT,
                                personal_fouls FLOAT,
                                plus_minus FLOAT,
                                date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                date_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                CONSTRAINT unique_player_game UNIQUE (player_id, game_date)
                            );

                        """
                        logging.info("Creating table: nba_players_last_5_games")
                        cursor.execute(create_last_5_games_table_query)
                    else:
                        logging.info("Table 'nba_players_last_5_games' already exists.")

                    # Create triggers and functions to automatically update the `date_updated` column if they don't exist
                    create_trigger_function_query = """
                        CREATE OR REPLACE FUNCTION update_date_updated()
                        RETURNS TRIGGER AS $$
                        BEGIN
                            NEW.date_updated = CURRENT_TIMESTAMP;
                            RETURN NEW;
                        END;
                        $$ LANGUAGE plpgsql;
                    """
                    logging.info("Creating function: update_date_updated")
                    cursor.execute(create_trigger_function_query)

                    create_trigger_query_season_stats = """
                        DO $$
                        BEGIN
                            IF NOT EXISTS (
                                SELECT 1 FROM pg_trigger WHERE tgname = 'update_date_updated_trigger_season_stats'
                            ) THEN
                                CREATE TRIGGER update_date_updated_trigger_season_stats
                                BEFORE UPDATE ON nba_player_season_stats
                                FOR EACH ROW
                                EXECUTE FUNCTION update_date_updated();
                            END IF;
                        END
                        $$;
                    """
                    logging.info("Creating trigger: update_date_updated_trigger_season_stats")
                    cursor.execute(create_trigger_query_season_stats)

                    create_trigger_query_last_5_games = """
                        DO $$
                        BEGIN
                            IF NOT EXISTS (
                                SELECT 1 FROM pg_trigger WHERE tgname = 'update_date_updated_trigger_last_5_games'
                            ) THEN
                                CREATE TRIGGER update_date_updated_trigger_last_5_games
                                BEFORE UPDATE ON nba_players_last_5_games
                                FOR EACH ROW
                                EXECUTE FUNCTION update_date_updated();
                            END IF;
                        END
                        $$;
                    """
                    logging.info("Creating trigger: update_date_updated_trigger_last_5_games")
                    cursor.execute(create_trigger_query_last_5_games)

                    connection.commit()
                    logging.info("Tables and triggers created successfully or already exist.")

        except (Exception, psycopg2.Error) as error:
            logging.error(f"Error creating tables: {error}")

