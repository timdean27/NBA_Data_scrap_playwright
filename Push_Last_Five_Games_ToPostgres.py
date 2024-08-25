import psycopg2
from psycopg2 import sql
import logging

class Push_Last_5_games_data_to_POSTGRES:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def get_db_connection(self):
        """Establishes and returns a connection to the PostgreSQL database."""
        return psycopg2.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )

    def push_last_five_games_to_db(self, player_last_five_games_dict):
        """Inserts or updates last 5 games data for players into the database."""
        try:
            with self.get_db_connection() as connection:
                with connection.cursor() as cursor:
                    for player_name, games in player_last_five_games_dict.items():
                        for game_key, game_data in games.items():
                            insert_query = sql.SQL("""
                                INSERT INTO nba_players_last_5_games (
                                    player_name, game_date, matchup, win_loss, minutes, points, field_goals_made, 
                                    field_goals_attempted, field_goal_percentage, three_points_made, 
                                    three_points_attempted, three_point_percentage, free_throws_made, 
                                    free_throws_attempted, free_throw_percentage, offensive_rebounds, 
                                    defensive_rebounds, total_rebounds, assists, steals, blocks, 
                                    turnovers, personal_fouls, plus_minus
                                )
                                VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                )
                                ON CONFLICT (player_name, game_date) DO UPDATE SET
                                    matchup = EXCLUDED.matchup,
                                    win_loss = EXCLUDED.win_loss,
                                    minutes = EXCLUDED.minutes,
                                    points = EXCLUDED.points,
                                    field_goals_made = EXCLUDED.field_goals_made,
                                    field_goals_attempted = EXCLUDED.field_goals_attempted,
                                    field_goal_percentage = EXCLUDED.field_goal_percentage,
                                    three_points_made = EXCLUDED.three_points_made,
                                    three_points_attempted = EXCLUDED.three_points_attempted,
                                    three_point_percentage = EXCLUDED.three_point_percentage,
                                    free_throws_made = EXCLUDED.free_throws_made,
                                    free_throws_attempted = EXCLUDED.free_throws_attempted,
                                    free_throw_percentage = EXCLUDED.free_throw_percentage,
                                    offensive_rebounds = EXCLUDED.offensive_rebounds,
                                    defensive_rebounds = EXCLUDED.defensive_rebounds,
                                    total_rebounds = EXCLUDED.total_rebounds,
                                    assists = EXCLUDED.assists,
                                    steals = EXCLUDED.steals,
                                    blocks = EXCLUDED.blocks,
                                    turnovers = EXCLUDED.turnovers,
                                    personal_fouls = EXCLUDED.personal_fouls,
                                    plus_minus = EXCLUDED.plus_minus
                            """)
                            cursor.execute(insert_query, (
                                player_name,
                                game_data["Game Date"], game_data["Matchup"], game_data["W/L"], game_data["MIN"], game_data["PTS"],
                                game_data["FGM"], game_data["FGA"], game_data["FG%"], game_data["3PM"], game_data["3PA"],
                                game_data["3P%"], game_data["FTM"], game_data["FTA"], game_data["FT%"], game_data["OREB"],
                                game_data["DREB"], game_data["REB"], game_data["AST"], game_data["STL"], game_data["BLK"],
                                game_data["TOV"], game_data["PF"], game_data["+/-"]
                            ))
                    connection.commit()
                    logging.info("Player last five games data inserted or updated successfully.")
        except (Exception, psycopg2.Error) as error:
            logging.error(f"Error inserting last five games data: {error}")
            connection.rollback()
