import psycopg2
import logging

class PushLast5GamesDataToPostgres:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def push_last_five_games_to_db(self, player_last_five_games_dict):
        try:
            # Check if player_last_five_games_dict is a dictionary
            if not isinstance(player_last_five_games_dict, dict):
                raise TypeError("Expected player_last_five_games_dict to be a dictionary.")

            # Connect to PostgreSQL database
            with psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            ) as connection:
                with connection.cursor() as cursor:
                    # Iterate over each player's game data
                    for player_id, games in player_last_five_games_dict.items():
                        # Check if games is a list
                        if not isinstance(games, list):
                            logging.error(f"Expected games for player_id {player_id} to be a list.")
                            continue

                        for game_data in games:
                            # Check if game_data is a dictionary
                            if not isinstance(game_data, dict):
                                logging.error(f"Expected game_data for player_id {player_id} to be a dictionary.")
                                continue

                            # Extract relevant data
                            player_id = game_data.get('player_id')
                            game_date = game_data.get('Game Date')
                            matchup = game_data.get('Matchup')
                            win_loss = game_data.get('W/L')
                            minutes = float(game_data.get('MIN', 0)) if game_data.get('MIN') else None
                            points = float(game_data.get('PTS', 0)) if game_data.get('PTS') else None
                            field_goals_made = float(game_data.get('FGM', 0)) if game_data.get('FGM') else None
                            field_goals_attempted = float(game_data.get('FGA', 0)) if game_data.get('FGA') else None
                            field_goal_percentage = float(game_data.get('FG%', 0)) if game_data.get('FG%') else None
                            three_points_made = float(game_data.get('3PM', 0)) if game_data.get('3PM') else None
                            three_points_attempted = float(game_data.get('3PA', 0)) if game_data.get('3PA') else None
                            three_point_percentage = float(game_data.get('3P%', 0)) if game_data.get('3P%') else None
                            free_throws_made = float(game_data.get('FTM', 0)) if game_data.get('FTM') else None
                            free_throws_attempted = float(game_data.get('FTA', 0)) if game_data.get('FTA') else None
                            free_throw_percentage = float(game_data.get('FT%', 0)) if game_data.get('FT%') else None
                            offensive_rebounds = float(game_data.get('OREB', 0)) if game_data.get('OREB') else None
                            defensive_rebounds = float(game_data.get('DREB', 0)) if game_data.get('DREB') else None
                            total_rebounds = float(game_data.get('REB', 0)) if game_data.get('REB') else None
                            assists = float(game_data.get('AST', 0)) if game_data.get('AST') else None
                            steals = float(game_data.get('STL', 0)) if game_data.get('STL') else None
                            blocks = float(game_data.get('BLK', 0)) if game_data.get('BLK') else None
                            turnovers = float(game_data.get('TOV', 0)) if game_data.get('TOV') else None
                            personal_fouls = float(game_data.get('PF', 0)) if game_data.get('PF') else None
                            plus_minus = float(game_data.get('+/-', 0)) if game_data.get('+/-') else None

                            # Construct SQL query
                            insert_query = """
                            INSERT INTO nba_players_last_5_games (
                                player_id, game_date, matchup, win_loss, minutes, points,
                                field_goals_made, field_goals_attempted, field_goal_percentage,
                                three_points_made, three_points_attempted, three_point_percentage,
                                free_throws_made, free_throws_attempted, free_throw_percentage,
                                offensive_rebounds, defensive_rebounds, total_rebounds, assists,
                                steals, blocks, turnovers, personal_fouls, plus_minus
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (player_id, game_date) DO UPDATE SET
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
                            """

                            # Values to insert
                            values = (
                                player_id, game_date, matchup, win_loss, minutes, points,
                                field_goals_made, field_goals_attempted, field_goal_percentage,
                                three_points_made, three_points_attempted, three_point_percentage,
                                free_throws_made, free_throws_attempted, free_throw_percentage,
                                offensive_rebounds, defensive_rebounds, total_rebounds, assists,
                                steals, blocks, turnovers, personal_fouls, plus_minus
                            )

                            # Execute the query
                            cursor.execute(insert_query, values)

                    # Commit the transaction
                    connection.commit()
                    logging.info("Successfully inserted or updated last 5 games data for players.")

        except (Exception, psycopg2.Error) as error:
            logging.error(f"Error inserting last 5 games data: {error}")

# Set up logging
logging.basicConfig(level=logging.DEBUG)
