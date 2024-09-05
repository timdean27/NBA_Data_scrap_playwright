import psycopg2
import logging
import os


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
                database=self.database,
                # need port for rds
                # port=5432
            ) as connection:
                with connection.cursor() as cursor:
                    # Iterate over each player's game data
                    # for player_name in player_last_five_games_dict:
                    #     print("player_name", player_name)
                    #     for games in player_last_five_games_dict[player_name]:
                    #         print("games" , games)
                    #         for game_data in player_last_five_games_dict[player_name][games]:
                    #             print("game_data" , player_last_five_games_dict[player_name][games][game_data])
                                # Extract relevant data
                    for player_name, games in player_last_five_games_dict.items():
                        # print(player_name, games)
                        for game_key, game_data in games.items():
                            player_id = game_data.get('player_id')
                            game_date = game_data.get('Game Date')
                            print("gamekey",game_key, game_date)
                            if game_date is None:
                                continue
                                        
                            print(player_id)
                            
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
                            ON CONFLICT (player_id, game_date) 
                            DO UPDATE SET 
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
                                plus_minus = EXCLUDED.plus_minus,
                                date_updated = CURRENT_TIMESTAMP;
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





########## TEST 
# player_last_five_games_dict = {
#     "Precious Achiuwa": {
#         "game1": {
#             "player_id": "1630173",
#             "Game Date": "MAY 19, 2024",
#             "Matchup": "NYK vs. IND",
#             "W/L": "L",
#             "MIN": "28",
#             "PTS": "4",
#             "FGM": "2",
#             "FGA": "7",
#             "FG%": "28.6",
#             "3PM": "0",
#             "3PA": "1",
#             "3P%": "0.0",
#             "FTM": "0",
#             "FTA": "0",
#             "FT%": "0.0",
#             "OREB": "0",
#             "DREB": "0",
#             "REB": "0",
#             "AST": "1",
#             "STL": "0",
#             "BLK": "0",
#             "TOV": "0",
#             "PF": "3",
#             "+/-": "-3"
#         },
#         "game2": {
#             "player_id": "1630173",
#             "Game Date": "MAY 17, 2024",
#             "Matchup": "NYK @ IND",
#             "W/L": "L",
#             "MIN": "26",
#             "PTS": "12",
#             "FGM": "5",
#             "FGA": "8",
#             "FG%": "62.5",
#             "3PM": "0",
#             "3PA": "1",
#             "3P%": "0.0",
#             "FTM": "2",
#             "FTA": "5",
#             "FT%": "40.0",
#             "OREB": "4",
#             "DREB": "4",
#             "REB": "8",
#             "AST": "0",
#             "STL": "2",
#             "BLK": "2",
#             "TOV": "0",
#             "PF": "3",
#             "+/-": "-5"
#         },
#         "game3": {
#             "player_id": "1630173",
#             "Game Date": "MAY 14, 2024",
#             "Matchup": "NYK vs. IND",
#             "W/L": "W",
#             "MIN": "23",
#             "PTS": "4",
#             "FGM": "2",
#             "FGA": "6",
#             "FG%": "33.3",
#             "3PM": "0",
#             "3PA": "1",
#             "3P%": "0.0",
#             "FTM": "0",
#             "FTA": "0",
#             "FT%": "0.0",
#             "OREB": "2",
#             "DREB": "3",
#             "REB": "5",
#             "AST": "2",
#             "STL": "2",
#             "BLK": "2",
#             "TOV": "0",
#             "PF": "4",
#             "+/-": "15"
#         },
#         "game4": {
#             "player_id": "1630173",
#             "Game Date": "MAY 12, 2024",
#             "Matchup": "NYK @ IND",
#             "W/L": "L",
#             "MIN": "24",
#             "PTS": "8",
#             "FGM": "4",
#             "FGA": "7",
#             "FG%": "57.1",
#             "3PM": "0",
#             "3PA": "0",
#             "3P%": "0.0",
#             "FTM": "0",
#             "FTA": "2",
#             "FT%": "0.0",
#             "OREB": "5",
#             "DREB": "1",
#             "REB": "6",
#             "AST": "0",
#             "STL": "0",
#             "BLK": "0",
#             "TOV": "0",
#             "PF": "0",
#             "+/-": "-22"
#         },
#         "game5": {
#             "player_id": "1630173",
#             "Game Date": "MAY 10, 2024",
#             "Matchup": "NYK @ IND",
#             "W/L": "L",
#             "MIN": "22",
#             "PTS": "5",
#             "FGM": "2",
#             "FGA": "3",
#             "FG%": "66.7",
#             "3PM": "0",
#             "3PA": "0",
#             "3P%": "0.0",
#             "FTM": "1",
#             "FTA": "2",
#             "FT%": "50.0",
#             "OREB": "4",
#             "DREB": "2",
#             "REB": "6",
#             "AST": "0",
#             "STL": "0",
#             "BLK": "3",
#             "TOV": "2",
#             "PF": "2",
#             "+/-": "-6"
#         }
#     },
#     "Melvin Ajinca": {
#         "game1": {},
#         "game2": {},
#         "game3": {},
#         "game4": {},
#         "game5": {}
#     },
#     "James Akinjo": {
#         "game1": {},
#         "game2": {},
#         "game3": {},
#         "game4": {},
#         "game5": {}
#     },
#     "Santi Aldama": {
#         "game1": {
#             "player_id": "1630583",
#             "Game Date": "APR 01, 2024",
#             "Matchup": "MEM @ DET",
#             "W/L": "W",
#             "MIN": "23",
#             "PTS": "2",
#             "FGM": "1",
#             "FGA": "3",
#             "FG%": "33.3",
#             "3PM": "0",
#             "3PA": "2",
#             "3P%": "0.0",
#             "FTM": "0",
#             "FTA": "0",
#             "FT%": "0.0",
#             "OREB": "0",
#             "DREB": "3",
#             "REB": "3",
#             "AST": "2",
#             "STL": "0",
#             "BLK": "3",
#             "TOV": "0",
#             "PF": "2",
#             "+/-": "3"
#         },
#         "game2": {
#             "player_id": "1630583",
#             "Game Date": "MAR 27, 2024",
#             "Matchup": "MEM vs. LAL",
#             "W/L": "L",
#             "MIN": "27",
#             "PTS": "6",
#             "FGM": "2",
#             "FGA": "6",
#             "FG%": "33.3",
#             "3PM": "2",
#             "3PA": "6",
#             "3P%": "33.3",
#             "FTM": "0",
#             "FTA": "0",
#             "FT%": "0.0",
#             "OREB": "1",
#             "DREB": "2",
#             "REB": "3",
#             "AST": "1",
#             "STL": "1",
#             "BLK": "0",
#             "TOV": "0",
#             "PF": "2",
#             "+/-": "-15"
#         },
#         "game3": {
#             "player_id": "1630583",
#             "Game Date": "MAR 25, 2024",
#             "Matchup": "MEM @ DEN",
#             "W/L": "L",
#             "MIN": "31",
#             "PTS": "5",
#             "FGM": "2",
#             "FGA": "4",
#             "FG%": "50.0",
#             "3PM": "1",
#             "3PA": "3",
#             "3P%": "33.3",
#             "FTM": "0",
#             "FTA": "0",
#             "FT%": "0.0",
#             "OREB": "2",
#             "DREB": "7",
#             "REB": "9",
#             "AST": "3",
#             "STL": "1",
#             "BLK": "4",
#             "TOV": "1",
#             "PF": "1",
#             "+/-": "-17"
#         },
#         "game4": {
#             "player_id": "1630583",
#             "Game Date": "MAR 22, 2024",
#             "Matchup": "MEM @ SAS",
#             "W/L": "W",
#             "MIN": "34",
#             "PTS": "15",
#             "FGM": "5",
#             "FGA": "10",
#             "FG%": "50.0",
#             "3PM": "3",
#             "3PA": "6",
#             "3P%": "50.0",
#             "FTM": "2",
#             "FTA": "2",
#             "FT%": "100",
#             "OREB": "1",
#             "DREB": "12",
#             "REB": "13",
#             "AST": "3",
#             "STL": "0",
#             "BLK": "1",
#             "TOV": "0",
#             "PF": "0",
#             "+/-": "-6"
#         },
#         "game5": {
#             "player_id": "1630583",
#             "Game Date": "MAR 20, 2024",
#             "Matchup": "MEM @ GSW",
#             "W/L": "L",
#             "MIN": "34",
#             "PTS": "27",
#             "FGM": "9",
#             "FGA": "18",
#             "FG%": "50.0",
#             "3PM": "6",
#             "3PA": "12",
#             "3P%": "50.0",
#             "FTM": "3",
#             "FTA": "4",
#             "FT%": "75.0",
#             "OREB": "3",
#             "DREB": "6",
#             "REB": "9",
#             "AST": "4",
#             "STL": "2",
#             "BLK": "1",
#             "TOV": "2",
#             "PF": "0",
#             "+/-": "-23"
#         }
#     }
# }



# test_class = PushLast5GamesDataToPostgres(
#     host = os.environ.get('POSTGRES_HOST'),
#     user = os.environ.get('POSTGRES_USER'),
#     password = os.environ.get('POSTGRES_PASSWORD'),
#     database = os.environ.get('POSTGRES_DATABASE')
# )
# test_fucntion = test_class.push_last_five_games_to_db(player_last_five_games_dict)
# print(test_fucntion)