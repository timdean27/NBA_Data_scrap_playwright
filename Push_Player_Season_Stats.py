import psycopg2
import logging

class PushSeasonDataToPostgres:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def push_season_data_nba_player_season_stats_table(self, profile_season_data):
        print("profile_season_data IN PUSH PLAYER SEASON STATS", profile_season_data)
        try:
            with psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            ) as connection:
                with connection.cursor() as cursor:
                    for profile_data in profile_season_data:
                        player_id = profile_data['player_id']
                        season_year = profile_data['game_data']['season_year']
                        team = profile_data['game_data']['team']
                        games_played = float(profile_data['game_data']['games_played']) if profile_data['game_data']['games_played'] else None
                        minutes_per_game = float(profile_data['game_data']['minutes_per_game']) if profile_data['game_data']['minutes_per_game'] else None
                        points_per_game = float(profile_data['game_data']['points_per_game']) if profile_data['game_data']['points_per_game'] else None
                        field_goals_made = float(profile_data['game_data']['field_goals_made']) if profile_data['game_data']['field_goals_made'] else None
                        field_goals_attempted = float(profile_data['game_data']['field_goals_attempted']) if profile_data['game_data']['field_goals_attempted'] else None
                        field_goal_percentage = float(profile_data['game_data']['field_goal_percentage']) if profile_data['game_data']['field_goal_percentage'] else None
                        three_points_made = float(profile_data['game_data']['three_points_made']) if profile_data['game_data']['three_points_made'] else None
                        three_points_attempted = float(profile_data['game_data']['three_points_attempted']) if profile_data['game_data']['three_points_attempted'] else None
                        three_point_percentage = float(profile_data['game_data']['three_point_percentage']) if profile_data['game_data']['three_point_percentage'] else None
                        free_throws_made = float(profile_data['game_data']['free_throws_made']) if profile_data['game_data']['free_throws_made'] else None
                        free_throws_attempted = float(profile_data['game_data']['free_throws_attempted']) if profile_data['game_data']['free_throws_attempted'] else None
                        free_throw_percentage = float(profile_data['game_data']['free_throw_percentage']) if profile_data['game_data']['free_throw_percentage'] else None
                        offensive_rebounds = float(profile_data['game_data']['offensive_rebounds']) if profile_data['game_data']['offensive_rebounds'] else None
                        defensive_rebounds = float(profile_data['game_data']['defensive_rebounds']) if profile_data['game_data']['defensive_rebounds'] else None
                        total_rebounds = float(profile_data['game_data']['total_rebounds']) if profile_data['game_data']['total_rebounds'] else None
                        assists = float(profile_data['game_data']['assists']) if profile_data['game_data']['assists'] else None
                        turnovers = float(profile_data['game_data']['turnovers']) if profile_data['game_data']['turnovers'] else None
                        steals = float(profile_data['game_data']['steals']) if profile_data['game_data']['steals'] else None
                        blocks = float(profile_data['game_data']['blocks']) if profile_data['game_data']['blocks'] else None
                        personal_fouls = float(profile_data['game_data']['personal_fouls']) if profile_data['game_data']['personal_fouls'] else None
                        fantasy_points = float(profile_data['game_data']['fantasy_points']) if profile_data['game_data']['fantasy_points'] else None
                        double_doubles = float(profile_data['game_data']['double_doubles']) if profile_data['game_data']['double_doubles'] else None
                        triple_doubles = float(profile_data['game_data']['triple_doubles']) if profile_data['game_data']['triple_doubles'] else None
                        plus_minus = float(profile_data['game_data']['plus_minus']) if profile_data['game_data']['plus_minus'] else None

                        # Construct SQL query
                        insert_query = """
                        INSERT INTO nba_player_season_stats (
                            player_id, season_year, team, games_played, minutes_per_game,
                            points_per_game, field_goals_made, field_goals_attempted,
                            field_goal_percentage, three_points_made, three_points_attempted,
                            three_point_percentage, free_throws_made, free_throws_attempted,
                            free_throw_percentage, offensive_rebounds, defensive_rebounds,
                            total_rebounds, assists, turnovers, steals, blocks,
                            personal_fouls, fantasy_points, double_doubles, triple_doubles,
                            plus_minus
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        
                        # Values to insert
                        values = (
                            player_id, season_year, team, games_played, minutes_per_game,
                            points_per_game, field_goals_made, field_goals_attempted,
                            field_goal_percentage, three_points_made, three_points_attempted,
                            three_point_percentage, free_throws_made, free_throws_attempted,
                            free_throw_percentage, offensive_rebounds, defensive_rebounds,
                            total_rebounds, assists, turnovers, steals, blocks,
                            personal_fouls, fantasy_points, double_doubles, triple_doubles,
                            plus_minus
                        )

                        # Execute the query
                        cursor.execute(insert_query, values)

                    # Commit the transaction
                    connection.commit()
                    logging.info(f"Successfully updated season data for {len(profile_season_data)} players in 'nba_player_season_stats'.")

        except (Exception, psycopg2.Error) as error:
            logging.error(f"Error pushing season data: {error}")

# Set up logging
logging.basicConfig(level=logging.DEBUG)  # Set to DEBUG level to capture detailed logs

# import psycopg2
# import logging

# class PushSeasonDataToPostgres:
#     def __init__(self, host, user, password, database):
#         self.host = host
#         self.user = user
#         self.password = password
#         self.database = database

#     def push_season_data_nba_player_season_stats_table(self, profile_season_data):
#         print("profile_season_data IN PUSH PLAYER SEASON STATS", profile_season_data)
#         try:
#             with psycopg2.connect(
#                 host=self.host,
#                 user=self.user,
#                 password=self.password,
#                 database=self.database
#             ) as connection:
#                 with connection.cursor() as cursor:
#                     for profile_data in profile_season_data:
#                         player_name = profile_data['player_name']
#                         player_id = profile_data['player_id']
#                         season_year = profile_data['game_data']['season_year']
#                         team = profile_data['game_data']['team']
#                         games_played = float(profile_data['game_data']['games_played']) if profile_data['game_data']['games_played'] else None
#                         minutes_per_game = float(profile_data['game_data']['minutes_per_game']) if profile_data['game_data']['minutes_per_game'] else None
#                         points_per_game = float(profile_data['game_data']['points_per_game']) if profile_data['game_data']['points_per_game'] else None
#                         field_goals_made = float(profile_data['game_data']['field_goals_made']) if profile_data['game_data']['field_goals_made'] else None
#                         field_goals_attempted = float(profile_data['game_data']['field_goals_attempted']) if profile_data['game_data']['field_goals_attempted'] else None
#                         field_goal_percentage = float(profile_data['game_data']['field_goal_percentage']) if profile_data['game_data']['field_goal_percentage'] else None
#                         three_points_made = float(profile_data['game_data']['three_points_made']) if profile_data['game_data']['three_points_made'] else None
#                         three_points_attempted = float(profile_data['game_data']['three_points_attempted']) if profile_data['game_data']['three_points_attempted'] else None
#                         three_point_percentage = float(profile_data['game_data']['three_point_percentage']) if profile_data['game_data']['three_point_percentage'] else None
#                         free_throws_made = float(profile_data['game_data']['free_throws_made']) if profile_data['game_data']['free_throws_made'] else None
#                         free_throws_attempted = float(profile_data['game_data']['free_throws_attempted']) if profile_data['game_data']['free_throws_attempted'] else None
#                         free_throw_percentage = float(profile_data['game_data']['free_throw_percentage']) if profile_data['game_data']['free_throw_percentage'] else None
#                         offensive_rebounds = float(profile_data['game_data']['offensive_rebounds']) if profile_data['game_data']['offensive_rebounds'] else None
#                         defensive_rebounds = float(profile_data['game_data']['defensive_rebounds']) if profile_data['game_data']['defensive_rebounds'] else None
#                         total_rebounds = float(profile_data['game_data']['total_rebounds']) if profile_data['game_data']['total_rebounds'] else None
#                         assists = float(profile_data['game_data']['assists']) if profile_data['game_data']['assists'] else None
#                         turnovers = float(profile_data['game_data']['turnovers']) if profile_data['game_data']['turnovers'] else None
#                         steals = float(profile_data['game_data']['steals']) if profile_data['game_data']['steals'] else None
#                         blocks = float(profile_data['game_data']['blocks']) if profile_data['game_data']['blocks'] else None
#                         personal_fouls = float(profile_data['game_data']['personal_fouls']) if profile_data['game_data']['personal_fouls'] else None
#                         fantasy_points = float(profile_data['game_data']['fantasy_points']) if profile_data['game_data']['fantasy_points'] else None
#                         double_doubles = float(profile_data['game_data']['double_doubles']) if profile_data['game_data']['double_doubles'] else None
#                         triple_doubles = float(profile_data['game_data']['triple_doubles']) if profile_data['game_data']['triple_doubles'] else None
#                         plus_minus = float(profile_data['game_data']['plus_minus']) if profile_data['game_data']['plus_minus'] else None

#                         # Print the type of each converted value
#                         print(f"player_name: {type(player_name)}")
#                         print(f"player_id: {type(player_id)}")
#                         print(f"season_year: {type(season_year)}")
#                         print(f"team: {type(team)}")
#                         print(f"games_played: {type(games_played)}")
#                         print(f"minutes_per_game: {type(minutes_per_game)}")
#                         print(f"points_per_game: {type(points_per_game)}")
#                         print(f"field_goals_made: {type(field_goals_made)}")
#                         print(f"field_goals_attempted: {type(field_goals_attempted)}")
#                         print(f"field_goal_percentage: {type(field_goal_percentage)}")
#                         print(f"three_points_made: {type(three_points_made)}")
#                         print(f"three_points_attempted: {type(three_points_attempted)}")
#                         print(f"three_point_percentage: {type(three_point_percentage)}")
#                         print(f"free_throws_made: {type(free_throws_made)}")
#                         print(f"free_throws_attempted: {type(free_throws_attempted)}")
#                         print(f"free_throw_percentage: {type(free_throw_percentage)}")
#                         print(f"offensive_rebounds: {type(offensive_rebounds)}")
#                         print(f"defensive_rebounds: {type(defensive_rebounds)}")
#                         print(f"total_rebounds: {type(total_rebounds)}")
#                         print(f"assists: {type(assists)}")
#                         print(f"turnovers: {type(turnovers)}")
#                         print(f"steals: {type(steals)}")
#                         print(f"blocks: {type(blocks)}")
#                         print(f"personal_fouls: {type(personal_fouls)}")
#                         print(f"fantasy_points: {type(fantasy_points)}")
#                         print(f"double_doubles: {type(double_doubles)}")
#                         print(f"triple_doubles: {type(triple_doubles)}")
#                         print(f"plus_minus: {type(plus_minus)}")

#                     # Remove or comment out the commit and logging lines if you're only testing the type
#                     connection.commit()
#                     logging.info(f"Successfully updated season data for {len(profile_season_data)} players in 'nba_player_season_stats'.")

#         except (Exception, psycopg2.Error) as error:
#             logging.error(f"Error pushing season data: {error}")

# # Set up logging
# logging.basicConfig(level=logging.DEBUG)  # Set to DEBUG level to capture detailed logs
