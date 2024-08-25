import psycopg2
import logging

class Push_Season_data_to_POSTGRES:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def push_season_data_to_nba_player_season_stats_table(self, profile_data_list):
        try:
            with psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            ) as connection:
                with connection.cursor() as cursor:
                    for profile_data in profile_data_list:
                        player_name = profile_data['player_name']
                        player_id = profile_data['player_id']
                        game_data = profile_data['game_data']

                        insert_query = """
                            INSERT INTO nba_player_season_stats (
                                player_id, season_year, games_played, minutes_per_game, points_per_game, 
                                field_goals_made, field_goals_attempted, field_goal_percentage, 
                                three_points_made, three_points_attempted, three_point_percentage, 
                                free_throws_made, free_throws_attempted, free_throw_percentage, 
                                offensive_rebounds, defensive_rebounds, total_rebounds, assists, 
                                turnovers, steals, blocks, personal_fouls, fantasy_points, 
                                double_doubles, triple_doubles, plus_minus
                            )
                            VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                            ON CONFLICT (player_id, season_year) DO UPDATE SET
                                games_played = EXCLUDED.games_played,
                                minutes_per_game = EXCLUDED.minutes_per_game,
                                points_per_game = EXCLUDED.points_per_game,
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
                                turnovers = EXCLUDED.turnovers,
                                steals = EXCLUDED.steals,
                                blocks = EXCLUDED.blocks,
                                personal_fouls = EXCLUDED.personal_fouls,
                                fantasy_points = EXCLUDED.fantasy_points,
                                double_doubles = EXCLUDED.double_doubles,
                                triple_doubles = EXCLUDED.triple_doubles,
                                plus_minus = EXCLUDED.plus_minus
                        """
                        values = (
                            player_id, game_data['season_year'], game_data['games_played'], game_data['minutes_per_game'],
                            game_data['points_per_game'], game_data['field_goals_made'], game_data['field_goals_attempted'], game_data['field_goal_percentage'],
                            game_data['three_points_made'], game_data['three_points_attempted'], game_data['three_point_percentage'],
                            game_data['free_throws_made'], game_data['free_throws_attempted'], game_data['free_throw_percentage'],
                            game_data['offensive_rebounds'], game_data['defensive_rebounds'], game_data['total_rebounds'], game_data['assists'],
                            game_data['turnovers'], game_data['steals'], game_data['blocks'], game_data['personal_fouls'], game_data['fantasy_points'],
                            game_data['double_doubles'], game_data['triple_doubles'], game_data['plus_minus']
                        )

                        cursor.execute(insert_query, values)

                    connection.commit()
                    logging.info(f"Successfully updated season data for {len(profile_data_list)} players in 'nba_player_season_stats'.")

        except (Exception, psycopg2.Error) as error:
            logging.error(f"Error pushing season data: {error}")

# Set up logging
logging.basicConfig(level=logging.INFO)
