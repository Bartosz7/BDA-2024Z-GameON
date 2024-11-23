SELECT 
  "inning" AS column_name, COUNT(*) AS total_rows, COUNT(inning) AS non_null_count, COUNT(*) - COUNT(inning) AS null_count
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "over", COUNT(*), COUNT("over"), COUNT(*) - COUNT("over")
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "batter", COUNT(*), COUNT(batter), COUNT(*) - COUNT(batter)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "bowler", COUNT(*), COUNT(bowler), COUNT(*) - COUNT(bowler)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "non_striker", COUNT(*), COUNT(non_striker), COUNT(*) - COUNT(non_striker)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "runs_batter", COUNT(*), COUNT(runs_batter), COUNT(*) - COUNT(runs_batter)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "runs_extras", COUNT(*), COUNT(runs_extras), COUNT(*) - COUNT(runs_extras)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "runs_total", COUNT(*), COUNT(runs_total), COUNT(*) - COUNT(runs_total)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "cumulative_score", COUNT(*), COUNT(cumulative_score), COUNT(*) - COUNT(cumulative_score)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "cumulative_wickets", COUNT(*), COUNT(cumulative_wickets), COUNT(*) - COUNT(cumulative_wickets)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "winner", COUNT(*), COUNT(winner), COUNT(*) - COUNT(winner)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "toss_decision", COUNT(*), COUNT(toss_decision), COUNT(*) - COUNT(toss_decision)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "toss_winner", COUNT(*), COUNT(toss_winner), COUNT(*) - COUNT(toss_winner)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "season", COUNT(*), COUNT(season), COUNT(*) - COUNT(season)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "current_team", COUNT(*), COUNT(current_team), COUNT(*) - COUNT(current_team)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "first_inning_total_score", COUNT(*), COUNT(first_inning_total_score), COUNT(*) - COUNT(first_inning_total_score)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "first_inning_total_wickets", COUNT(*), COUNT(first_inning_total_wickets), COUNT(*) - COUNT(first_inning_total_wickets)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "first_inning_run_rate", COUNT(*), COUNT(first_inning_run_rate), COUNT(*) - COUNT(first_inning_run_rate)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_1", COUNT(*), COUNT(team_1), COUNT(*) - COUNT(team_1)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_2", COUNT(*), COUNT(team_2), COUNT(*) - COUNT(team_2)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_1_player_1", COUNT(*), COUNT(team_1_player_1), COUNT(*) - COUNT(team_1_player_1)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_1_player_2", COUNT(*), COUNT(team_1_player_2), COUNT(*) - COUNT(team_1_player_2)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_1_player_3", COUNT(*), COUNT(team_1_player_3), COUNT(*) - COUNT(team_1_player_3)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_1_player_4", COUNT(*), COUNT(team_1_player_4), COUNT(*) - COUNT(team_1_player_4)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_1_player_5", COUNT(*), COUNT(team_1_player_5), COUNT(*) - COUNT(team_1_player_5)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_1_player_6", COUNT(*), COUNT(team_1_player_6), COUNT(*) - COUNT(team_1_player_6)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_1_player_7", COUNT(*), COUNT(team_1_player_7), COUNT(*) - COUNT(team_1_player_7)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_1_player_8", COUNT(*), COUNT(team_1_player_8), COUNT(*) - COUNT(team_1_player_8)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_1_player_9", COUNT(*), COUNT(team_1_player_9), COUNT(*) - COUNT(team_1_player_9)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_1_player_10", COUNT(*), COUNT(team_1_player_10), COUNT(*) - COUNT(team_1_player_10)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_1_player_11", COUNT(*), COUNT(team_1_player_11), COUNT(*) - COUNT(team_1_player_11)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_2_player_1", COUNT(*), COUNT(team_2_player_1), COUNT(*) - COUNT(team_2_player_1)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_2_player_2", COUNT(*), COUNT(team_2_player_2), COUNT(*) - COUNT(team_2_player_2)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_2_player_3", COUNT(*), COUNT(team_2_player_3), COUNT(*) - COUNT(team_2_player_3)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_2_player_4", COUNT(*), COUNT(team_2_player_4), COUNT(*) - COUNT(team_2_player_4)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_2_player_5", COUNT(*), COUNT(team_2_player_5), COUNT(*) - COUNT(team_2_player_5)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_2_player_6", COUNT(*), COUNT(team_2_player_6), COUNT(*) - COUNT(team_2_player_6)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_2_player_7", COUNT(*), COUNT(team_2_player_7), COUNT(*) - COUNT(team_2_player_7)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_2_player_8", COUNT(*), COUNT(team_2_player_8), COUNT(*) - COUNT(team_2_player_8)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_2_player_9", COUNT(*), COUNT(team_2_player_9), COUNT(*) - COUNT(team_2_player_9)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_2_player_10", COUNT(*), COUNT(team_2_player_10), COUNT(*) - COUNT(team_2_player_10)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "team_2_player_11", COUNT(*), COUNT(team_2_player_11), COUNT(*) - COUNT(team_2_player_11)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "run_rate", COUNT(*), COUNT(run_rate), COUNT(*) - COUNT(run_rate)
FROM `bda-gameon-demo.cricket.historic_data`
UNION ALL
SELECT 
  "required_run_rate", COUNT(*), COUNT(required_run_rate), COUNT(*) - COUNT(required_run_rate)
FROM `bda-gameon-demo.cricket.historic_data`
