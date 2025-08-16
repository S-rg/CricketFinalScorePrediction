# CricketFinalScorePrediction

## Datasets

The main dataset is the match data obtained from [cricksheets](https://cricsheet.org/matches/). Player data such as bowler type and batter hand was obtained from [cricinfo](https://www.espncricinfo.com) using [python-espncricinfo](https://github.com/outside-edge/python-espncricinfo) API wrapper. 

To recreate the setup, download the json files from [cricksheets](https://cricsheet.org/matches/) and place them in `odis_json` folder. You will also need to download the `people.csv` file from [here](https://cricsheet.org/register/) which links the cricsheets identifier to cricinfo id for all players.

Once you have the necessary data, we can run the data extraction and processing pipeline by running the following files.
1. `json_to_bbb`
2. `bbb_to_game_stats`
3. `game_stats_to_team_stats`
4. `team_stats_to_player_stats`

Each file simply creates more infered stats and features, after which we can use the data to train our AI/ML models.