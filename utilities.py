from tqdm import tqdm
import pandas as pd
import numpy as np


def game_stats(dfs):
    """
    Most probably messsed up
    To be used to get game info
    """

    for df in tqdm(dfs, desc="Concatenating Matches: "):
        df['batter_total_runs'] = df.groupby(['innings', 'batter'])['runs_batter'].cumsum()
        df['batter_balls_faced'] = df.apply(
            lambda row: 1 if not ((row['runs_extras'] > 0) and (row['runs_batter'] == 0)) else 0, axis=1)
        df['batter_balls_faced'] = df.groupby(['innings', 'batter'])['batter_balls_faced'].cumsum()

        df['bowler_total_runs'] = df.groupby(['innings', 'bowler'])['runs_total'].cumsum()
        df['bowler_balls_bowled'] = df.apply(
            lambda row: 1 if not ((row['runs_extras'] > 0) and (row['runs_batter'] == 0)) else 0, axis=1)
        df['bowler_balls_bowled'] = df.groupby(['innings', 'bowler'])['bowler_balls_bowled'].cumsum()

        df['team_total_runs'] = df.groupby('innings')['runs_total'].cumsum()
        df['wickets_taken'] = df['player_out'].notna().astype(int)
        df['wickets_taken'] = df.groupby('innings')['wickets_taken'].cumsum()

        df ['rr'] = df['team_total_runs'] / (df['over'] + df['ball'] / 6)
        target = df[df['innings'] == 1]['team_total_runs'].iloc[-1] + 1
        df['target'] = df['innings'].apply(lambda x: target if x == 2 else None)
        
        df['remaining_balls'] = 300 - (df['over'] * 6 + df['ball'])
        df['rrr'] = df['target'] / (df['remaining_balls'] / 6)

    return df

def game_stats_rewritten(dfs):
    """
    This is the rewritten version of the game_stats function.
    """
    for df in tqdm(dfs, desc="Concatenating Matches: "):
        df['batter_total_runs'] = df.groupby(['innings', 'batter'])['runs_batter'].cumsum()

        df['batter_balls_faced'] = df['extras_type'].apply(lambda x: 0 if x in ['wides', 'noballs'] else 1)   
        df['batter_balls_faced'] = df.groupby(['innings', 'batter'])['batter_balls_faced'].cumsum()

        df['bowler_total_runs'] = np.where(df['extras_type'].isin(['byes', 'legbyes']), 0, df['runs_total'])
        df['bowler_total_runs'] = df.groupby(['innings', 'bowler'])['bowler_total_runs'].cumsum()

        df['bowler_balls_bowled'] = df['extras_type'].apply(lambda x: 0 if x in ['wides', 'noballs'] else 1)
        df['bowler_balls_bowled'] = df.groupby(['innings', 'bowler'])['bowler_balls_bowled'].cumsum()

        df['bowler_economy'] = df['bowler_total_runs'] / (df['bowler_balls_bowled'] / 6)

        df['bowler_wickets_taken'] = df['wicket_type'].apply(lambda x: 1 if x not in [None, 'run out', 'retired hurt', 'retired out'] else 0)
        df['bowler_wickets_taken'] = df.groupby(['innings', 'bowler'])['bowler_wickets_taken'].cumsum()

        df['team_total_runs'] = df.groupby('innings')['runs_total'].cumsum()
        df['wickets_taken'] = df['player_out'].notna().astype(int)
        df['wickets_taken'] = df.groupby('innings')['wickets_taken'].cumsum()

        df ['rr'] = df['team_total_runs'] / (df['over'] + df['ball'] / 6)
        target = df[df['innings'] == 1]['team_total_runs'].iloc[-1] + 1
        df['target'] = df['innings'].apply(lambda x: target if x == 2 else None)
        
        df['remaining_balls'] = 120 - (df['over'] * 6 + df['ball'])
        df['rrr'] = df['target'] / (df['remaining_balls'] / 6)

    return df

