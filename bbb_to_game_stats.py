"""
Adds Game Stats to our Base Database.
"""

import numpy as np
import pandas as pd
from tqdm.auto import tqdm
from typing import Dict


# ------------ CHANGE THIS TO THE FORMAT OF YOUR MATCHES ------------- #
format = 'ODI'

format_to_balls_map = {
    'ODI': 300,  # 50 overs
    'T20': 120,  # 20 overs
    'Test': None,
}

inputfile = "data/ball_by_ball_data.csv"
outputfile = "data/game_stats.csv"
# -------------------------------------------------------------------- #

def add_batter_total_runs(df: pd.DataFrame) -> pd.DataFrame:
    df['match_batter_total_runs'] = df.groupby(['innings', 'batter'])['ball_runs_batter'].cumsum()
    return df


def add_batter_balls_faced(df: pd.DataFrame) -> pd.DataFrame:
    df['match_batter_balls_faced'] = df['ball_extras_type'].apply(lambda x: 0 if x == 'wides' else 1)
    df['match_batter_balls_faced'] = df.groupby(['innings', 'batter'])['match_batter_balls_faced'].cumsum()
    return df

def add_bowler_total_runs(df: pd.DataFrame) -> pd.DataFrame:
    df['match_bowler_total_runs'] = np.where(
        df['ball_extras_type'].isin(['byes', 'legbyes']), 0, df['ball_runs_total']
    )
    df['match_bowler_total_runs'] = df.groupby(['innings', 'bowler'])['match_bowler_total_runs'].cumsum()
    return df

def add_bowler_balls_bowled(df: pd.DataFrame) -> pd.DataFrame:
    df['match_bowler_balls_bowled'] = df['ball_extras_type'].apply(lambda x: 0 if x in ['wides','noballs'] else 1)
    df['match_bowler_balls_bowled'] = df.groupby(['innings', 'bowler'])['match_bowler_balls_bowled'].cumsum()
    return df

def add_bowler_economy(df: pd.DataFrame) -> pd.DataFrame:
    df['match_bowler_economy'] = df['match_bowler_total_runs'] / (df['match_bowler_balls_bowled'] / 6)
    return df

def add_bowler_wickets(df: pd.DataFrame) -> pd.DataFrame:
    df['match_bowler_wickets_taken'] = df['ball_wicket_type'].apply(
        lambda x: 1 if x not in [np.nan, None, 'run out', 'retired hurt', 'retired out'] else 0
    )
    df['match_bowler_wickets_taken'] = df.groupby(['innings', 'bowler'])['match_bowler_wickets_taken'].cumsum()
    return df

def add_team_runs(df: pd.DataFrame) -> pd.DataFrame:
    df['match_team_total_runs'] = df.groupby('innings')['ball_runs_total'].cumsum()
    return df

def add_team_wickets(df: pd.DataFrame) -> pd.DataFrame:
    df['match_wickets_taken'] = df['ball_player_out'].notna().astype(int)
    df['match_wickets_taken'] = df.groupby('innings')['match_wickets_taken'].cumsum()
    return df

def add_team_run_rate(df: pd.DataFrame) -> pd.DataFrame:
    df['match_team_rr'] = df['match_team_total_runs'] / (df['over'] + df['ball'] / 6)
    return df

def add_target(df: pd.DataFrame) -> pd.DataFrame:
    target = df[df['innings'] == 1]['match_team_total_runs'].iloc[-1] + 1
    df['target'] = df['innings'].apply(lambda x: target if x == 2 else None)
    return df

def add_remaining_balls_rrr(df: pd.DataFrame, total_balls: int) -> pd.DataFrame:
    df['remaining_balls'] = total_balls - (df['over'] * 6 + df['ball'])
    df['rrr'] = df['target'] / (df['remaining_balls'] / 6)
    return df

def process_match(df: pd.DataFrame, format: str, format_to_balls_map: Dict[str, int]) -> pd.DataFrame:
    df = add_batter_total_runs(df)
    df = add_batter_balls_faced(df)
    df = add_bowler_total_runs(df)
    df = add_bowler_balls_bowled(df)
    df = add_bowler_economy(df)
    df = add_bowler_wickets(df)
    df = add_team_runs(df)
    df = add_team_wickets(df)
    df = add_team_run_rate(df)
    df = add_target(df)
    df = add_remaining_balls_rrr(df, format_to_balls_map[format])
    return df

def process_all_matches(df, format, format_to_balls_map):
    processed_dfs = []
    for _, df_group in tqdm(df.groupby('match_id', group_keys=False), desc="Calculating Match Stats"):
        processed_dfs.append(process_match(df_group, format, format_to_balls_map))
    df = pd.concat(processed_dfs, ignore_index=True)

    df.sort_values(by=['match_date', 'match_id', 'innings', 'over'], inplace=True)
    df = df.reset_index(drop=True)
    df['ball_id'] = df.index
    df.set_index('ball_id', inplace=True)

def main():
    df = pd.read_csv(inputfile)
    df['match_date'] = pd.to_datetime(df['match_date'])
    
    process_all_matches(df, format, format_to_balls_map)

    df.to_csv(outputfile, index=False)


if __name__ == "__main__":
    main()