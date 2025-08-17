"""
json_to_bbb.py

A script to convert JSON files of ball by ball data (from Cricsheets) into a ball by ball DataFrame.
""" 

import json
import pandas as pd
from typing import Any, Dict, List, Tuple
from os import path, listdir
from tqdm.auto import tqdm

def load_match_data(filepath: str) -> Dict[str, Any]:
    """Load match data from a JSON file. Return {} if parsing fails."""
    try:
        with open(filepath, encoding='utf-8', errors='replace') as f:
            return json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError):
        print(f"Error loading JSON data from {filepath}")
        return {}


def get_match_metadata(data: Dict[str, Any], match_id: str) -> Dict[str, Any]:
    """Extract basic match-level metadata."""
    return {
        "match_id": match_id,
        "match_date": data["info"]["dates"][0],
        "venue": data["info"].get("venue", None),
        "dls": data["info"].get("outcome", {}).get("method") == "D/L",
        "gender": data["info"]["gender"],
        "registry": data["info"]["registry"]["people"],
    }


def process_ball(ball: Dict[str, Any], over_number: int, ball_number: int, ball_id: int,
    innings_number: int, bat_team: str, bowl_team: str, registry: Dict[str, str], match_meta: Dict[str, Any]
) -> Dict[str, Any]:
    """Convert a single ball entry into a flat dictionary row."""
    wicket_type, player_out, extra_type = None, None, None

    if "wickets" in ball:
        wicket = ball["wickets"][0]
        wicket_type = wicket.get("kind")
        player_out = registry.get(wicket.get("player_out"))

    if "extras" in ball:
        extra_type = list(ball["extras"].keys())[0]

    return {
        "ball_id": ball_id,
        **match_meta,
        "innings": innings_number + 1,
        "bat_team": bat_team,
        "bowl_team": bowl_team,
        "over": over_number,
        "ball": ball_number,
        "batter": registry[ball["batter"]],
        "batter_name": ball["batter"],
        "non_striker": registry[ball["non_striker"]],
        "non_striker_name": ball["non_striker"],
        "bowler": registry[ball["bowler"]],
        "bowler_name": ball["bowler"],
        "ball_runs_batter": ball["runs"]["batter"],
        "ball_runs_extras": ball["runs"]["extras"],
        "ball_extras_type": extra_type,
        "ball_runs_total": ball["runs"]["total"],
        "ball_wicket_type": wicket_type,
        "ball_player_out": player_out,
    }


def process_innings(innings_number: int, innings: Dict[str, Any], match_meta: Dict[str, Any], 
    registry: Dict[str, str], ball_id: int, teams: List[str]
) -> Tuple[List[Dict[str, Any]], int]:
    """Process an innings into rows of ball-by-ball data + updated ball_id."""
    bat_team = innings["team"]
    bowl_team = teams[1] if bat_team == teams[0] else teams[0]

    rows = []
    for over in innings["overs"]:
        over_number = over["over"]
        ball_number = 1

        for ball in over["deliveries"]:
            row = process_ball(ball, over_number, ball_number, ball_id,
                               innings_number, bat_team, bowl_team,
                               registry, match_meta)
            rows.append(row)

            ball_id += 1
            ball_number += 1

            if "extras" in ball and any(k in ball["extras"] for k in ("wides", "noballs")):
                ball_number -= 1

    return rows, ball_id


def process_match(match_data: Dict[str, Any], ball_id: int, match_id: str) -> Tuple[pd.DataFrame, ]:
    """Process a full match into a DataFrame of ball-by-ball rows."""
    match_meta = get_match_metadata(match_data, match_id)
    registry = match_meta.pop("registry")
    teams = match_data["info"]["teams"]

    rows: List[Dict[str, Any]] = []
    for innings_number, innings in enumerate(match_data["innings"]):
        innings_rows, ball_id = process_innings(innings_number, innings, match_meta, registry, ball_id, teams)
        rows.extend(innings_rows)

    return pd.DataFrame(rows), ball_id

def process_all_matches(filenames: List[str], folder: str) -> pd.DataFrame:
    dfs = []
    ball_id = 1

    for filename in tqdm(filenames, desc="Processing matches"):
        match_data = load_match_data(path.join(folder, filename))
        if not match_data:
            continue

        match_id = filename.split(".")[0]

        new_df, ball_id = process_match(match_data, ball_id, match_id)
        dfs.append(new_df)

    out = pd.concat(dfs)
    return out


def save_df(df: pd.DataFrame, filename: str):
    """Save DataFrame to a CSV file."""
    df['match_date'] = pd.to_datetime(df['match_date'])
    df.sort_values(by=['match_date', 'match_id', 'innings', 'over', 'ball_id'], inplace=True)
    df.set_index(['ball_id'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    df.to_csv(filename, index=True)


def main():
    folder = "odis_json"
    df = process_all_matches(folder=folder, filenames=listdir(folder))

    save_df(df, "ball_by_ball_data.csv")


if __name__ == "__main__":
    main()