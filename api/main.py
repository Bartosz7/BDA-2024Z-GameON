from fastapi import FastAPI
import pandas as pd
import numpy as np
import json
from pathlib import Path

app = FastAPI()

# cricket api
@app.get("/cricket/{cricketId}/{inning}/{over}")
def cricket_over(cricketId, inning, over):
    file_path = Path(f"data/cricket_{cricketId}_{inning}_{over}.json")
    if file_path.exists():
        with open(file_path, "r") as f:
            data = json.load(f)
        return data
    
    return "Invalid data provided."


@app.get("/laps/{raceId}/{lap}")
def laps(raceId, lap):
    df = pd.read_csv("data/lap_times.csv")
    df = df[(df["raceId"] == int(raceId)) & (df["lap"] == int(lap))]
    out_dict = {}
    try:
        given_lap = {}
        for _, driver in df.iterrows():
            given_lap[driver.driverId] = {
                "position": driver.position,
                "milliseconds": driver.milliseconds
            }

        out_dict["races"] = {
            f"{raceId}": {
                "laps": {
                    f"{lap}": {
                        "drivers": given_lap
                    }
                }
            }
        }
    except:
        out_dict = "Invalid data provided."

    return out_dict

@app.get("/laps/{raceId}")
def all_laps(raceId):
    df = pd.read_csv("data/lap_times.csv")
    df = df[(df["raceId"] == int(raceId))]
    out_dict = {}
    out_dict["races"] = {
        f"{raceId}": {
            "laps":{}
        }
    }
    try:
        for lap in np.unique(df.lap):
            lap_df = df[(df["lap"] == lap)]
            given_lap = {}
            for _, driver in lap_df.iterrows():
                given_lap[driver.driverId] = {
                    "position": driver.position,
                    "milliseconds": driver.milliseconds
                }

            out_dict["races"][f"{raceId}"]["laps"][f"{lap}"] = {"drivers": given_lap}

    except:
        out_dict = "Invalid data provided."

    return out_dict

@app.get("/races/{raceId}")
def races(raceId):
    df = pd.read_csv("data/races.csv")
    df = df[(df["raceId"] == int(raceId))]

    out_dict = {}
    try:
        out_dict["race"] = {
            f"{raceId}": {
                "name": df["name"].values[0],
                "date": df["date"].values[0]
            }
        }
    except:
        out_dict = "Invalid data provided."

    return out_dict
# football events api
@app.get("/events/{matchId}")
def events(matchId: int, start_time: float = 0, end_time: float = float("inf")):
    df = pd.read_parquet("data/events.parquet")
    df = df.fillna('')

    df = df[
        (df["matchId"] == matchId) &
        (df["eventSec"] >= start_time) &
        (df["eventSec"] <= end_time)
    ]

    out_dict = {}

    try:
        given_events = {}
        for _, event in df.iterrows():
            given_events[event.id] = {
                "subEventName": event.subEventName,
                "tags": event.tags,
                "playerId": event.playerId,
                "positions": {
                    "origin": {
                        "x": event.pos_orig_x,
                        "y": event.pos_orig_y
                    },
                    "destination": {
                        "x": event.pos_dest_x,
                        "y": event.pos_dest_y
                    }
                },
                "matchPeriod": event.matchPeriod,
                "eventSec": event.eventSec,
                "subEventId": event.subEventId,
                "id": event.id,
                "tagsList": event.tagsList,
                "teamId": event.teamId,
                "eventName": event.eventName,
            }

        out_dict["matches"] = {
            f"{matchId}": {
                "events": given_events
            }
        }
    except Exception as e:
        out_dict = {"error": f"Invalid data provided. Error: {str(e)}"}

    return out_dict

# football matches api
@app.get("/matches/{matchId}")
def matches(matchId):
    df = pd.read_csv("data/matches.csv")
    df = df[(df["wyId"] == int(matchId))]

    out_dict = {}
    try:
        out_dict["match"] = {
            f"{matchId}": {
                "roundId": df["roundId"].values[0],
                "gameweek": df["gameweek"].values[0],
                "teamsData": df["teamsData"].values[0],
                "seasonId": df["seasonId"].values[0],
                "dateutc": df["dateutc"].values[0],
                "venue": df["venue"].values[0],
                "date": df["date"].values[0],
                "referees": df["referees"].values[0],
                "competitionId": df["competitionId"].values[0],
                "team1": {
                    "scoreET": df["team1.scoreET"].values[0],
                    "coachId": df["team1.coachId"].values[0],
                    "side": df["team1.side"].values[0],
                    "teamId": df["team1.teamId"].values[0],
                    "score": df["team1.score"].values[0],
                    "scoreP": df["team1.scoreP"].values[0],
                    "hasFormation": df["team1.hasFormation"].values[0],
                    "formation": df["team1.formation"].values[0],
                    "scoreHT": df["team1.scoreHT"].values[0],
                    "formation.bench": df["team1.formation.bench"].values[0],
                    "formation.lineup": df["team1.formation.lineup"].values[0],
                    "formation.substitutions": df["team1.formation.substitutions"].values[0],
                },
                "team2": {
                    "scoreET": df["team2.scoreET"].values[0],
                    "coachId": df["team2.coachId"].values[0],
                    "side": df["team2.side"].values[0],
                    "teamId": df["team2.teamId"].values[0],
                    "score": df["team2.score"].values[0],
                    "scoreP": df["team2.scoreP"].values[0],
                    "hasFormation": df["team2.hasFormation"].values[0],
                    "formation": df["team2.formation"].values[0],
                    "scoreHT": df["team2.scoreHT"].values[0],
                    "formation.bench": df["team2.formation.bench"].values[0],
                    "formation.lineup": df["team2.formation.lineup"].values[0],
                    "formation.substitutions": df["team2.formation.substitutions"].values[0],
                },
                "groupName": df["groupName"].values[0],
            }
        }
    except:
        out_dict = "Invalid data provided."

    return out_dict