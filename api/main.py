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


@app.get("/laps/positions/{raceId}/{lap}")
def laps(raceId, lap):
    df = pd.read_csv("data/lap_times.csv")
    df = df[(df["raceId"] == int(raceId)) & (df["lap"] == int(lap))]
    out_dict = {}
    try:
        given_lap = {}
        for _, driver in df.iterrows():
            given_lap[driver.driverId] = {
                "position": driver.position,
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

@app.get("/laps/times/{raceId}/{lap}")
def laps(raceId, lap):
    df = pd.read_csv("data/lap_times.csv")
    df = df[(df["raceId"] == int(raceId)) & (df["lap"] == int(lap))]
    out_dict = {}
    try:
        given_lap = {}
        for _, driver in df.iterrows():
            given_lap[driver.driverId] = {
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
        (
            ((df["matchPeriod"] == "2H") & ((df["eventSec"] + 3600) >= start_time) & ((df["eventSec"] + 3600) <= end_time)) |
            ((df["matchPeriod"] != "2H") & (df["eventSec"] >= start_time) & (df["eventSec"] <= end_time))
        )
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
@app.get("/matches")
def matches(matchId: int = None, gameweek: int = None, competitionId: int = None):
    df = pd.read_parquet("data/matches.parquet")
    df = df.fillna('')

    if matchId is not None:
        df = df[df["wyId"] == matchId]
    elif gameweek is not None:
        df = df[df["gameweek"] == gameweek]
        if competitionId is not None:
            df = df[df["competitionId"] == competitionId]

    if df.empty:
        return {"error": "No matches found with the given parameters."}

    out_dict = {"matches": {}}

    try:
        for _, match in df.iterrows():
            match_id = match["wyId"]
            out_dict["matches"][f"{match_id}"] = {
                "roundId": match["roundId"],
                "gameweek": match["gameweek"],
                "teamsData": match["teamsData"],
                "seasonId": match["seasonId"],
                "dateutc": match["dateutc"],
                "venue": match["venue"],
                "date": match["date"],
                "referees": match["referees"],
                "competitionId": match["competitionId"],
                "status": match["status"],
                "winner": match["winner"],
                "duration": match["duration"],
                "label": match["label"],

                "team1": {
                    "scoreET": match["team1.scoreET"],
                    "coachId": match["team1.coachId"],
                    "side": match["team1.side"],
                    "teamId": match["team1.teamId"],
                    "score": match["team1.score"],
                    "scoreP": match["team1.scoreP"],
                    "hasFormation": match["team1.hasFormation"],
                    "formation": match["team1.formation"],
                    "scoreHT": match["team1.scoreHT"],
                    "formation.bench": match["team1.formation.bench"],
                    "formation.lineup": match["team1.formation.lineup"],
                    "formation.substitutions": match["team1.formation.substitutions"],
                },
                "team2": {
                    "scoreET": match["team2.scoreET"],
                    "coachId": match["team2.coachId"],
                    "side": match["team2.side"],
                    "teamId": match["team2.teamId"],
                    "score": match["team2.score"],
                    "scoreP": match["team2.scoreP"],
                    "hasFormation": match["team2.hasFormation"],
                    "formation": match["team2.formation"],
                    "scoreHT": match["team2.scoreHT"],
                    "formation.bench": match["team2.formation.bench"],
                    "formation.lineup": match["team2.formation.lineup"],
                    "formation.substitutions": match["team2.formation.substitutions"],
                }
            }
    except Exception as e:
        return {"error": f"An error occurred while processing the data: {str(e)}"}

    return out_dict