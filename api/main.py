from fastapi import FastAPI
import pandas as pd
import numpy as np

app = FastAPI()

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

# football api
@app.get("/events/{matchId}/{start_time}/{end_time}")
def events(matchId, start_time, end_time):
    df = pd.read_csv("data/events.csv")
    df = df[(df["matchId"] == int(matchId)) & (df["eventSec"] >= int(start_time)) & (df["eventSec"] <= int(end_time))]
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
    except:
        out_dict = "Invalid data provided."

    return out_dict

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
                    "coachId": df["team1.coachId"].values[0],
                    "side": df["team1.side"].values[0],
                    "teamId": df["team1.teamId"].values[0],
                },
                "team2": {
                    "coachId": df["team2.coachId"].values[0],
                    "side": df["team2.side"].values[0],
                    "teamId": df["team2.teamId"].values[0],
                }
            }
        }
    except:
        out_dict = "Invalid data provided."

    return out_dict