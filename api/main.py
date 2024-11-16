from fastapi import FastAPI
import pandas as pd

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