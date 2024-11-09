from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def test():
    return {"Elo": "Helo"}

@app.get("/test")
def test_2():
    return {"test": "test :)"}