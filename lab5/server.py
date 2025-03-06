import asyncio
import json
import time

import pandas
from fastapi import FastAPI, WebSocket

app = FastAPI()

pd = pandas.read_csv('car_cor.csv')
counter = 0


def get_doc():
    global counter
    if counter == len(pd) - 1:
        counter = 0

    res = pd.iloc[counter]
    counter += 1
    return res


@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await websocket.accept()
    while True:
        time.sleep(5)
        cor = get_doc()
        data = {
            "user_id": 1,
            "gps": {
                "latitude": cor.iloc[0],
                "longitude": cor.iloc[1]
            }
        }
        await websocket.send_text(json.dumps(data))


if  __name__ == '__main__':
    asyncio.run(websocket_endpoint(app))