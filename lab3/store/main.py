import asyncio
import json
from typing import Set, Dict, List, Any
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    DateTime,
)
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from pydantic import BaseModel, field_validator
from config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

# FastAPI app setup
app = FastAPI()

# SQLAlchemy setup
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()

# Define the ProcessedAgentData table
processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("user_id", Integer),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timestamp", DateTime),
)
SessionLocal = sessionmaker(bind=engine)

# FastAPI models
class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float

class GpsData(BaseModel):
    latitude: float
    longitude: float

class AgentData(BaseModel):
    user_id: int
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime

    @classmethod
    @field_validator("timestamp", mode="before")
    def check_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)."
            )

class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData

# WebSocket subscriptions
subscriptions: Dict[int, Set[WebSocket]] = {}

# WebSocket endpoint
@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await websocket.accept()
    if user_id not in subscriptions:
        subscriptions[user_id] = set()
    subscriptions[user_id].add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions[user_id].remove(websocket)

# Function to send data to subscribers
async def send_data_to_subscribers(user_id: int, data):
    if user_id in subscriptions:
        for websocket in subscriptions[user_id]:
            await websocket.send_json(json.dumps(data, default=str))


# CRUD Operations
@app.post("/processed_agent_data/")
async def create_processed_agent_data(data: List[ProcessedAgentData]):
    db = SessionLocal()
    for record in data:
        agent_data = record.agent_data
        db.execute(processed_agent_data.insert().values(
            road_state=record.road_state,
            user_id=agent_data.user_id,
            x=agent_data.accelerometer.x,
            y=agent_data.accelerometer.y,
            z=agent_data.accelerometer.z,
            latitude=agent_data.gps.latitude,
            longitude=agent_data.gps.longitude,
            timestamp=agent_data.timestamp
        ))
        await send_data_to_subscribers(agent_data.user_id, record.dict())
    db.commit()
    return {"status": "Data saved and sent to WebSocket subscribers"}

@app.get("/processed_agent_data/{processed_agent_data_id}")
def read_processed_agent_data(processed_agent_data_id: int):
    db = SessionLocal()
    result = db.execute(
        processed_agent_data.select().where(processed_agent_data.c.id == processed_agent_data_id)
    ).fetchone()
    if result:
        return dict(result._mapping)
    raise HTTPException(status_code=404, detail="Data not found")

@app.get("/processed_agent_data/")
def list_processed_agent_data():
    db = SessionLocal()
    results = db.execute(processed_agent_data.select()).fetchall()
    return [dict(row._mapping) for row in results]  #

@app.put("/processed_agent_data/{processed_agent_data_id}")
def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentData):
    db = SessionLocal()
    db.execute(
        processed_agent_data.update()
        .where(processed_agent_data.c.id == processed_agent_data_id)
        .values(
            road_state=data.road_state,
            timestamp=data.agent_data.timestamp
        )
    )
    db.commit()
    return {"status": "Updated"}

@app.delete("/processed_agent_data/{processed_agent_data_id}")
def delete_processed_agent_data(processed_agent_data_id: int):
    db = SessionLocal()
    db.execute(
        processed_agent_data.delete().where(processed_agent_data.c.id == processed_agent_data_id)
    )
    db.commit()
    return {"status": "Deleted"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
