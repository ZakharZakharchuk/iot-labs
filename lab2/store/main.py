import json
from typing import Set, Dict, List
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from pydantic import BaseModel, field_validator
from config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

# Initialize FastAPI application
app = FastAPI()

# Configure database connection
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()
SessionLocal = sessionmaker(bind=engine)

# Define the database table schema
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

# Define SQLAlchemy model for database interactions
class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    user_id: int
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime

# Define FastAPI models for request validation
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
            raise ValueError("Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ).")

class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData

# WebSocket subscriptions for real-time updates
subscriptions: Dict[int, Set[WebSocket]] = {}

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    """ WebSocket connection handler for live updates """
    await websocket.accept()
    if user_id not in subscriptions:
        subscriptions[user_id] = set()
    subscriptions[user_id].add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions[user_id].remove(websocket)

async def send_data_to_subscribers(user_id: int, data):
    """ Sends real-time data updates to subscribed clients """
    if user_id in subscriptions:
        for websocket in subscriptions[user_id]:
            await websocket.send_json(json.dumps(data))

# 💾 CRUD API

@app.post("/processed_agent_data/")
async def create_processed_agent_data(data: List[ProcessedAgentData]):
    """ Inserts new processed agent data into the database """
    db = SessionLocal()
    mapped_data = [
        {
            "road_state": d.road_state,
            "user_id": d.agent_data.user_id,
            "x": d.agent_data.accelerometer.x,
            "y": d.agent_data.accelerometer.y,
            "z": d.agent_data.accelerometer.z,
            "latitude": d.agent_data.gps.latitude,
            "longitude": d.agent_data.gps.longitude,
            "timestamp": d.agent_data.timestamp
        }
        for d in data
    ]
    db.execute(processed_agent_data.insert(), mapped_data)
    db.commit()
    for d in data:
        await send_data_to_subscribers(d.agent_data.user_id, d)
    return {"status": "success"}

@app.get("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def read_processed_agent_data(processed_agent_data_id: int):
    """ Retrieves a specific processed agent data entry by ID """
    db = SessionLocal()
    query = db.execute(processed_agent_data.select().where(processed_agent_data.c.id == processed_agent_data_id))
    result = query.fetchone()
    if not result:
        raise HTTPException(status_code=404, detail="Data not found")
    return ProcessedAgentDataInDB(**dict(result))

@app.get("/processed_agent_data/", response_model=List[ProcessedAgentDataInDB])
def list_processed_agent_data():
    """ Retrieves all processed agent data entries """
    db = SessionLocal()
    query = db.execute(processed_agent_data.select())
    return [ProcessedAgentDataInDB(**dict(r)) for r in query.fetchall()]

@app.put("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentData):
    """ Updates an existing processed agent data entry """
    db = SessionLocal()
    db.execute(processed_agent_data.update().where(processed_agent_data.c.id == processed_agent_data_id).values(
        road_state=data.road_state,
        user_id=data.agent_data.user_id,
        x=data.agent_data.accelerometer.x,
        y=data.agent_data.accelerometer.y,
        z=data.agent_data.accelerometer.z,
        latitude=data.agent_data.gps.latitude,
        longitude=data.agent_data.gps.longitude,
        timestamp=data.agent_data.timestamp
    ))
    db.commit()
    return read_processed_agent_data(processed_agent_data_id)

@app.delete("/processed_agent_data/{processed_agent_data_id}", response_model=dict)
def delete_processed_agent_data(processed_agent_data_id: int):
    """ Deletes a processed agent data entry by ID """
    db = SessionLocal()
    result = db.execute(processed_agent_data.delete().where(processed_agent_data.c.id == processed_agent_data_id))
    db.commit()
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Data not found")
    return {"status": "deleted"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
