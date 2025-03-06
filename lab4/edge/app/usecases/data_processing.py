import logging

from app.entities.agent_data import AgentData
from app.entities.processed_agent_data import ProcessedAgentData

data_points = []
MAX_DATA_POINTS = 5
ANOMALY_THRESHOLD = 0.01


def process_agent_data(
        agent_data: AgentData,
) -> ProcessedAgentData:
    """
    Process agent data and classify the state of the road surface.
    Parameters:
        agent_data (AgentData): Agent data that containing accelerometer, GPS, and timestamp.
    Returns:
        processed_data_batch (ProcessedAgentData): Processed data containing the classified state of the road surface and agent data.
    """
    data_points.append(agent_data.accelerometer)

    if len(data_points) > MAX_DATA_POINTS:
        data_points.pop(0)

    if len(data_points) < 2:
        return ProcessedAgentData(road_state="Not enough data", agent_data=agent_data)

    last_point = data_points[-1]
    prev_points = data_points[:-1]

    is_upper_anomaly = all(
        last_point.z > prev_point.z * (1 + ANOMALY_THRESHOLD) for prev_point in prev_points)
    is_lower_anomaly = all(
        last_point.z < prev_point.z * (1 - ANOMALY_THRESHOLD) for prev_point in prev_points)

    if is_upper_anomaly:
        road_state = "Speeding bump"
    elif is_lower_anomaly:
        road_state = "Pit"
    else:
        road_state = "Even"

    prep = ProcessedAgentData(road_state=road_state, agent_data=agent_data)
    logging.info(prep)
    return prep
