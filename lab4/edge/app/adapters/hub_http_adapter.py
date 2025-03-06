import logging

import requests
import pydantic_core
import json

from app.entities.processed_agent_data import ProcessedAgentData
from app.interfaces.hub_gateway import HubGateway


class HubHttpAdapter(HubGateway):
    def __init__(self, api_base_url):
        self.api_base_url = api_base_url

    def save_data(self, processed_data: ProcessedAgentData):
        """
        Save the processed road data to the Hub.
        Parameters:
            processed_data (ProcessedAgentData): Processed road data to be saved.
        Returns:
            bool: True if the data is successfully saved, False otherwise.
        """
        response = requests.post(f"{self.api_base_url}/processed_agent_data", data=json.dumps(
            processed_data, default=pydantic_core.to_jsonable_python))
        logging.info(f"Sending processed data to hub: {processed_data}")
        if response.status_code != 200:
            logging.info(
                f"Invalid Hub response\nData: {processed_data.model_dump_json()}\nResponse: {response}"
            )
            return False
        return True
