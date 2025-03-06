import json
import logging
from typing import List

import pydantic_core
import requests

from app.entities.processed_agent_data import ProcessedAgentData
from app.interfaces.store_gateway import StoreGateway


class StoreApiAdapter(StoreGateway):
    def __init__(self, api_base_url):
        self.api_base_url = api_base_url

    def save_data(self, processed_agent_data_batch: List[ProcessedAgentData]):
        """
        Save the processed road data to the Store API.
        Parameters:
            processed_agent_data_batch (dict): Processed road data to be saved.
        Returns:
            bool: True if the data is successfully saved, False otherwise.
        """
        # Implement it
        try:
            # Convert the processed agent data to JSON
            data = [data.dict() for data in processed_agent_data_batch]

            # Make a POST request to the Store API endpoint with the processed data
            # response = requests.post(f"{self.api_base_url}/processed_agent_data", json=data)
            response = requests.post(f"{self.api_base_url}/processed_agent_data", data=json.dumps(
                processed_agent_data_batch, default=pydantic_core.to_jsonable_python))

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                return True
            else:
                return False
        except Exception as e:
            # Handle exceptions if any
            print(f"Error saving data to Store API: {e}")
            return False
