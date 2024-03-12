# nba_api_client/client.py
import json
from nba_api.stats import endpoints
from data_pipeline.utils import utils

ENDPOINT_MAP = {
    'playergamelogs': {
        'resource': 'PlayerGameLogs',
        'resultSets': 'PlayerGameLogs'
    },
    'teamgamelogs': {
        'resource': 'TeamGameLogs',
        'resultSets': 'TeamGameLogs'
    },
    'teamdetails': {
        'resource': 'TeamDetails',
        'resultSets': 'TeamBackground'
    },
}

def fetch_nba_data(endpoint, **kwargs):
    print("Fetching NBA data...")
    if endpoint not in ENDPOINT_MAP:
        raise ValueError(f"Unsupported endpoint: {endpoint}")

    if utils.check_file_exists(endpoint, **kwargs):
        print("File already exists with requested data. Returning file contents instead.")
        file_path = utils.generate_file_name(endpoint, **kwargs)
        with open(file_path, "r") as file:
            return json.load(file)

    else:
        endpoint_function_name = ENDPOINT_MAP[endpoint]['resource']

        fetch_endpoint = getattr(endpoints, endpoint_function_name)
        if not fetch_endpoint:
            raise ValueError(f"{endpoint_function_name} not found in nba_api.stats.endpoints")

        response = fetch_endpoint(**kwargs).get_json()
        utils.save_data_to_file(response, endpoint, **kwargs)
        print("Successfully fetched NBA data and saved to json folder.")
        
        return response


if __name__ == "__main__":
    pass
