"""Module for fetching and transforming NBA data using nba_api package.

This module interfaces with the NBA API through the use of the nba_api package.
It supports fetching data for different endpoints such as player game logs,
team game logs, and team details, and saving the fetched data to a file. It
also provides functionality to parse and transform the raw JSON data into a
more usable format.
"""

import json
from datetime import datetime

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
    """Fetches NBA data from the specified endpoint.

    Args:
        endpoint: The API endpoint to fetch data from.
        **kwargs: Additional keyword arguments to pass to the API call.

    Returns:
        The fetched data as a JSON object.

    Raises:
        ValueError: If the endpoint is not supported or found in the NBA API.
    """
    print("Fetching NBA data...")
    if endpoint not in ENDPOINT_MAP:
        raise ValueError(f"Unsupported endpoint: {endpoint}")

    if utils.check_file_exists(endpoint, **kwargs):
        print("File already exists with requested data.")
        print("Returning file contents instead.")
        file_path = utils.generate_file_name(endpoint, **kwargs)
        with open(file_path, "r", encoding="utf-8") as file:
            return json.load(file)

    else:
        endpoint_function_name = ENDPOINT_MAP[endpoint]['resource']

        fetch_endpoint = getattr(endpoints, endpoint_function_name)
        if not fetch_endpoint:
            raise ValueError(
                f"{endpoint_function_name} not found in nba_api")

        response = fetch_endpoint(**kwargs).get_json()
        utils.save_data_to_file(response, endpoint, **kwargs)
        print("Successfully fetched NBA data and saved to json folder.")

        return response


def parse_transform_nba_data(response,
                             resultset_name,
                             first_primary_key=None,
                             second_primary_key=None):
    """Parses and transforms NBA data from a raw JSON response.

    Args:
        response: The raw JSON response from the NBA API.
        result_set_name: The name of the result set to extract data from.
        first_primary_key: Optional. The first column to use as part of a
            composite primary key.
        second_primary_key: Optional. The second column to use as part of a
            composite primary key.

    Returns:
        A tuple containing the headers and records of the transformed data.
    """
    response_json = json.loads(response)
    result_set = next((
        item for item in response_json['resultSets']
        if item['name'] == resultset_name), None
        )

    if result_set is None:
        print(f"No result set found with the name: {resultset_name}")
        return None, None

    headers = result_set['headers']
    records = result_set['rowSet']

    headers = [header.lower() for header in headers]

    if 'game_date' in headers:
        game_date_index = headers.index('game_date')
        records = [
            record[:game_date_index] + [
                datetime.strptime(record[game_date_index], '%Y-%m-%dT%H:%M:%S')
                ] + record[game_date_index + 1:]
            if record[game_date_index] else record
            for record in records
        ]

    if first_primary_key and second_primary_key:
        new_primary_key = f"{first_primary_key}_{second_primary_key}"
        headers.append(new_primary_key.lower())

        first_key_index = headers.index(first_primary_key.lower())
        second_key_index = headers.index(second_primary_key.lower())

        records = [
            record + [f"{record[first_key_index]}_{record[second_key_index]}"]
            for record in records
        ]

    return headers, records


if __name__ == "__main__":
    pass
