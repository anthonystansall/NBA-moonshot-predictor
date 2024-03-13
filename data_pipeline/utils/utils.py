"""Module for utility functions used in api module.

This module provides utility functions to save, store, and retrieve data from
the NBA and Astronomy API in order to limit future API calls.
"""
import os
import json
import hashlib
import csv

# Define the base directory for storing JSON data files
BASE_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'json')


def generate_file_name(endpoint, **kwargs):
    """Generates a file name based on endpoint and keyword arguments.

    Args:
        endpoint: Name of endpoint used in nba_api or Astronomy API.
        kwargs: Additional arguments used in the API call.

    Returns:
        A hashed file name based on arguments used in the API call.
    """
    sorted_kwargs = sorted(kwargs.items())
    identifier = f"{endpoint}_{sorted_kwargs}"
    file_name_hash = hashlib.sha256(identifier.encode()).hexdigest()
    file_path = os.path.join(BASE_DIR, f"{file_name_hash}.json")
    return file_path


def save_data_to_file(response, endpoint, **kwargs):
    """Saves API response data using API arguments as the naming convention.

    Args:
        response: API response data to store in file.
        endpoint: Name of endpoint used in nba_api or Astronomy API.
        kwargs: Additional arguments used in the API call.
    """
    file_path = generate_file_name(endpoint, **kwargs)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(response, file)


def check_file_exists(endpoint, **kwargs):
    """Checks if file exists based on endpoint and keyword arguments.

    Args:
        endpoint: Name of endpoint used in nba_api or Astronomy API.
        kwargs: Additional arguments used in the API call.

    Returns:
        The file path based on the naming convention.
    """
    file_path = generate_file_name(endpoint, **kwargs)
    return os.path.exists(file_path)


def get_home_team(matchup):
    """Identifies the home team from matchup format from nba_api.

    Args:
        matchup: Game matchup in the form of 'LAL vs. LAC' or 'LAL @ LAC'

    Returns:
        The abbreviation of the home team.
    """
    if " @ " in matchup:
        _, home_team = matchup.split(" @ ")
    elif " vs. " in matchup:
        home_team, _ = matchup.split(" vs. ")
    else:
        raise ValueError("Unknown matchup format.")
    return home_team


def save_to_csv(data, filename, directory="data_pipeline/data"):
    """Saves data as a csv to data directory.

    Args:
        data: Data to store in csv.
        filename: Desired name for file.
        directory: Location to save the file.
    """
    os.makedirs(directory, exist_ok=True)

    file_path = os.path.join(directory, filename)

    try:
        with open(file_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerows(data)

        print(f"Data successfully saved to {file_path}")
    except (OSError, csv.Error) as e:
        print(f"An error occurred while writing to the file: {e}")


if __name__ == "__main__":
    pass
