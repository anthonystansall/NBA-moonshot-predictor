import json

def get_headers_rowSets(response, result_set_name):
    print("Parsing response data...")
    data_parsed = json.loads(response)

    headers, row_sets = [], []
    for resultSet in data_parsed["resultSets"]:
        if resultSet["name"] == result_set_name:
            headers = resultSet["headers"]
            row_sets = resultSet["rowSet"]
            break

    print("Successfully parsed response data.")
    return headers, row_sets

def concat_primary_key(first_index, second_index, primarykey_name, headers, row_sets):
    print("Creating primary key...")
    # Find indexes of player_id and game_id in headers
    first_idx = headers.index(first_index)
    second_idx = headers.index(second_index)
    
    # Add 'player_game_id' to headers
    new_headers = [primarykey_name] + headers
    
    # Concatenate player_id and game_id for each row and prepend it to the row
    new_row_sets = [[f"{row[first_idx]}_{row[second_idx]}"] + row for row in row_sets]
    
    print("Successfully created primary key.")
    return new_headers, new_row_sets
