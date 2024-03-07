# db/queries.py
import pandas as pd
import psycopg2
from datetime import datetime
from sqlalchemy import create_engine
# from sqlalchemy.sql import select, distinct
# from sqlalchemy import Table, MetaData
from db.db_connection import get_database_config
# from db_connection import get_database_config

def insert_data(table, primary_key, headers, row_sets):
    print("Inserting data into database...")
    db_config = get_database_config()

    # Initialize counters
    total_attempts = len(row_sets)
    records_before = 0
    records_after = 0

    try:
        with psycopg2.connect(dbname=db_config['dbname'], user=db_config['user'], password=db_config['password'], host=db_config['host']) as conn:
            with conn.cursor() as cur:
                # Count records before insert
                cur.execute(f"SELECT COUNT(*) FROM {table};")
                records_before = cur.fetchone()[0]

                # Prepare the INSERT query with ON CONFLICT clause to do nothing on conflict
                columns = ', '.join(headers).lower()  # Convert headers to lowercase to match your table column names
                placeholders = ', '.join(['%s' for _ in headers])  # Create placeholders for values
                insert_query = f"""
                    INSERT INTO {table} ({columns})
                    VALUES ({placeholders})
                    ON CONFLICT ({primary_key}) DO NOTHING;
                """
                
                for row in row_sets:
                    # Convert game_date from string to datetime object if present
                    if "GAME_DATE" in headers:
                        game_date_index = headers.index("GAME_DATE")
                        row[game_date_index] = datetime.strptime(row[game_date_index], "%Y-%m-%dT%H:%M:%S")
                    
                    # Execute the insert query
                    cur.execute(insert_query, tuple(row))
                
                conn.commit()  # Commit the transaction
                
                # Count records after insert
                cur.execute(f"SELECT COUNT(*) FROM {table};")
                records_after = cur.fetchone()[0]

                # Calculate inserted and skipped
                records_inserted = records_after - records_before
                records_skipped = total_attempts - records_inserted

                print(f"Data insertion summary: {records_inserted} records inserted, {records_skipped} records skipped.")

    except psycopg2.Error as e:
        print(f"Database error occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

def get_unique_team_ids():
    print("Grabbing unique team ids...")
    db_config = get_database_config()
    try:
        # Establish the database connection using psycopg2
        with psycopg2.connect(dbname=db_config['dbname'], user=db_config['user'], password=db_config['password'], host=db_config['host']) as conn:
            with conn.cursor() as cur:
                # Execute the SELECT DISTINCT query
                cur.execute("SELECT DISTINCT team_id FROM team_game_logs;")
                
                # Fetch all unique team_ids
                unique_team_ids = [row[0] for row in cur.fetchall()]
                
                print("Unique Team IDs: ")
                print(unique_team_ids)
                return unique_team_ids

    except psycopg2.Error as e:
        print(f"Database error occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


def print_first_5_rows(table_name, columns=None):
    db_config = get_database_config()
    
    # Construct the database connection string (URI)
    db_uri = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['dbname']}"
    
    try:
        # Create an SQLAlchemy engine
        engine = create_engine(db_uri)
        
        # Construct the SQL query based on whether specific columns are provided
        if columns and isinstance(columns, list):
            columns_str = ', '.join(columns)  # Join the column names into a string for the SQL query
        else:
            columns_str = '*'  # Select all columns if none are specified
        
        query = f"SELECT {columns_str} FROM {table_name} LIMIT 5;"
        
        # Use pandas to read the SQL query into a DataFrame directly using the engine
        df = pd.read_sql(query, engine)
        
        # Print the DataFrame
        print(df)
    
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # With SQLAlchemy, the engine.dispose() method is used to close connections
        engine.dispose()

def update_team_details_with_location(csv_file_path):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file_path)

    # Get database configuration
    db_config = get_database_config()

    updated_teams = []  # To track which teams were updated

    try:
        # Connect to the database
        with psycopg2.connect(dbname=db_config['dbname'], user=db_config['user'], password=db_config['password'], host=db_config['host']) as conn:
            with conn.cursor() as cur:
                # Iterate through the DataFrame rows
                for index, row in df.iterrows():
                    # Extract team abbreviation, latitude, and longitude
                    team_abbreviation = row['Abbreviation']
                    latitude = row['Latitude']
                    longitude = row['Longitude']
                    
                    # Prepare the UPDATE statement
                    update_query = """
                    UPDATE team_details
                    SET latitude = %s, longitude = %s
                    WHERE abbreviation = %s;
                    """
                    # Execute the UPDATE statement
                    cur.execute(update_query, (latitude, longitude, team_abbreviation))
                    if cur.rowcount > 0:  # Check if the update affected any rows
                        updated_teams.append(team_abbreviation)
                
                # Commit the transaction
                conn.commit()
                
                # Fetch all team abbreviations from the database to identify which were not updated
                cur.execute("SELECT abbreviation FROM team_details;")
                all_teams = [row[0] for row in cur.fetchall()]
                
                # Determine which teams were not updated
                not_updated_teams = set(all_teams) - set(updated_teams)
                if not_updated_teams:
                    print("The following team abbreviations were not updated because latitude and longitude data was not found:")
                    for team in not_updated_teams:
                        print(team)
                else:
                    print("All team details updated successfully with location data.")

    except psycopg2.Error as e:
        print(f"Database error occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

def get_game_details():
    print("Getting game details...")
    db_config = get_database_config()
    game_details = []

    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT game_id, matchup, game_date
                    FROM team_game_logs;
                """)
                for row in cur.fetchall():
                    game_id, matchup, game_date = row
                    home_team = matchup.split(' vs. ')[1] if ' vs. ' in matchup else matchup.split(' @ ')[0]
                    game_details.append((game_id, game_date.date(), game_date.time(), home_team))
        print("Grabbed game details: ")
        print(game_details)
        return game_details
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

def insert_moon_data(headers, row_sets):
    print("Inserting moon events...")
    db_config = get_database_config()
    table = "moon_events"  # Adjust as necessary

    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                for row in row_sets:
                    columns = ', '.join(headers)
                    placeholders = ', '.join(['%s' for _ in headers])
                    query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) ON CONFLICT (moon_event_id) DO NOTHING;"
                    cur.execute(query, row)
                conn.commit()
                print("Successfully inserted moon events.")
    except Exception as e:
        print(f"Failed to insert moon data: {e}")

def get_lat_long(home_team):
    print("Grabbing lat and long...")
    db_config = get_database_config()
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT latitude, longitude FROM team_details WHERE abbreviation = %s;", (home_team,))
                result = cur.fetchone()
                if result:
                    print("Succesfully grabbed lat and long.")
                    return result
                else:
                    print("No abbreviation or lat long details in team_details.")
                    return None, None
    except Exception as e:
        print(f"Failed to fetch latitude and longitude for {home_team}: {e}")
        return None, None

if __name__ == "__main__":

    # print_first_5_rows("player_game_logs")
    # print_first_5_rows("moon_events")
    print_first_5_rows("team_details")
    print_first_5_rows("team_game_logs", columns=['team_abbreviation', 'game_date'])