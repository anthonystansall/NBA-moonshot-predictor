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

def print_first_5_rows(table_name):
    db_config = get_database_config()
    
    # Construct the database connection string (URI)
    # This example assumes PostgreSQL, adjust accordingly for other databases
    db_uri = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['dbname']}"
    
    try:
        # Create an SQLAlchemy engine
        engine = create_engine(db_uri)
        
        # Use pandas to read the SQL query into a DataFrame directly using the engine
        query = f"SELECT * FROM {table_name} LIMIT 5;"
        df = pd.read_sql(query, engine)
        
        # Print the DataFrame
        print(df)
    
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # With SQLAlchemy, the engine.dispose() method is used to close connections
        engine.dispose()

if __name__ == "__main__":
    print_first_5_rows("player_game_logs")