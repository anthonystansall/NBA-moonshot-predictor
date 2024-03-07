# db/db_connection.py
import configparser
import os
import psycopg2

def get_database_config():
    # Initialize the parser
    config = configparser.ConfigParser()
    config_file_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini')
    config.read(config_file_path)
    
    # Extract the PostgreSQL configuration
    dbname = config.get('PostgreSQL', 'dbname')
    user = config.get('PostgreSQL', 'user')
    password = config.get('PostgreSQL', 'password')
    host = config.get('PostgreSQL', 'host')
    
    return {
        'dbname': dbname,
        'user': user,
        'password': password,
        'host': host
    }

def test_database_connection():
    config = get_database_config()

    try:
        # Attempt to connect to the database
        conn = psycopg2.connect(**config)
        print("Successfully connected to the database.")
        # Optionally, execute a simple query
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            print("Query executed successfully:", cur.fetchone())
    except Exception as e:
        print("Failed to connect to the database:", e)
    finally:
        conn.close()

if __name__ == "__main__":
    pass