"""
This module provides functionalities for setting up, creating, and wiping
a PostgreSQL database. It includes functions to create and drop both the
database and its tables based on the provided schemas.
"""
import os
import configparser
from psycopg2 import connect, sql, Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from data_pipeline.database.schema import ALL_TABLE_SCHEMAS

CONFIG_FILE_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), '..', 'config', 'db_credentials.ini')
)


def get_database_config():
    """Returns the database configuration parameters."""
    config = configparser.ConfigParser()
    if not os.path.exists(CONFIG_FILE_PATH):
        raise FileNotFoundError(
            f"Configuration file not found at {CONFIG_FILE_PATH}"
        )

    config.read(CONFIG_FILE_PATH)

    try:
        db_config = {
            'dbname': config.get('PostgreSQL', 'dbname'),
            'user': config.get('PostgreSQL', 'user'),
            'password': config.get('PostgreSQL', 'password'),
            'host': config.get('PostgreSQL', 'host')
        }
    except configparser.NoSectionError as e:
        raise configparser.NoSectionError(
            "Section 'PostgreSQL' not found in the configuration file."
        ) from e

    return db_config


def connect_to_database(config=None,
                        admin_db=False,
                        default_dbname='postgres'
                        ):
    """
    Connects to the PostgreSQL database based on the configuration parameters
    or connection to the default database for administrative actions such as
    creating or deleting databases.

    Returns the connection object and the original dbname in the configuration
    file that can be used for administrative functions.
    """
    if config is None:
        config = get_database_config()

    # Toggle to default database for administrative actions.
    if admin_db:
        conn_dbname = default_dbname
    else:
        conn_dbname = config.get('dbname')

    config_dbname = config.get('dbname')

    try:
        conn = connect(
            dbname=conn_dbname,
            user=config['user'],
            password=config['password'],
            host=config['host']
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn, config_dbname
    except Error as e:
        print(f"Database connection failed: {e}")
        return None, None


def create_database():
    """Creates the database from config file."""
    conn = None
    try:
        conn, dbname = connect_to_database(admin_db=True)
        if conn is None:
            print("Database connection could not be established.")
            return

        check_db_exists_sql = sql.SQL(
            "SELECT 1 FROM pg_database WHERE datname = {};"
            ).format(sql.Literal(dbname))

        create_db_sql = sql.SQL(
            "CREATE DATABASE {};"
            ).format(sql.Identifier(dbname))

        with conn.cursor() as cur:
            cur.execute(check_db_exists_sql)
            if cur.fetchone():
                print(f"{dbname} already exists.")
            else:
                cur.execute(create_db_sql)
                print(f"{dbname} created successfully.")
    except Error as e:
        print(f"Failed to execute database operation: {e}")
    finally:
        if conn:
            conn.close()


def drop_database():
    """Deletes the database from config file."""
    conn = None
    try:
        conn, dbname = connect_to_database(admin_db=True)
        if conn is None:
            print("Database connection could not be established.")
            return

        drop_db_sql = sql.SQL(
            "DROP DATABASE IF EXISTS {};"
            ).format(sql.Identifier(dbname))

        with conn.cursor() as cur:
            cur.execute(drop_db_sql)
            print(f"{dbname} deleted successfully.")
    except Error as e:
        print(f"Failed to delete database {dbname}: {e}")
    finally:
        if conn:
            conn.close()


def create_table(table):
    """Create table from provided table schema."""
    conn = None
    try:
        conn, _ = connect_to_database()
        if conn is None:
            print("Database connection could not be established.")
            return

        table_name = table.get('table_name')
        table_create_sql = table.get('table_creation_sql')
        if not table_name or not table_create_sql:
            print("Table schema is missing required information.")
            return

        with conn.cursor() as cur:
            cur.execute(table_create_sql)
            print(f"{table_name} created successfully.")

    except Error as e:
        print(f"Failed to create {table_name}: {e}")
    finally:
        if conn:
            conn.close()


def drop_table(table):
    """Drop table from provided table schema."""
    conn = None
    try:
        conn, _ = connect_to_database()
        if conn is None:
            print("Database connection could not be established.")
            return

        table_name = table.get('table_name')
        if not table_name:
            print("Table schema does not contain a table name.")
            return

        drop_table_sql = sql.SQL(
            "DROP TABLE IF EXISTS {} CASCADE;"
            ).format(sql.Identifier(table_name))

        with conn.cursor() as cur:
            cur.execute(drop_table_sql)
            print(f"{table_name} dropped successfully.")

    except Error as e:
        print(f"Failed to drop '{table_name}': {e}")
    finally:
        if conn:
            conn.close()


def setup_database_schema():
    """Sets up database and all defined tables."""
    create_database()
    for table in ALL_TABLE_SCHEMAS:
        create_table(table)


def wipe_database_schema():
    """Wipes the database and all defined tables."""
    for table in ALL_TABLE_SCHEMAS:
        drop_table(table)
    drop_database()


if __name__ == "__main__":
    # Used to test full db setup and tear down.
    setup_database_schema()
    # wipe_database_schema()
