"""Database connection module for PostgreSQL.

This module provides functionalities to read database configurations from a
configuration file and establish a connection to a PostgreSQL database using
these configurations. It supports connecting to a specific database for
regular operations or to a default administrative database for administrative
tasks like creating or deleting databases.
"""

import os
import configparser

from psycopg2 import connect, Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


# Define the path to the configuration file relative to this module.
CONFIG_FILE_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), '..', 'config', 'db_credentials.ini')
)


def get_database_config():
    """Fetches the database configuration from a .ini file.

    Reads database credentials from a configuration file and returns them as
    a dictionary.

    Returns:
        A dictionary containing database configuration.

    Raises:
        FileNotFoundError: If the configuration file does not exist.
        configparser.NoSectionError: If the required section is not found in
            the configuration file.
    """
    config = configparser.ConfigParser()
    if not os.path.exists(CONFIG_FILE_PATH):
        raise FileNotFoundError(
            f"Configuration file not found at {CONFIG_FILE_PATH}"
        )

    config.read(CONFIG_FILE_PATH)

    if not config.has_section('PostgreSQL'):
        raise configparser.NoSectionError(
            "Section 'PostgreSQL' not found in the configuration file."
        )

    db_config = {key: value for key, value in config.items('PostgreSQL')}

    return db_config


def connect_to_database(config=None,
                        admin_db=False,
                        default_dbname='postgres'):
    """Establishes a connection to the database.

    Connects to the PostgreSQL database using provided configuration or
    fetches configuration from file if not provided. Optionally connects to
    a default database for administrative tasks.

    Args:
        config: A dictionary containing the database configuration. Defaults
            to None, in which case the configuration is read from file.
        admin_db: Whether to connect to the default database for
            administrative tasks. Defaults to False.
        default_dbname: The name of the default database for administrative
            tasks. Defaults to 'postgres'.

    Returns:
        A tuple containing the connection object and the database name, or
        (None, None) if the connection fails.
    """
    if config is None:
        config = get_database_config()

    # Toggles to default db name for administrative tasks.
    conn_dbname = default_dbname if admin_db else config['dbname']
    config_dbname = config.get('dbname')

    try:
        connection = connect(
            dbname=conn_dbname,
            user=config['user'],
            password=config['password'],
            host=config['host']
        )
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return connection, config_dbname
    except Error as e:
        print(f"Database connection failed: {e}")
        return None, None


if __name__ == "__main__":
    # Example usage: Connect to the database and print the connection status
    conn, _ = connect_to_database()
    if conn:
        print("Connected to database.")
    else:
        print("Failed to connect to the database.")
