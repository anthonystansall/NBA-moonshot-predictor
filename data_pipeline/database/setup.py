"""Module for managing database operations.

This module provides functions to create and drop databases and tables,
leveraging the psycopg2 library for PostgreSQL database interactions.
It includes functionality for setting up and wiping the database schema,
based on predefined table schemas.
"""

from psycopg2 import sql, Error

from data_pipeline.database import db_connection
from data_pipeline.database.schema import ALL_TABLE_SCHEMAS


def create_database():
    """Creates a new database if it does not already exist."""
    conn = None
    try:
        conn, dbname = db_connection.connect_to_database(admin_db=True)
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
    """Drops the database if it exists."""
    conn = None
    try:
        conn, dbname = db_connection.connect_to_database(admin_db=True)
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


def create_table(table_schema):
    """Creates a table based on the provided schema.

    Args:
        table_schema: A dictionary containing the table name and creation SQL.
    """
    conn = None
    try:
        conn, _ = db_connection.connect_to_database()
        if conn is None:
            print("Database connection could not be established.")
            return

        table_name = table_schema.get('table_name')
        table_create_sql = table_schema.get('table_creation_sql')
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


def drop_table(table_schema):
    """Drops a table based on the provided schema.

    Args:
        table_schema: A dictionary containing the table name.
    """
    conn = None
    try:
        conn, _ = db_connection.connect_to_database()
        if conn is None:
            print("Database connection could not be established.")
            return

        table_name = table_schema.get('table_name')
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
    """Sets up the database schema by creating the database and all tables."""
    create_database()
    for table in ALL_TABLE_SCHEMAS:
        create_table(table)


def wipe_database_schema():
    """Wipes the database schema by dropping all tables and the database."""
    for table in ALL_TABLE_SCHEMAS:
        drop_table(table)
    drop_database()


if __name__ == "__main__":
    # Example usage: Setup or wipe the database schema.
    setup_database_schema()
    # To wipe the database schema, uncomment the following line:
    # wipe_database_schema()
