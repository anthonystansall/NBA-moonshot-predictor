"""Module for performing database queries.

This module facilitates the insertion, update, and retrieval of data from
a PostgreSQL database. It provides functions to insert new records, update
existing records from a CSV file, retrieve distinct records, and fetch
records based on specific conditions. It uses psycopg2 for database
connection and operations.
"""

import pandas as pd
from psycopg2 import sql, Error
from psycopg2.extras import execute_batch

from data_pipeline.database import db_connection
from data_pipeline.utils import utils


def insert_new_data(table_name, primary_key, headers, records):
    """Inserts new records into a specified table.

    Args:
        table_name: Name of the table to insert data into.
        primary_key: The primary key column of the table.
        headers: List of column names for the insert operation.
        records: List of tuples representing the records to be inserted.
    """
    conn = None
    try:
        conn, _ = db_connection.connect_to_database()
        if conn is None:
            print("Database connection could not be established.")
            return

        record_count_sql = sql.SQL(
            "SELECT COUNT(*) FROM {}"
            ).format(sql.Identifier(table_name))

        insert_sql = sql.SQL(
            """INSERT INTO {table} ({columns})
            VALUES ({values})
            ON CONFLICT ({primary_key}) DO NOTHING;"""
            ).format(
                table=sql.Identifier(table_name),
                columns=sql.SQL(', ').join(
                    map(sql.Identifier, headers)),
                values=sql.SQL(', ').join(
                    sql.Placeholder() * len(headers)),
                primary_key=sql.Identifier(primary_key)
                )

        with conn.cursor() as cur:
            cur.execute(record_count_sql)
            initial_count = cur.fetchone()[0]

            execute_batch(cur, insert_sql, records)

            cur.execute(record_count_sql)
            final_count = cur.fetchone()[0]

            records_added = final_count - initial_count
            records_skipped = len(records) - records_added

            print(f"Records added: {records_added}")
            print(f"Records skipped: {records_skipped}")

    except Error as e:
        print(f"Error while inserting data: {e}")

    finally:
        if conn:
            conn.close()


def get_distinct_records(column_names, table_name):
    """Retrieves distinct records for specified columns from a table.

    Args:
        column_names: List or tuple of column names to get distinct records.
        table_name: Name of the table to query.

    Returns:
        A list of tuples representing the distinct records.
    """
    conn = None
    try:
        conn, _ = db_connection.connect_to_database()
        if conn is None:
            print("Database connection could not be established.")
            return None

        if not isinstance(column_names, (list, tuple)):
            raise ValueError(
                "column_names must be a list or tuple of column names"
                )

        columns = sql.SQL(", ").join(
            [sql.Identifier(column) for column in column_names]
            )

        get_distinct_sql = sql.SQL(
            "SELECT DISTINCT {column} FROM {table};"
            ).format(
                column=columns,
                table=sql.Identifier(table_name)
                )

        with conn.cursor() as cur:
            cur.execute(get_distinct_sql)
            unique_records = [row for row in cur.fetchall()]
            return unique_records

    except Error as e:
        print(f"Error while grabbing distinct records: {e}")
        return None

    finally:
        if conn:
            conn.close()


def update_records_from_csv(csv_file_path, table_name, key_column):
    """Updates records in a table from a CSV file.

    Args:
        csv_file_path: Path to the CSV file containing the update data.
        table_name: Name of the table to update.
        key_column: The key column to match records for updating.
    """
    conn = None
    try:
        conn, _ = db_connection.connect_to_database()
        if conn is None:
            print("Database connection could not be established.")
            return

        df = pd.read_csv(csv_file_path)
        df.columns = df.columns.str.lower()

        with conn.cursor() as cur:
            cur.execute(sql.SQL(
                """SELECT column_name
                FROM information_schema.columns
                WHERE table_name = {}"""
                ).format(
                    sql.Literal(table_name)
                    )
                )
            table_columns = [row[0] for row in cur.fetchall()]

        columns_to_update = set(df.columns) & set(table_columns)
        columns_to_update.discard(key_column.lower())

        for _, row in df.iterrows():
            set_clauses = sql.SQL(", ").join([sql.SQL("{} = {}").format(
                sql.Identifier(col), sql.Placeholder()
                ) for col in columns_to_update])
            values = [
                row[col] for col in columns_to_update
                ] + [row[key_column.lower()]]

            with conn.cursor() as cur:
                update_query = sql.SQL(
                    "UPDATE {} SET {} WHERE {} = {}").format(
                        sql.Identifier(table_name),
                        set_clauses,
                        sql.Identifier(key_column),
                        sql.Placeholder()
                        )
                cur.execute(update_query, values)

    except (Error, FileNotFoundError) as e:
        print(f"Error updating records: {e}")

    finally:
        if conn:
            conn.close()


def get_records(table_name,
                columns=None,
                where_clause=None,
                where_params=None):
    """Fetches records from a table, optionally filtered by a WHERE clause.

    Args:
        table_name: Name of the table to fetch records from.
        columns: List of column names to include in the result set. If None,
                 all columns are included.
        where_clause: Optional SQL WHERE clause for filtering records.
        where_params: Parameters to substitute into the WHERE clause.

    Returns:
        A list of tuples representing the fetched records.
    """
    conn = None
    try:
        conn, _ = db_connection.connect_to_database()
        if conn is None:
            print("Database connection could not be established.")
            return None

        with conn.cursor() as cur:
            if not columns:
                columns_sql = sql.SQL("*")
            else:
                columns_sql = sql.SQL(", ").join(map(sql.Identifier, columns))

            get_records_sql = sql.SQL(
                "SELECT {fields} FROM {table}"
                ).format(fields=columns_sql, table=sql.Identifier(table_name))

            if where_clause and where_params:
                get_records_sql = sql.SQL(
                    "{base_sql} WHERE {where}"
                    ).format(
                        base_sql=get_records_sql,
                        where=sql.SQL(where_clause)
                        )

            cur.execute(
                get_records_sql, where_params if where_params else None
                )

            return cur.fetchall()

    except Error as e:
        print(f"Error fetching records: {e}")
        return None

    finally:
        if conn:
            conn.close()


def create_all_records_all_tables_csv():
    """Creates a csv containing all joined records from database."""
    conn = None
    try:
        conn, _ = db_connection.connect_to_database()
        if conn is None:
            print("Database connection could not be established.")
            return

        with conn.cursor() as cur:
            # Directly using table and column names since they are static
            join_query = """
                SELECT pgl.*, tgl.*, td.*, me.*
                FROM player_game_logs pgl
                FULL OUTER JOIN team_game_logs tgl
                    ON pgl.team_id_game_id = tgl.team_id_game_id
                FULL OUTER JOIN team_details td
                    ON tgl.team_id = td.team_id
                FULL OUTER JOIN moon_events me
                    ON tgl.game_id = me.game_id;
            """

            cur.execute(join_query)
            records = cur.fetchall()

            columns = [desc[0] for desc in cur.description]

            data = [columns] + records

            utils.save_to_csv(data, "all_nba_moon_data.csv")

    except Error as e:
        print(f"Error fetching records: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    pass
