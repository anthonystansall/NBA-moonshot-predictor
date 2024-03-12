# db/queries.py
import csv
from datetime import datetime
import pandas as pd
from psycopg2 import sql, Error
from psycopg2.extras import execute_batch, execute_values
from data_pipeline.database.setup import connect_to_database


def insert_new_data(table_name, primary_key, headers, records):
    """
    Insert data into the specified table without creating duplicates based on
    the primary key and print the number of records added or skipped.
    """
    conn = None
    try:
        conn, _ = connect_to_database()
        if conn is None:
            print("Database connection could not be established.")
            return

        record_count_sql = sql.SQL(
            "SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name))

        insert_sql = sql.SQL("""
                            INSERT INTO {table} ({columns})
                            VALUES ({values})
                            ON CONFLICT ({primary_key}) DO NOTHING;
                            """).format(
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
    """Returns a list of all unique records in a table."""
    conn = None
    try:
        conn, _ = connect_to_database()
        if conn is None:
            print("Database connection could not be established.")
            return None

        if not isinstance(column_names, (list, tuple)):
            raise ValueError("column_names must be a list or tuple of column names")

        columns = sql.SQL(", ").join([sql.Identifier(column) for column in column_names])
        get_distinct_sql = sql.SQL("""
                                        SELECT DISTINCT {column}
                                        FROM {table};
                                        """).format(
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
    """
    Update records in a specified table based on a CSV file.
    Column names from the CSV are matched to the table column names in lowercase.
    Records are matched based on a specified key column.
    """
    conn = None
    try:
        conn, _ = connect_to_database()
        if conn is None:
            print("Database connection could not be established.")
            return

        # Read CSV and lowercase headers
        df = pd.read_csv(csv_file_path)
        df.columns = df.columns.str.lower()
        
        # Fetch table columns
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT column_name FROM information_schema.columns WHERE table_name = {}").format(sql.Literal(table_name)))
            table_columns = [row[0] for row in cur.fetchall()]
        
        # Identify columns to update
        columns_to_update = set(df.columns) & set(table_columns)
        columns_to_update.discard(key_column.lower())  # Remove key column if present

        # User confirmation
        print(f"Columns to be updated: {columns_to_update}")
        user_confirmation = input("Proceed with updates? (yes/no): ").strip().lower()
        if user_confirmation != 'yes':
            print("Update cancelled.")
            return
        
        # Update records
        for index, row in df.iterrows():
            set_clauses = sql.SQL(", ").join([
                sql.SQL("{} = {}").format(sql.Identifier(col), sql.Placeholder()) for col in columns_to_update
            ])
            values = [row[col] for col in columns_to_update] + [row[key_column.lower()]]  # Append key column value for WHERE clause
            
            with conn.cursor() as cur:
                update_query = sql.SQL("UPDATE {} SET {} WHERE {} = {}").format(
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


def get_records(table_name, columns=None, where_clause=None, where_params=None):
    """
    Fetch records from a specified table with options to select specific columns and apply a WHERE filter.

    Parameters:
        table_name: The name of the table to fetch records from.
        columns: A list of column names to fetch. If None or empty, fetches all columns.
        where_clause: A string representing the WHERE clause without the 'WHERE' keyword, e.g., "id = %s".
        where_params: A tuple of parameters to safely inject into the where_clause.

    Returns:
        A list of tuples representing the fetched rows, or None if an error occurs.
    """
    conn = None
    try:
        conn, _ = connect_to_database()
        if conn is None:
            print("Database connection could not be established.")
            return None

        with conn.cursor() as cur:
            if not columns:
                columns_sql = sql.SQL("*")
            else:
                columns_sql = sql.SQL(", ").join(map(sql.Identifier, columns))

            get_records_sql = sql.SQL("SELECT {fields} FROM {table}").format(fields=columns_sql, table=sql.Identifier(table_name))

            if where_clause and where_params:
                get_records_sql = sql.SQL("{base_sql} WHERE {where}").format(base_sql=get_records_sql, where=sql.SQL(where_clause))

            cur.execute(get_records_sql, where_params if where_params else None)

            return cur.fetchall()

    except Error as e:
        print(f"Error fetching records: {e}")
        return None

    finally:
        if conn:
            conn.close()


def get_record_count(table_name):
    conn = None
    try:
        conn, _ = connect_to_database()  # Ensure this function returns a connection object correctly
        if conn is None:
            print("Database connection could not be established.")
            return None

        with conn.cursor() as cur:
            # Prepare the SQL query to count records
            cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name)))
            # Fetch the result
            count = cur.fetchone()[0]  # fetchone() returns a tuple, and we need the first element
            return count

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

    finally:
        if conn:
            conn.close()


def get_first_five_rows(table_name):
    """Fetches the first 5 rows of a specified table."""
    conn = None
    try:
        # Assuming connect_to_database() returns a connection object and optionally a cursor
        conn, _ = connect_to_database()
        if conn is None:
            print("Database connection could not be established.")
            return None
        
        # Prepare the SQL query
        query = sql.SQL("SELECT * FROM {} LIMIT 5").format(sql.Identifier(table_name))

        with conn.cursor() as cur:
            # Execute the query
            cur.execute(query)
            # Fetch the results
            rows = cur.fetchall()
            return rows

    except Error as e:
        print(f"Error while fetching the first 5 rows: {e}")
        return None
    finally:
        if conn:
            # Ensure the connection is closed
            conn.close()


def get_min_max_game_date(table_name):
    """
    Fetch the minimum and maximum game_date from a specified table.

    Parameters:
        table_name: The name of the table to fetch the dates from.

    Returns:
        A tuple containing the minimum and maximum game_date, or None if an error occurs.
    """
    conn = None
    try:
        conn, _ = connect_to_database()
        if conn is None:
            print("Database connection could not be established.")
            return None

        with conn.cursor() as cur:
            # Query to select the minimum and maximum game_date
            query = sql.SQL("SELECT MIN(game_date), MAX(game_date) FROM {table}").format(table=sql.Identifier(table_name))
            cur.execute(query)
            
            return cur.fetchone()

    except Error as e:
        print(f"Error fetching game dates: {e}")
        return None

    finally:
        if conn:
            conn.close()


def fetch_date_range_and_location():
    """
    Fetch the minimum and maximum game dates along with latitude and longitude
    for every unique combination of latitude and longitude in the team_details table.
    This data can be used to fetch moon data over a range of dates for each location.
    """
    conn = None
    try:
        conn, _ = connect_to_database()  # Replace with your actual connection function
        with conn.cursor() as cur:
            query = """
                SELECT MIN(g.game_date) as from_date, 
                       MAX(g.game_date) as to_date, 
                       d.latitude, 
                       d.longitude,
                       array_agg(g.game_id) as game_ids
                FROM team_game_logs g
                JOIN team_details d ON g.team_id = d.team_id
                GROUP BY d.latitude, d.longitude
            """
            cur.execute(query)
            ranges_locations_game_ids = cur.fetchall()
            return ranges_locations_game_ids
    except Exception as e:
        print(f"Error fetching date ranges, locations, and game IDs: {e}")
        return None
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    pass
