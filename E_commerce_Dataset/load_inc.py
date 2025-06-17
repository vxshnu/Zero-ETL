import pandas as pd
import os
import mysql.connector
from mysql.connector import Error

# Define directories and file mapping
leftover_dir = os.path.join("output_tables", "leftover")
# Map leftover CSV filenames to their corresponding MySQL table names
file_table_map = {
    "leftover_product_details.csv": "product_details",
    "leftover_product_rating.csv": "product_rating",
    "leftover_revenue_data.csv": "revenue_data",
    "leftover_time_periods.csv": "time_periods"
}

def create_db_connection():
    """Establish a connection to the MySQL database."""
    connection = None
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            database="source_db"
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")
    return connection

def load_leftover_data(file_path, table_name, connection):
    """
    Load 5 rows from the leftover CSV into the specified MySQL table,
    then update the CSV file to remove those rows.
    """
    try:
        # Read leftover data
        df = pd.read_csv(file_path)
        if df.empty:
            print(f"No leftover data in {file_path}.")
            return

        # Select the first 5 rows (or all if fewer than 5)
        rows_to_import = df.head(5)
        remaining_rows = df.iloc[5:]
        
        # Prepare data for insertion
        data = [tuple(x) for x in rows_to_import.to_numpy()]
        columns = ', '.join(rows_to_import.columns)
        placeholders = ', '.join(['%s'] * len(rows_to_import.columns))
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # Execute insertion
        cursor = connection.cursor()
        cursor.executemany(sql, data)
        connection.commit()
        print(f"Imported {len(rows_to_import)} rows into {table_name} from {os.path.basename(file_path)}.")
        
        # Overwrite the CSV file with the remaining rows
        remaining_rows.to_csv(file_path, index=False)
        print(f"Updated {os.path.basename(file_path)} with {len(remaining_rows)} leftover rows remaining.")
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

def main():
    connection = create_db_connection()
    if connection is None:
        print("Failed to connect to MySQL. Exiting.")
        return

    for filename, table_name in file_table_map.items():
        file_path = os.path.join(leftover_dir, filename)
        if os.path.exists(file_path):
            load_leftover_data(file_path, table_name, connection)
        else:
            print(f"Leftover file {filename} does not exist.")
    
    connection.close()
    print("MySQL connection closed.")

if __name__ == "__main__":
    main()
