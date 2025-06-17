import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from datetime import datetime
from sqlalchemy import text
import json

# Hardcoded connection details for raw_db (target system)
RAW_DB_HOST = 'localhost'
RAW_DB_USER = 'root'
RAW_DB_PASSWORD = 'root'
RAW_DB_NAME = 'raw_db'

# Function to connect to the raw_db (target MySQL database)
def connect_to_raw_db():
    try:
        engine = create_engine(f"mysql+mysqlconnector://{RAW_DB_USER}:{RAW_DB_PASSWORD}@{RAW_DB_HOST}/{RAW_DB_NAME}")
        return engine
    except Exception as e:
        return None

# Total Refresh Function
def total_refresh(src_engine, raw_db_engine, table_name):
    try:
        with src_engine.connect() as src_conn, raw_db_engine.connect() as raw_db_conn:
            # First, check if the table exists in the source database
            check_table_query = f"SHOW TABLES LIKE '{table_name}'"
            table_exists = src_conn.execute(text(check_table_query)).fetchone() is not None
            
            if not table_exists:
                return f"Error: Table '{table_name}' not found in source database.", datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
            # Get the table creation SQL
            schema_query = f"SHOW CREATE TABLE {table_name}"
            schema_result = src_conn.execute(text(schema_query)).fetchone()
            if not schema_result:
                return f"Error: Could not get schema for table '{table_name}'.", datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
            schema_sql = schema_result[1].replace("DEFINER=root@localhost", "")
            
            # Drop the table if it exists in raw_db
            raw_db_conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            
            # Create the table in raw_db with the same schema
            raw_db_conn.execute(text(schema_sql))
            
            # Get number of rows in source table
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            source_count = src_conn.execute(text(count_query)).scalar()
            
            if source_count == 0:
                return f"No data in '{table_name}' for refresh, but table structure created.", datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # For small tables, use pandas
            if source_count < 10000:  # Adjust threshold as needed
                src_df = pd.read_sql(f"SELECT * FROM {table_name}", src_conn)
                if not src_df.empty:
                    src_df.to_sql(table_name, raw_db_engine, if_exists="append", index=False, method="multi", chunksize=1000)
            else:
                # For larger tables, use direct SQL INSERT (more efficient)
                # Get column names
                cols_query = f"SHOW COLUMNS FROM {table_name}"
                columns = [row[0] for row in src_conn.execute(text(cols_query)).fetchall()]
                columns_str = ", ".join(columns)
                
                # Use direct INSERT ... SELECT for efficiency
                insert_query = f"INSERT INTO raw_db.{table_name} ({columns_str}) SELECT {columns_str} FROM source_db.{table_name}"
                raw_db_conn.execute(text(insert_query))
            
            # Verify the data was inserted correctly
            row_count_query = f"SELECT COUNT(*) FROM {table_name}"
            raw_db_row_count = raw_db_conn.execute(text(row_count_query)).scalar()
            
            if raw_db_row_count == source_count:
                return f"Full Refresh completed for '{table_name}'. {raw_db_row_count} rows transferred.", datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            else:
                return f"Warning: For '{table_name}', {source_count} rows in source but only {raw_db_row_count} transferred.", datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"Error refreshing '{table_name}': {e}")  # Print for debugging
        return f"Error refreshing '{table_name}': {e}", datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Incremental Load Function
def incremental_load(src_engine, raw_db_engine, table_name):
    try:
        with src_engine.connect() as src_conn, raw_db_engine.connect() as raw_db_conn:
            # Check if table exists in raw_db
            check_table = raw_db_conn.execute(text(f"SHOW TABLES LIKE '{table_name}'"))
            table_exists = check_table.fetchone() is not None
            
            # If table doesn't exist, create it with the same schema as source
            if not table_exists:
                schema_query = f"SHOW CREATE TABLE {table_name}"
                schema_result = src_conn.execute(text(schema_query)).fetchone()
                if not schema_result:
                    return f"Error: Table '{table_name}' not found in source database.", None, datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                schema_sql = schema_result[1].replace("DEFINER=root@localhost", "")
                raw_db_conn.execute(text(schema_sql))
                
                # For new tables, perform a full load
                src_df = pd.read_sql(f"SELECT * FROM {table_name}", src_conn)
                if not src_df.empty:
                    src_df.to_sql(table_name, raw_db_engine, if_exists="append", index=False, method="multi", chunksize=1000)
                    return f"New table '{table_name}' created and loaded with {len(src_df)} rows.", src_df, datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                else:
                    return f"Table '{table_name}' created but no data to load.", None, datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Get the first column name from the table
            columns_query = f"SHOW COLUMNS FROM {table_name}"
            first_column = raw_db_conn.execute(text(columns_query)).fetchone()[0]  # First column name
            
            # If table exists, get last recorded value of the first column
            last_val_result = raw_db_conn.execute(text(f"SELECT MAX({first_column}) FROM {table_name}"))
            last_val = last_val_result.fetchone()[0]
            last_val = last_val if last_val is not None else 0  # Default to 0 if no data
            
            # Fetch only new records
            query = f"SELECT * FROM {table_name} WHERE {first_column} > '{last_val}'"
            src_df = pd.read_sql(query, src_conn)
            
            if src_df.empty:
                return f"No new data to load for '{table_name}'.", None, datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Insert only new records
            src_df.to_sql(table_name, raw_db_engine, if_exists="append", index=False, method="multi", chunksize=1000)
            
            return f"Incremental data loaded for '{table_name}'. {len(src_df)} new rows transferred.", src_df, datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        return f"Error: {e}", None, datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Scheduler Logic to run at specific time
# Update to the schedule_etl_job function in data_extraction.py
def schedule_etl_job(config_file='extraction.json'):
    try:
        # Read configuration file
        with open(config_file, 'r') as file:
            config = json.load(file)
        
        # Connect to source database
        src_engine = create_engine(
            f"mysql+mysqlconnector://{config['source_db']['user']}:{config['source_db']['password']}@{config['source_db']['host']}/{config['source_db']['db']}"
        )
        
        # Connect to raw_db
        raw_db_engine = connect_to_raw_db()
        if raw_db_engine is None:
            return "Error connecting to raw_db."
        
        results = []
        failed_tables = []
        
        # Loop through selected tables and perform the appropriate extraction
        for table in config['tables']:
            extraction_type = config['extraction_type']
            try:
                if extraction_type == "Full Refresh":
                    msg, timestamp = total_refresh(src_engine, raw_db_engine, table)
                else:
                    msg, df, timestamp = incremental_load(src_engine, raw_db_engine, table)
                
                # Check if operation was successful by looking for error indicators
                if "Error" in msg:
                    failed_tables.append(table)
                
                results.append({"table": table, "message": msg, "timestamp": timestamp})
            except Exception as e:
                error_msg = f"Exception while processing table {table}: {str(e)}"
                results.append({"table": table, "message": error_msg, "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
                failed_tables.append(table)
        
        # Report any failed tables
        if failed_tables:
            return {
                "status": f"ETL job completed with errors. Failed tables: {', '.join(failed_tables)}", 
                "results": results
            }
        else:
            return {"status": "ETL job completed successfully.", "results": results}
    except Exception as e:
        return {"status": f"Error during scheduled ETL job: {e}", "results": []}

# Execute the ETL job
if __name__ == "__main__":
    result = schedule_etl_job()
    print(result)