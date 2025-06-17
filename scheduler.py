# scheduler.py
import json
import time
import schedule
import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine

# Import functions from your modules
from data_extraction import schedule_etl_job, connect_to_raw_db
from mapping import DatasetMapper
from transformations_code import (
    extract_all_tables, transform_all_tables, load_all_tables, list_tables_in_raw_db
)

# Simple log function to record basic status and errors
def write_log(message):
    """
    Write simple status messages to status_log.txt
    """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open("status_log.txt", "a") as log_file:
        log_file.write(f"{timestamp} - {message}\n")

def run_extraction():
    """
    Run the extraction process based on extraction.json config
    """
    write_log("Extraction process started")
    try:
        # Use absolute path for more reliable file detection
        current_dir = os.path.dirname(os.path.abspath(__file__))
        extraction_path = os.path.join(current_dir, 'extraction.json')
        
        if os.path.exists(extraction_path):
            result = schedule_etl_job(extraction_path)
            write_log("Extraction completed successfully")
            return True
        else:
            write_log("ERROR: extraction.json not found, cannot run extraction process")
            return False
    except Exception as e:
        write_log(f"ERROR during extraction: {str(e)}")
        return False

def run_mapping():
    """
    Run the mapping process based on mapping_status.json config
    """
    write_log("Mapping process started")
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        mapping_path = os.path.join(current_dir, 'mapping_status.json')
        
        if os.path.exists(mapping_path):
            with open(mapping_path, 'r') as f:
                mapping_config = json.load(f)
            
            if mapping_config.get("mapping", True):
                # Run automated mapping
                mapper = DatasetMapper()
                mapper.merge_tables()
                write_log("Automated mapping completed successfully")
            else:
                # Move data directly to silver_db_mapping
                config = {
                    "host": "localhost",
                    "user": "root",
                    "password": "root",
                    "database": "raw_db"
                }
                conn = create_engine(f"mysql+mysqlconnector://{config['user']}:{config['password']}@{config['host']}/{config['database']}")
                
                # Connect to silver_db_mapping
                silver_config = {
                    "host": "localhost",
                    "user": "root",
                    "password": "root",
                    "database": "silver_db_mapping"
                }
                silver_engine = create_engine(f"mysql+mysqlconnector://{silver_config['user']}:{silver_config['password']}@{silver_config['host']}/{silver_config['database']}")
                
                # Get tables from raw_db
                tables = list_tables_in_raw_db()
                
                # Move tables to silver_db_mapping
                for table in tables:
                    try:
                        # Read data from raw_db
                        df = pd.read_sql(f"SELECT * FROM {table}", conn)
                        # Write to silver_db_mapping
                        df.to_sql(table, silver_engine, if_exists='replace', index=False)
                    except Exception as e:
                        write_log(f"ERROR moving table {table}: {str(e)}")
            
            return True
        else:
            write_log("ERROR: mapping_status.json not found, cannot run mapping process")
            return False
    except Exception as e:
        write_log(f"ERROR during mapping: {str(e)}")
        return False

def run_transformation():
    """
    Run the transformation process based on selected_transformations.json config
    """
    write_log("Transformation process started")
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        transform_path = os.path.join(current_dir, 'selected_transformations.json')
        
        if os.path.exists(transform_path):
            with open(transform_path, 'r') as f:
                transform_config = json.load(f)
            
            selected_transforms = transform_config.get("selected_transformations", [])
            
            # Extract data
            raw_data = extract_all_tables()
            if not raw_data:
                write_log("ERROR: No data found to transform")
                return False
            
            # Apply transformations
            transformed_data = transform_all_tables(raw_data, selected_transforms)
            
            # Load to silver_db
            load_all_tables(transformed_data, prefix="transformed")
            
            # Look for individual table aggregation files
            agg_results = {}
            
            # Find all *_agg.json files in the current directory
            agg_files = [f for f in os.listdir(current_dir) if f.endswith('_agg.json')]
            
            if agg_files:
                write_log(f"Found {len(agg_files)} table-specific aggregation files")
                
                # Process each table's aggregation from its specific file
                for agg_file in agg_files:
                    try:
                        with open(os.path.join(current_dir, agg_file), 'r') as f:
                            agg_config = json.load(f)
                        
                        # Extract table name from the filename
                        table_name = agg_file.replace('_agg.json', '')
                        
                        # Each file contains a dict with table name as key
                        for table, params in agg_config.items():
                            if table in transformed_data:
                                df = transformed_data[table]
                                groupby_cols = params.get("groupby_columns", [])
                                agg_cols = params.get("aggregation_columns", [])
                                agg_funcs = params.get("aggregation_functions", ["sum"])
                                
                                if groupby_cols and agg_cols and agg_funcs:
                                    try:
                                        aggregator_dict = {col: agg_funcs for col in agg_cols}
                                        aggregated_df = df.groupby(groupby_cols).agg(aggregator_dict)
                                        aggregated_df.columns = ["_".join(x) for x in aggregated_df.columns.ravel()]
                                        aggregated_df = aggregated_df.reset_index()
                                        agg_results[table] = aggregated_df
                                        write_log(f"Successfully aggregated table '{table}' with params from {agg_file}")
                                    except Exception as e:
                                        write_log(f"ERROR aggregating table '{table}' from {agg_file}: {str(e)}")
                    except Exception as e:
                        write_log(f"ERROR processing aggregation file {agg_file}: {str(e)}")
                
                # Load aggregated data
                if agg_results:
                    load_all_tables(agg_results, prefix="agg")
                    write_log(f"Loaded {len(agg_results)} aggregated tables to silver_db")
            
            # Mark transformation as complete
            with open(os.path.join(current_dir, "transformation_status.json"), "w") as f:
                json.dump({"transformation_complete": True}, f)
            
            write_log("Transformation process completed successfully")
            return True
        else:
            write_log("ERROR: selected_transformations.json not found, cannot run transformation process")
            return False
    except Exception as e:
        write_log(f"ERROR during transformation: {str(e)}")
        return False

def run_etl_pipeline():
    """
    Run the complete ETL pipeline in sequence
    """
    write_log("ETL Pipeline started")
    
    # Run extraction
    extraction_success = run_extraction()
    if not extraction_success:
        write_log("ETL Pipeline stopped: Extraction failed")
        return
    
    # Run mapping
    mapping_success = run_mapping()
    if not mapping_success:
        write_log("ETL Pipeline stopped: Mapping failed")
        return
    
    # Run transformation
    transformation_success = run_transformation()
    if not transformation_success:
        write_log("ETL Pipeline stopped: Transformation failed")
        return
    
    write_log("ETL Pipeline completed successfully")

def setup_schedule():
    """
    Set up the schedule based on extraction.json config
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    extraction_path = os.path.join(current_dir, 'extraction.json')
    
    if not os.path.exists(extraction_path):
        write_log("ERROR: extraction.json not found, cannot set up schedule")
        return
    
    with open(extraction_path, 'r') as f:
        config = json.load(f)
    
    frequency = config.get('frequency', None)
    
    if frequency == "Once":
        # Schedule a one-time run
        run_etl_pipeline()
    
    elif frequency.startswith("Every"):
        # Parse frequency for recurring schedule
        try:
            minutes = int(frequency.split()[1])
            write_log(f"Setting up recurring ETL job every {minutes} minutes")
            
            # Schedule recurring job
            schedule.every(minutes).minutes.do(run_etl_pipeline)
            
            # Keep the scheduler running
            while True:
                schedule.run_pending()
                time.sleep(1)
        except (ValueError, IndexError):
            write_log(f"ERROR: Invalid frequency format: {frequency}")
            return
    else:
        write_log(f"ERROR: Unsupported frequency: {frequency}")

def check_config_files():
    """
    Debug function to check the existence of config files
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    files_to_check = [
        'extraction.json', 
        'mapping_status.json', 
        'selected_transformations.json', 
        'transformation_status.json'
    ]
    
    found_files = []
    for file_name in files_to_check:
        file_path = os.path.join(current_dir, file_name)
        exists = os.path.exists(file_path)
        if exists:
            found_files.append(file_name)
    
    return len(found_files) > 0

if __name__ == "__main__":
    # Create or clear the log file at startup
    with open("status_log.txt", "w") as log_file:
        log_file.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - ETL Scheduler started\n")
    
    # Debug file existence
    files_exist = check_config_files()
    
    if files_exist:
        # Check if it's a scheduled run or an immediate run
        if len(sys.argv) > 1 and sys.argv[1] == "--now":
            write_log("Running ETL pipeline immediately")
            run_etl_pipeline()
        else:
            # Set up the schedule
            setup_schedule()
    else:
        write_log("ERROR: No configuration files found. Please run the frontend application first to create them.")