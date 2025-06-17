import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from dateutil.parser import parse
import difflib

# -------------------------
# Helper Functions for Fuzzy Matching and Column Detection
# -------------------------

def find_similar_column(target, columns, cutoff=0.7):
    """
    Returns the best matching column from the list based on fuzzy matching.
    If no match exceeds the cutoff, returns None.
    """
    matches = difflib.get_close_matches(target.lower(), [col.lower() for col in columns], n=1, cutoff=cutoff)
    if matches:
        for col in columns:
            if col.lower() == matches[0]:
                return col
    return None

def is_date_column(col, threshold=0.7):
    """
    Determines if a column should be considered a date column.
    Returns True if the column name contains 'date' or is similar to common date synonyms.
    """
    col_lower = col.lower()
    if "date" in col_lower:
        return True
    synonyms = ["arrivaldate","dob", "dateofbirth", "birthdate","orderdate","shippingdate","deliverydate","date","datetime","timestamp"]
    for syn in synonyms:
        if difflib.SequenceMatcher(None, col_lower, syn).ratio() > threshold:
            return True
    return False

# -------------------------
# Extraction Functions
# -------------------------

def connect_to_raw_db():
    config = {
        "host": "localhost",
        "user": "root",
        "password": "root",
        "database": "silver_db_mapping"
    }
    connection = mysql.connector.connect(**config)
    return connection

def list_tables_in_raw_db():
    conn = connect_to_raw_db()
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES;")
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return tables

def extract_all_tables():
    """
    Extracts data from every table in raw_db.
    Returns a dictionary mapping table name -> DataFrame.
    """
    tables = list_tables_in_raw_db()
    data = {}
    for table in tables:
        try:
            data[table] = pd.read_sql(f"SELECT * FROM {table}", connect_to_raw_db())
        except Exception as e:
            print(f"Error extracting table {table}: {e}")
    return data

# -------------------------
# Transformation Functions
# -------------------------

def remove_duplicates(df):
    return df.drop_duplicates()

def remove_nulls(df):
    return df.dropna()

def impute_nulls(df):
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(df[col].mean())
        elif pd.api.types.is_string_dtype(df[col]) or pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].fillna("N/A")
    return df

def trim_whitespace(df):
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip()
    return df

def standardize_dates(df):
    for col in df.columns:
        if is_date_column(col):
            def parse_date(val):
                if pd.isnull(val):
                    return None
                try:
                    dt = parse(str(val), dayfirst=True, fuzzy=True)
                    return dt.strftime('%Y-%m-%d')
                except Exception as e:
                    print(f"Unable to parse '{val}' in column '{col}': {e}")
                    return None
            df[col] = df[col].apply(parse_date)
    return df

def combine_names(df):
    columns = list(df.columns)
    first_name_col = find_similar_column("first name", columns, cutoff=0.6) or find_similar_column("firstname", columns, cutoff=0.6)
    last_name_col = find_similar_column("last name", columns, cutoff=0.6) or find_similar_column("lastname", columns, cutoff=0.6)
    if first_name_col and last_name_col:
        df["full_name"] = (
            df[first_name_col].fillna("").astype(str).str.strip() + " " +
            df[last_name_col].fillna("").astype(str).str.strip()
        )
    return df

def split_names(df):
    if "full_name" in df.columns:
        df["full_name"] = df["full_name"].fillna("").astype(str)
        df[["first_name_split", "last_name_split"]] = df["full_name"].str.split(" ", n=1, expand=True)
    return df

# -------------------------
# Masking Functions for Sensitive Data
# -------------------------

def mask_phone_number(phone, visible_digits=4):
    """
    Masks a phone number, showing only the last n digits
    Example: mask_phone_number("1234567890") returns "******7890"
    """
    if not phone or not isinstance(phone, str):
        return phone
    
    # Remove non-numeric characters
    clean_phone = ''.join(c for c in phone if c.isdigit())
    
    if not clean_phone:
        return phone
    
    # Keep only the specified number of digits visible
    masked_length = max(0, len(clean_phone) - visible_digits)
    masked_phone = '*' * masked_length + clean_phone[-visible_digits:]
    
    return masked_phone

def mask_email(email, visible_domain=True):
    """
    Masks an email address, showing only the first character of the local part
    and optionally the domain.
    
    Example: 
    mask_email("john.doe@example.com") returns "j*******@example.com"
    mask_email("john.doe@example.com", visible_domain=False) returns "j*******@********"
    """
    if not email or not isinstance(email, str) or '@' not in email:
        return email
    
    local_part, domain = email.split('@', 1)
    
    # Keep only the first character of the local part visible
    if local_part:
        masked_local = local_part[0] + '*' * max(1, len(local_part) - 1)
    else:
        masked_local = '*'
    
    # Decide whether to mask the domain
    if visible_domain:
        masked_email = f"{masked_local}@{domain}"
    else:
        masked_email = f"{masked_local}@{'*' * len(domain)}"
    
    return masked_email

def mask_sensitive_data(df):
    """
    Creates masked versions of phone and email columns in the dataframe
    """
    df_result = df.copy()
    columns = list(df.columns)
    
    # Find and mask phone number columns
    phone_cols = [col for col in columns if any(term in col.lower() for term in ['phone', 'mobile', 'cell', 'tel'])]
    for col in phone_cols:
        df_result[f"{col}_masked"] = df_result[col].astype(str).apply(mask_phone_number)
    
    # Find and mask email columns
    email_cols = [col for col in columns if any(term in col.lower() for term in ['email', 'e-mail', 'mail'])]
    for col in email_cols:
        df_result[f"{col}_masked"] = df_result[col].astype(str).apply(mask_email)
    
    return df_result

# Dictionary mapping transformation names to functions.
TRANSFORMATIONS = {
    "Remove Duplicates": remove_duplicates,
    "Remove Null Rows": remove_nulls,
    "Impute Nulls": impute_nulls,
    "Trim Whitespace": trim_whitespace,
    "Standardize Dates": standardize_dates,
    "Combine Names": combine_names,
    "Split Names": split_names,
    "Mask Sensitive Data": mask_sensitive_data,
}

def transform_data(df, selected_transforms):
    """
    Applies the selected transformation pipeline to a DataFrame.
    """
    for name in selected_transforms:
        func = TRANSFORMATIONS.get(name)
        if func:
            df = func(df)
    return df

def transform_all_tables(raw_data, selected_transforms):
    """
    Applies the selected transformation pipeline to every table in raw_data.
    Returns a dictionary mapping table name -> transformed DataFrame.
    """
    transformed = {}
    for table, df in raw_data.items():
        if selected_transforms:
            transformed_df = transform_data(df.copy(), selected_transforms)
        else:
            transformed_df = df.copy()
        transformed[table] = transformed_df
    return transformed

# -------------------------
# Aggregation Functions
# -------------------------

def aggregate_all_tables(transformed_data, groupby_cols, agg_cols, agg_funcs):
    """
    Applies aggregation to each table in transformed_data (if required columns exist).
    Returns a dictionary mapping table name -> aggregated DataFrame.
    """
    agg_results = {}
    for table, df in transformed_data.items():
        if all(col in df.columns for col in groupby_cols) and all(col in df.columns for col in agg_cols):
            aggregator_dict = {col: agg_funcs for col in agg_cols}
            try:
                aggregated_df = df.groupby(groupby_cols).agg(aggregator_dict)
                aggregated_df.columns = ["_".join(x) for x in aggregated_df.columns.ravel()]
                aggregated_df = aggregated_df.reset_index()
                agg_results[table] = aggregated_df
            except Exception as e:
                print(f"Error aggregating table '{table}': {e}")
    return agg_results

# -------------------------
# Loading Functions
# -------------------------

def load_table_to_silver(df, silver_table_name):
    silver_config = {
        "host": "localhost",
        "user": "root",
        "password": "root",
        "database": "silver_db"
    }
    conn_str = (
        f"mysql+mysqlconnector://{silver_config['user']}:{silver_config['password']}@{silver_config['host']}/{silver_config['database']}"
    )
    engine = create_engine(conn_str)
    try:
        df.to_sql(name=silver_table_name, con=engine, if_exists='replace', index=False)
    except Exception as e:
        print(f"Error loading table '{silver_table_name}' into silver_db: {e}")

def load_all_tables(data_dict, prefix):
    """
    Loads every table from data_dict into silver_db.
    Table names are prefixed by the given prefix.
    """
    for table, df in data_dict.items():
        silver_table_name = f"{prefix}_{table}"
        load_table_to_silver(df, silver_table_name)
