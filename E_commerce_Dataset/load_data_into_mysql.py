import pandas as pd
import os
import mysql.connector
from mysql.connector import Error

# Create output directories if they don't exist
output_dir = "output_tables"
leftover_dir = os.path.join(output_dir, "leftover")
os.makedirs(output_dir, exist_ok=True)
os.makedirs(leftover_dir, exist_ok=True)

def split_dataframe(df):
    """Split a DataFrame into two halves."""
    half = len(df) // 2
    return df.iloc[:half].copy(), df.iloc[half:].copy()

# Read the input CSV file
input_file = "ecommerce_sales_analysis.csv"  # Replace with your actual file path
try:
    df = pd.read_csv(input_file)
    print(f"Successfully read {input_file} with {len(df)} rows")
except Exception as e:
    print(f"Error reading file: {e}")
    exit(1)

# 1. Create Product Details Table
product_details = df[['product_id', 'product_name', 'category', 'price']].copy()
# Ensure product_id is treated as string
product_details['product_id'] = product_details['product_id'].astype(str)
product_details.rename(columns={
    'product_id': 'item_id',
    'product_name': 'item_name',
    'category': 'item_category',
    'price': 'item_price'
}, inplace=True)
# Save full data for reference
product_details.to_csv(f"{output_dir}/product_details.csv", index=False)
# Split into imported and leftover
imported_product_details, leftover_product_details = split_dataframe(product_details)
imported_product_details.to_csv(f"{output_dir}/imported_product_details.csv", index=False)
leftover_product_details.to_csv(f"{leftover_dir}/leftover_product_details.csv", index=False)
print(f"Product details: {len(imported_product_details)} rows for import, {len(leftover_product_details)} rows leftover.")

# 2. Create Product Rating Table
product_rating = df[['product_id', 'review_score', 'review_count']].copy()
# Ensure product_id is string here too
product_rating['product_id'] = product_rating['product_id'].astype(str)
product_rating.to_csv(f"{output_dir}/product_rating.csv", index=False)
imported_product_rating, leftover_product_rating = split_dataframe(product_rating)
imported_product_rating.to_csv(f"{output_dir}/imported_product_rating.csv", index=False)
leftover_product_rating.to_csv(f"{leftover_dir}/leftover_product_rating.csv", index=False)
print(f"Product rating: {len(imported_product_rating)} rows for import, {len(leftover_product_rating)} rows leftover.")

# 3. Create Revenue Data Table from sales columns
sales_columns = [col for col in df.columns if col.startswith('sales_month_')]
revenue_rows = []
for _, row in df.iterrows():
    # Cast product_id to string to match product_details.item_id
    prod_id = str(row['product_id'])
    for col in sales_columns:
        month = int(col.split('_')[-1])  # Extract month number
        sales = row[col]
        revenue_rows.append({'product_id': prod_id, 'period': month, 'quantity_sold': sales})
revenue_data = pd.DataFrame(revenue_rows)
revenue_data.to_csv(f"{output_dir}/revenue_data.csv", index=False)
imported_revenue_data, leftover_revenue_data = split_dataframe(revenue_data)
imported_revenue_data.to_csv(f"{output_dir}/imported_revenue_data.csv", index=False)
leftover_revenue_data.to_csv(f"{leftover_dir}/leftover_revenue_data.csv", index=False)
print(f"Revenue data: {len(imported_revenue_data)} rows for import, {len(leftover_revenue_data)} rows leftover.")

# 4. Create Time Periods Table
time_periods = pd.DataFrame({
    'period_id': range(1, 13),
    'period_name': ['January', 'February', 'March', 'April', 'May', 'June', 
                    'July', 'August', 'September', 'October', 'November', 'December']
})
time_periods.to_csv(f"{output_dir}/time_periods.csv", index=False)
imported_time_periods, leftover_time_periods = split_dataframe(time_periods)
imported_time_periods.to_csv(f"{output_dir}/imported_time_periods.csv", index=False)
leftover_time_periods.to_csv(f"{leftover_dir}/leftover_time_periods.csv", index=False)
print(f"Time periods: {len(imported_time_periods)} rows for import, {len(leftover_time_periods)} rows leftover.")

print("\nData split complete. Imported files are in the 'output_tables' directory and leftover data is in 'output_tables/leftover'.")

# -----------------------------
# MySQL Connection and Import Section
# -----------------------------
def create_db_connection():
    """Create database connection to MySQL"""
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

def execute_query(connection, query):
    """Execute SQL queries in MySQL"""
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

# Connect to MySQL
connection = create_db_connection()

if connection is not None:
    # Create tables if they don't exist
    create_product_details_table = """
    CREATE TABLE IF NOT EXISTS product_details (
        item_id VARCHAR(255) PRIMARY KEY,
        item_name VARCHAR(255) NOT NULL,
        item_category VARCHAR(255),
        item_price DECIMAL(10, 2)
    );
    """
    create_product_rating_table = """
    CREATE TABLE IF NOT EXISTS product_rating (
        product_id VARCHAR(255) PRIMARY KEY,
        review_score DECIMAL(3, 2),
        review_count INT
    );
    """
    create_time_periods_table = """
    CREATE TABLE IF NOT EXISTS time_periods (
        period_id INT PRIMARY KEY,
        period_name VARCHAR(255) NOT NULL
    );
    """
    create_revenue_data_table = """
    CREATE TABLE IF NOT EXISTS revenue_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        product_id VARCHAR(255),
        period INT,
        quantity_sold INT,
        FOREIGN KEY (product_id) REFERENCES product_details(item_id)
    );
    """
    execute_query(connection, create_product_details_table)
    execute_query(connection, create_product_rating_table)
    execute_query(connection, create_time_periods_table)
    execute_query(connection, create_revenue_data_table)
    
    cursor = connection.cursor()

    def import_dataframe_to_mysql(df, table_name, connection):
        """Generic function to import a DataFrame to a MySQL table."""
        if df.empty:
            print(f"No data to import for {table_name}")
            return
        data = [tuple(x) for x in df.to_numpy()]
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        try:
            cur = connection.cursor()
            cur.executemany(sql, data)
            connection.commit()
            print(f"Data imported successfully into {table_name} ({len(df)} records)")
        except Error as err:
            print(f"Error importing data into {table_name}: {err}")

    # Import the first halves into MySQL
    import_dataframe_to_mysql(imported_product_details, "product_details", connection)
    import_dataframe_to_mysql(imported_product_rating, "product_rating", connection)
    import_dataframe_to_mysql(imported_time_periods, "time_periods", connection)
    
    # For revenue_data, import only rows that reference a product in the imported product_details
    imported_product_ids = imported_product_details['item_id'].tolist()
    # Debug: print number of matching revenue rows
    revenue_filtered = imported_revenue_data[imported_revenue_data['product_id'].isin(imported_product_ids)]
    print(f"Revenue data rows matching imported product IDs: {len(revenue_filtered)}")
    import_dataframe_to_mysql(revenue_filtered, "revenue_data", connection)
    
    print("Data imported into MySQL successfully. Leftover data is stored in the leftover directory.")
    
    cursor.close()
    connection.close()
    print("MySQL connection is closed")
else:
    print("Failed to connect to MySQL database")
