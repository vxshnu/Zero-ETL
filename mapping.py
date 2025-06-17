import json
import pandas as pd
from gensim.models import Word2Vec
from sqlalchemy import create_engine, inspect
import os

# ---------------------- DATABASE CONFIGURATION ----------------------
SOURCE_DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "raw_db"
}

TARGET_DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "silver_db_mapping"
}

# ---------------------- SIMILARITY THRESHOLD ----------------------
SIMILARITY_THRESHOLD = 0.7

# ---------------------- CONNECT TO DATABASES ----------------------
source_engine = create_engine(
    f"mysql+mysqlconnector://{SOURCE_DB_CONFIG['user']}:{SOURCE_DB_CONFIG['password']}@{SOURCE_DB_CONFIG['host']}/{SOURCE_DB_CONFIG['database']}"
)
target_engine = create_engine(
    f"mysql+mysqlconnector://{TARGET_DB_CONFIG['user']}:{TARGET_DB_CONFIG['password']}@{TARGET_DB_CONFIG['host']}/{TARGET_DB_CONFIG['database']}"
)

# ---------------------- CLASS: DatasetMapper ----------------------
class DatasetMapper:
    def __init__(self):
        self.dataset = pd.DataFrame()
        self.schema_info, self.primary_keys, self.foreign_keys = self.extract_schema()
        self.word2vec_model = self.train_word2vec()

    def extract_schema(self):
        """
        Extracts schema from the MySQL source database, including primary keys and foreign keys.
        Returns:
            schema_info: dict {table_name: [col1, col2, ...]}
            primary_keys: dict {table_name: primary_key_column}
            foreign_keys: dict {table_name: foreign_key_column}
        """
        inspector = inspect(source_engine)
        schema_info = {}
        primary_keys = {}
        foreign_keys = {}

        for table_name in inspector.get_table_names():
            columns = [col["name"] for col in inspector.get_columns(table_name)]
            schema_info[table_name] = columns

            pk = inspector.get_pk_constraint(table_name)['constrained_columns']
            if pk:
                primary_keys[table_name] = pk[0]

            fk = inspector.get_foreign_keys(table_name)
            if fk:
                # For simplicity, assume one foreign key per table.
                foreign_keys[table_name] = fk[0]['constrained_columns'][0]

        return schema_info, primary_keys, foreign_keys

    def train_word2vec(self, model_path="word2vec_model.bin"):
        """
        Trains a Word2Vec model for semantic similarity detection or loads an existing one.
        Allows for incremental training with new column names.
        """
        try:
            # Try to load existing model
            model = Word2Vec.load(model_path)
            print("Loaded existing Word2Vec model")
            
            # Get current column names
            column_names = []
            for columns in self.schema_info.values():
                column_names.extend(columns)
            tokenized_columns = [col.split("_") for col in column_names]
            
            # Update model with new vocabulary
            model.build_vocab(tokenized_columns, update=True)
            
            # Train on new data (incremental training)
            model.train(tokenized_columns, 
                    total_examples=len(tokenized_columns),
                    epochs=model.epochs)
            
        except (FileNotFoundError, ValueError):
            print("Creating new Word2Vec model")
            # If no existing model, create common database terminology corpus
            common_terms = [
            # Basic identifiers
            "id", "name", "first", "last", "address", "city", "state", 
            "zip", "phone", "email", "date", "created", "modified",
            "customer", "product", "order", "price", "quantity", "total",
            
            # E-commerce specific
            "sku", "upc", "isbn", "ean", "gtin", "mpn", "asin",
            "category", "subcategory", "brand", "manufacturer", "vendor", "supplier",
            "inventory", "stock", "availability", "in_stock", "backorder", "restock",
            "discount", "coupon", "promo", "promotion", "sale", "special_offer",
            "shipping", "delivery", "tracking", "carrier", "fulfillment", "warehouse",
            "payment", "transaction", "invoice", "refund", "credit", "debit", "tax",
            "cart", "wishlist", "abandoned", "checkout", "conversion",
            "rating", "review", "feedback", "comment", "testimonial",
            
            # Product attributes
            "color", "size", "weight", "height", "width", "length", "dimension",
            "material", "fabric", "texture", "pattern", "style", "model", "version",
            "feature", "specification", "description", "detail", "overview",
            "image", "photo", "thumbnail", "gallery", "video", "media", "asset",
            
            # Marketing
            "campaign", "referral", "affiliate", "utm_source", "utm_medium", "utm_campaign",
            "visit", "session", "pageview", "impression", "click", "bounce", "retention",
            "subscriber", "membership", "loyalty", "points", "rewards", "tier",
            
            # Dates and timestamps
            "order_date", "purchase_date", "shipment_date", "delivery_date", "return_date",
            "expiration", "valid_from", "valid_until", "last_login", "last_purchase",
            
            # Financial
            "subtotal", "grand_total", "list_price", "retail_price", "wholesale_price", 
            "cost", "margin", "profit", "revenue", "commission", "fee",
            "currency", "exchange_rate", "vat", "sales_tax", "duty",
            
            # User behavior
            "viewed", "clicked", "added_to_cart", "purchased", "returned",
            "favorite", "recommended", "bestseller", "trending", "popular"
            ]
            
            # Get column names from current schema
            column_names = []
            for columns in self.schema_info.values():
                column_names.extend(columns)
            
            # Combine with common terms
            all_terms = common_terms + column_names
            tokenized_terms = [term.split("_") for term in all_terms]
            
            # Train new model with better parameters
            model = Word2Vec(
                sentences=tokenized_terms, 
                vector_size=300,  # Larger vector size
                window=5, 
                min_count=1,
                workers=4,  # Use multiple cores
                epochs=20  # More training epochs
            )
        
        # Save the model for future use
        model.save(model_path)
        return model

    def compute_similarity(self, col1, col2):
        """
        Computes cosine similarity between two column names using the Word2Vec model.
        Returns a float between 0 and 1.
        """
        try:
            return self.word2vec_model.wv.similarity(col1, col2)
        except KeyError:
            return 0.0

    def find_valid_similar_column(self, columns1, columns2):
        """
        Iterates over columns in two tables and returns a tuple (join_key, similarity)
        if a pair has a similarity score meeting or exceeding SIMILARITY_THRESHOLD.
        Returns (None, 0.0) if no valid match is found.
        """
        best_key = None
        best_score = 0.0
        for col1 in columns1:
            for col2 in columns2:
                sim = self.compute_similarity(col1, col2)
                if sim > best_score and sim >= SIMILARITY_THRESHOLD:
                    best_score = sim
                    best_key = col1
        return best_key, best_score

    def generate_sql(self, table1, table2, join_key):
        """
        Generates a SQL JOIN query to merge table1 and table2 on join_key.
        Aliases columns from each table to avoid duplicate names.
        """
        cols1 = self.schema_info[table1]
        cols2 = self.schema_info[table2]

        select_cols = []
        # For table1: include join_key once; alias other columns with table1 suffix.
        for col in cols1:
            if col == join_key:
                select_cols.append(f"t1.{col} AS {col}")
            else:
                select_cols.append(f"t1.{col} AS {col}_{table1}")
        # For table2: exclude join_key (already included) and alias remaining columns.
        for col in cols2:
            if col != join_key:
                select_cols.append(f"t2.{col} AS {col}_{table2}")

        select_clause = ", ".join(select_cols)
        sql_query = f"SELECT {select_clause} FROM {table1} AS t1 JOIN {table2} AS t2 ON t1.{join_key} = t2.{join_key};"
        return sql_query

    def merge_tables(self):
        """
        Iterates over pairs of tables and attempts to determine a valid join key:
          1. Prioritizes foreign key relationships.
          2. If no FK exists, uses Word2Vec semantic matching with a threshold.
        Verifies that the join key exists in both tables, then generates and executes a SQL JOIN
        query with explicit aliasing. Merged data is stored in the target database.
        Tables with no valid join key are transferred as-is.
        """
        joined_tables = set()

        for table1, columns1 in self.schema_info.items():
            for table2, columns2 in self.schema_info.items():
                if table1 != table2 and (table1, table2) not in joined_tables:
                    join_key = None

                    # 1. Check for foreign key relationship.
                    if table1 in self.foreign_keys and self.foreign_keys[table1] in columns2:
                        join_key = self.foreign_keys[table1]
                    elif table2 in self.foreign_keys and self.foreign_keys[table2] in columns1:
                        join_key = self.foreign_keys[table2]
                    
                    # 2. If no FK, use Word2Vec semantic matching.
                    if not join_key:
                        join_key, sim_score = self.find_valid_similar_column(columns1, columns2)
                        if join_key:
                            print(f"ℹ️ Found join candidate between `{table1}` and `{table2}` using semantic matching (score: {sim_score:.2f}).")

                    # Validate that join_key exists in both tables.
                    if join_key and (join_key in columns1) and (join_key in columns2):
                        sql_query = self.generate_sql(table1, table2, join_key)
                        print(f"✅ Merging `{table1}` with `{table2}` on `{join_key}`")
                        try:
                            with source_engine.connect() as connection:
                                merged_df = pd.read_sql(sql_query, connection)
                                # The SQL query already aliases columns explicitly.
                                merged_df.to_sql(f"{table1}_{table2}_merged", target_engine, if_exists="replace", index=False)
                        except Exception as e:
                            print(f"❌ Error merging {table1} and {table2}: {e}")
                        joined_tables.add((table1, table2))
                        joined_tables.add((table2, table1))
                    else:
                        print(f"⚠️ No valid join key found between `{table1}` and `{table2}`.")

        # Transfer tables that were not merged with any other table.
        for table in self.schema_info.keys():
            if not any(table in pair for pair in joined_tables):
                try:
                    df = pd.read_sql_table(table, source_engine)
                    df.to_sql(table, target_engine, if_exists="replace", index=False)
                    print(f"⚠️ Moving `{table}` as it has no matches.")
                except Exception as e:
                    print(f"❌ Error moving table {table}: {e}")

    def store_dataset(self, path="./utils/stored_dataset.csv"):
        """Stores the mapped dataset to a CSV file."""
        self.dataset.to_csv(path)

# Function to directly copy tables from raw_db to silver_db_mapping without mapping
def copy_tables_without_mapping():
    """
    Simply copies all tables from raw_db to silver_db_mapping without any joins or transformations.
    This is used when users select "No" to automated mapping.
    """
    try:
        # Get list of tables in raw_db
        inspector = inspect(source_engine)
        tables = inspector.get_table_names()
        
        if not tables:
            print("No tables found in raw_db to copy.")
            return False
        
        # Copy each table directly
        for table in tables:
            try:
                # Read data from raw_db
                df = pd.read_sql_table(table, source_engine)
                # Write directly to silver_db_mapping
                df.to_sql(table, target_engine, if_exists='replace', index=False)
                print(f"✅ Copied table '{table}' directly to silver_db_mapping without mapping.")
            except Exception as e:
                print(f"❌ Error copying table '{table}': {e}")
        
        return True
    except Exception as e:
        print(f"❌ Error during direct copy process: {e}")
        return False

# ---------------------- MAIN PROCESS ----------------------
if __name__ == "__main__":
    # Check if mapping is needed based on a flag file or argument
    mapping_config_path = 'mapping_status.json'
    
    if os.path.exists(mapping_config_path):
        with open(mapping_config_path, 'r') as f:
            mapping_config = json.load(f)
            
        # If mapping is true, run the full mapping process
        if mapping_config.get("mapping", True):
            print("Running automated mapping process...")
            mapper = DatasetMapper()
            mapper.merge_tables()
            print("\n✅ Data mapping & migration completed successfully.")
        else:
            # If mapping is false, just copy the tables directly
            print("Skipping automated mapping as requested...")
            success = copy_tables_without_mapping()
            if success:
                print("\n✅ Tables copied to silver_db_mapping without mapping.")
            else:
                print("\n❌ Failed to copy tables without mapping.")
    else:
        # Default to running the mapping process if no config exists
        print("No mapping configuration found. Running automated mapping by default...")
        mapper = DatasetMapper()
        mapper.merge_tables()
        print("\n✅ Data mapping & migration completed successfully.")