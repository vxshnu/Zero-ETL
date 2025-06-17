import torch
from transformers import T5ForConditionalGeneration, T5Tokenizer
import mysql.connector
import re

class TextToSQLGenerator:
    def __init__(self, model_path="Model", tokenizer_path="Model"):
        """
        Initialize the Text-to-SQL generator with a local T5 model.
        
        Args:
            model_path: Path to the local T5 model
            tokenizer_path: Path to the local tokenizer
        """
        try:
            # Load the tokenizer and model from local paths
            self.tokenizer = T5Tokenizer.from_pretrained(tokenizer_path)
            
            # Check if CUDA is available and set up properly
            if torch.cuda.is_available():
                self.device = torch.device("cuda")
                print(f"CUDA is available. Using GPU: {torch.cuda.get_device_name(0)}")
                
                # Load model directly to GPU
                self.model = T5ForConditionalGeneration.from_pretrained(model_path).to(self.device)
                
                # Optional: Use half-precision for faster inference on GPU
                self.model = self.model.half()
            else:
                self.device = torch.device("cpu")
                print("CUDA is not available. Using CPU.")
                self.model = T5ForConditionalGeneration.from_pretrained(model_path)
            
            # print(f"Model loaded successfully on {self.device}")
        except Exception as e:
            # print(f"Error loading model: {e}")
            raise

    def extract_schema_from_db(self):
        """
        Extract schema from MySQL silver_db and format it in the required format.
        
        Returns:
            Formatted schema string
        """
        try:
            # Connect to MySQL silver_db
            conn = mysql.connector.connect(
                host="localhost",
                user="root",
                password="root",
                database="silver_db"
            )
            cursor = conn.cursor()
            
            # Get all tables in the database
            cursor.execute("SHOW TABLES")
            tables = [table[0] for table in cursor.fetchall()]
            
            # Format for the schema output
            schema_string = ""
            
            for table in tables:
                # Get table structure
                cursor.execute(f"DESCRIBE {table}")
                columns = cursor.fetchall()
                
                schema_string += f"CREATE TABLE {table} (\n"
                column_definitions = []
                
                for column in columns:
                    col_name = column[0]
                    col_type = column[1]
                    
                    # Convert MySQL data types to simplified types (text/number)
                    if any(numeric_type in col_type.lower() for numeric_type in ['int', 'decimal', 'float', 'double', 'bit']):
                        simplified_type = "number"
                    else:
                        simplified_type = "text"
                    
                    column_definitions.append(f"    {col_name} {simplified_type}")
                
                schema_string += ",\n".join(column_definitions)
                schema_string += "\n)\n\n"
            
            cursor.close()
            conn.close()
            
            return schema_string.strip()
            
        except Exception as e:
            # print(f"Error extracting schema: {e}")
            return f"Error: {str(e)}"

    def generate_sql(self, query_text, max_length=128):
        """
        Generate SQL from natural language query with schema context.
        
        Args:
            query_text: Natural language query text
            max_length: Maximum length of generated SQL
            
        Returns:
            Generated SQL query
        """
        try:
            # Get database schema
            schema = self.extract_schema_from_db()
            
            # Format the input exactly as it was during training
            start_prompt = "Tables:\n"
            middle_prompt = "\n\nQuestion:\n"
            end_prompt = "\n\nAnswer:\n"
            
            input_text = start_prompt + schema + middle_prompt + query_text + end_prompt
            
            # Tokenize the input
            input_ids = self.tokenizer.encode(input_text, return_tensors="pt").to(self.device)
            
            # CUDA optimizations
            with torch.cuda.amp.autocast(enabled=self.device.type=="cuda"):
                with torch.no_grad():  # No need to track gradients for inference
                    # Generate SQL
                    outputs = self.model.generate(
                        input_ids=input_ids,
                        max_length=max_length,
                        num_beams=5,
                        early_stopping=True,
                        no_repeat_ngram_size=2,
                        length_penalty=1.0,
                    )
            
            # Decode the generated tokens to text
            sql_query = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
            
            # Clean up the output if needed
            if sql_query.upper().startswith("SELECT"):
                return sql_query
            else:
                # Try to extract just the SQL part using regex
                match = re.search(r'(SELECT.*?)(;|\Z)', sql_query, re.IGNORECASE | re.DOTALL)
                if match:
                    return match.group(1)
                return sql_query
                
        except Exception as e:
            # print(f"Error generating SQL: {e}")
            return f"Error: {str(e)}"

def process_query(query_text):
    """
    Process the query and return SQL.
    This function can be called from other modules.
    
    Args:
        query_text: Natural language query text
        
    Returns:
        Generated SQL query
    """
    # Lazy-load the model (only once)
    if not hasattr(process_query, "generator"):
        # Use the local model path
        process_query.generator = TextToSQLGenerator(model_path="Model", tokenizer_path="Model")
    
    return process_query.generator.generate_sql(query_text)
