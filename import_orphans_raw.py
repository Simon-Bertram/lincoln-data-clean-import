"""
Simple script to import cleaned_orphans_sept1.csv without any data cleaning.
This imports the data exactly as it appears in the CSV file.
"""

import pandas as pd
import psycopg2
import logging
from pathlib import Path
from config import DB_CONNECTION_STRING, validate_config

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/raw_import.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def create_orphans_table(cursor):
    """Create the civil_war_orphans table if it doesn't exist."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS civil_war_orphans_raw (
        id SERIAL PRIMARY KEY,
        family_name VARCHAR(200),
        given_name VARCHAR(200),
        aliases VARCHAR(500),
        birth_date VARCHAR(100),
        arrival VARCHAR(100),
        departure VARCHAR(100),
        scholarships VARCHAR(500),
        assignments VARCHAR(500),
        situation_1878 VARCHAR(500),
        assignment_scholarship_year VARCHAR(100),
        references TEXT,
        comments TEXT,
        birth_date_original_text TEXT,
        birth_date_uncertain VARCHAR(10),
        birth_date_clean VARCHAR(100),
        arrival_original_text TEXT,
        arrival_uncertain VARCHAR(10),
        arrival_at_lincoln VARCHAR(100),
        departure_original_text TEXT,
        departure_uncertain VARCHAR(10),
        departure_at_lincoln VARCHAR(100),
        departure_from_lincoln VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_table_sql)

def import_orphans_data(file_path, logger):
    """Import the orphans data without any cleaning."""
    try:
        # Read the CSV file
        logger.info(f"Reading file: {file_path}")
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} records from CSV")
        
        # Connect to database
        logger.info("Connecting to database...")
        with psycopg2.connect(DB_CONNECTION_STRING) as conn:
            with conn.cursor() as cursor:
                # Create table
                create_orphans_table(cursor)
                logger.info("Created/verified civil_war_orphans_raw table")
                
                # Clear existing data (optional - remove this line if you want to keep existing data)
                cursor.execute("DELETE FROM civil_war_orphans_raw")
                logger.info("Cleared existing data from table")
                
                # Insert data
                insert_sql = """
                INSERT INTO civil_war_orphans_raw (
                    family_name, given_name, aliases, birth_date, arrival, departure,
                    scholarships, assignments, situation_1878, assignment_scholarship_year,
                    references, comments, birth_date_original_text, birth_date_uncertain,
                    birth_date_clean, arrival_original_text, arrival_uncertain, arrival_at_lincoln,
                    departure_original_text, departure_uncertain, departure_at_lincoln,
                    departure_from_lincoln
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """
                
                # Insert each row
                for index, row in df.iterrows():
                    try:
                        cursor.execute(insert_sql, (
                            str(row.get('family_name', ''))[:200] if pd.notna(row.get('family_name')) else None,
                            str(row.get('given_name', ''))[:200] if pd.notna(row.get('given_name')) else None,
                            str(row.get('aliases', ''))[:500] if pd.notna(row.get('aliases')) else None,
                            str(row.get('birth_date', ''))[:100] if pd.notna(row.get('birth_date')) else None,
                            str(row.get('arrival', ''))[:100] if pd.notna(row.get('arrival')) else None,
                            str(row.get('departure', ''))[:100] if pd.notna(row.get('departure')) else None,
                            str(row.get('scholarships', ''))[:500] if pd.notna(row.get('scholarships')) else None,
                            str(row.get('assignments', ''))[:500] if pd.notna(row.get('assignments')) else None,
                            str(row.get('situation,_1878', ''))[:500] if pd.notna(row.get('situation,_1878')) else None,
                            str(row.get('assignment_/_scholarship_year', ''))[:100] if pd.notna(row.get('assignment_/_scholarship_year')) else None,
                            str(row.get('references', ''))[:1000] if pd.notna(row.get('references')) else None,
                            str(row.get('comments', ''))[:1000] if pd.notna(row.get('comments')) else None,
                            str(row.get('birth_date_original_text', ''))[:1000] if pd.notna(row.get('birth_date_original_text')) else None,
                            str(row.get('birth_date_uncertain', ''))[:10] if pd.notna(row.get('birth_date_uncertain')) else None,
                            str(row.get('birth_date', ''))[:100] if pd.notna(row.get('birth_date')) else None,
                            str(row.get('arrival_original_text', ''))[:1000] if pd.notna(row.get('arrival_original_text')) else None,
                            str(row.get('arrival_uncertain', ''))[:10] if pd.notna(row.get('arrival_uncertain')) else None,
                            str(row.get('arrival_at_lincoln', ''))[:100] if pd.notna(row.get('arrival_at_lincoln')) else None,
                            str(row.get('departure_original_text', ''))[:1000] if pd.notna(row.get('departure_original_text')) else None,
                            str(row.get('departure_uncertain', ''))[:10] if pd.notna(row.get('departure_uncertain')) else None,
                            str(row.get('departure_at_lincoln', ''))[:100] if pd.notna(row.get('departure_at_lincoln')) else None,
                            str(row.get('departure_from_lincoln', ''))[:100] if pd.notna(row.get('departure_from_lincoln')) else None
                        ))
                    except Exception as e:
                        logger.warning(f"Error inserting row {index + 1}: {str(e)}")
                        continue
                
                # Commit the transaction
                conn.commit()
                logger.info(f"Successfully imported {len(df)} records to civil_war_orphans_raw table")
                
    except Exception as e:
        logger.error(f"Import failed: {str(e)}")
        raise

def main():
    """Main function to run the raw import."""
    # Create logs directory if it doesn't exist
    Path('logs').mkdir(exist_ok=True)
    
    # Set up logging
    logger = setup_logging()
    
    try:
        # Validate configuration
        validate_config()
        logger.info("Configuration validated successfully")
        
        # Define file path
        file_path = 'data/cleaned_orphans_sept1.csv'
        
        # Check if file exists
        if not Path(file_path).exists():
            logger.error(f"File not found: {file_path}")
            print(f"Error: {file_path} not found!")
            return
        
        logger.info(f"Starting raw import of {file_path}")
        print(f"Importing {file_path} without data cleaning...")
        
        # Import the data
        import_orphans_data(file_path, logger)
        
        print("Import completed successfully!")
        print("Data has been imported to the 'civil_war_orphans_raw' table")
        print("Check logs/raw_import.log for detailed information")
        
    except Exception as e:
        logger.error(f"Import process failed: {str(e)}")
        print(f"Import failed: {str(e)}")
        print("Check logs/raw_import.log for more details")

if __name__ == '__main__':
    main()
