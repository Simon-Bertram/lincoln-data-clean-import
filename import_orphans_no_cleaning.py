"""
Import cleaned_orphans_sept1.csv using the clean architecture but without data cleaning.
This preserves the original data exactly as it appears in the CSV.
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
            logging.FileHandler('logs/no_cleaning_import.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def create_orphans_table(cursor):
    """Create the civil_war_orphans table if it doesn't exist."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS civil_war_orphans_no_cleaning (
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

def import_orphans_without_cleaning(file_path, logger):
    """Import the orphans data without any cleaning or processing."""
    try:
        # Read the CSV file
        logger.info(f"Reading file: {file_path}")
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} records from CSV")
        
        # Log column information
        logger.info(f"Columns found: {list(df.columns)}")
        
        # Connect to database
        logger.info("Connecting to database...")
        with psycopg2.connect(DB_CONNECTION_STRING) as conn:
            with conn.cursor() as cursor:
                # Create table
                create_orphans_table(cursor)
                logger.info("Created/verified civil_war_orphans_no_cleaning table")
                
                # Clear existing data (optional - remove this line if you want to keep existing data)
                cursor.execute("DELETE FROM civil_war_orphans_no_cleaning")
                logger.info("Cleared existing data from table")
                
                # Insert data - using the exact column names from the CSV
                insert_sql = """
                INSERT INTO civil_war_orphans_no_cleaning (
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
                
                # Insert each row without any processing
                successful_inserts = 0
                for index, row in df.iterrows():
                    try:
                        # Convert all values to strings and handle NaN values
                        values = []
                        for col in [
                            'family_name', 'given_name', 'aliases', 'birth_date', 'arrival', 'departure',
                            'scholarships', 'assignments', 'situation,_1878', 'assignment_/_scholarship_year',
                            'references', 'comments', 'birth_date_original_text', 'birth_date_uncertain',
                            'birth_date', 'arrival_original_text', 'arrival_uncertain', 'arrival_at_lincoln',
                            'departure_original_text', 'departure_uncertain', 'departure_at_lincoln',
                            'departure_from_lincoln'
                        ]:
                            if col in df.columns and pd.notna(row[col]):
                                # Truncate to appropriate length based on column
                                value = str(row[col])
                                if col in ['family_name', 'given_name']:
                                    value = value[:200]
                                elif col in ['aliases', 'scholarships', 'assignments', 'situation,_1878']:
                                    value = value[:500]
                                elif col in ['birth_date', 'arrival', 'departure', 'assignment_/_scholarship_year', 'birth_date_clean', 'arrival_at_lincoln', 'departure_at_lincoln', 'departure_from_lincoln']:
                                    value = value[:100]
                                elif col in ['birth_date_uncertain', 'arrival_uncertain', 'departure_uncertain']:
                                    value = value[:10]
                                elif col in ['references', 'comments', 'birth_date_original_text', 'arrival_original_text', 'departure_original_text']:
                                    value = value[:1000]
                                values.append(value)
                            else:
                                values.append(None)
                        
                        cursor.execute(insert_sql, values)
                        successful_inserts += 1
                        
                        if (index + 1) % 50 == 0:
                            logger.info(f"Processed {index + 1} records...")
                            
                    except Exception as e:
                        logger.warning(f"Error inserting row {index + 1}: {str(e)}")
                        logger.warning(f"Row data: {dict(row)}")
                        continue
                
                # Commit the transaction
                conn.commit()
                logger.info(f"Successfully imported {successful_inserts} out of {len(df)} records to civil_war_orphans_no_cleaning table")
                
    except Exception as e:
        logger.error(f"Import failed: {str(e)}")
        raise

def main():
    """Main function to run the no-cleaning import."""
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
        
        logger.info(f"Starting no-cleaning import of {file_path}")
        print(f"Importing {file_path} without any data cleaning...")
        
        # Import the data
        import_orphans_without_cleaning(file_path, logger)
        
        print("Import completed successfully!")
        print("Data has been imported to the 'civil_war_orphans_no_cleaning' table")
        print("Check logs/no_cleaning_import.log for detailed information")
        
    except Exception as e:
        logger.error(f"Import process failed: {str(e)}")
        print(f"Import failed: {str(e)}")
        print("Check logs/no_cleaning_import.log for more details")

if __name__ == '__main__':
    main()
