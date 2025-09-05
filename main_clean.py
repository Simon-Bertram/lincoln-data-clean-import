"""
Clean main script for Lincoln School Data Importer.
Uses the refactored architecture with separated concerns.
"""

import sys
from pathlib import Path
from config import DB_CONNECTION_STRING, validate_config
from src.lincoln_importer import LincolnImporter

def main():
    """Main function to run the clean import process."""
    try:
        # Validate configuration
        validate_config()
        
        # Define file paths
        data_dir = Path('data')
        lincoln_data_file = data_dir / 'Lincoln_student_data.csv'
        orphans_data_file = data_dir / 'cleaned_orphans_sept1.csv'
        
        # Check if files exist
        if not lincoln_data_file.exists():
            print(f"Error: {lincoln_data_file} not found!")
            print("Please ensure the Lincoln_student_data.csv file is in the data/ directory.")
            return
        
        if not orphans_data_file.exists():
            print(f"Error: {orphans_data_file} not found!")
            print("Please ensure the cleaned_orphans_sept1.csv file is in the data/ directory.")
            return
        
        # Create importer instance
        importer = LincolnImporter(DB_CONNECTION_STRING)
        
        # Import Lincoln student data
        print(f"Processing Lincoln student data: {lincoln_data_file.name}")
        importer.import_lincoln_data(str(lincoln_data_file))
        print("Lincoln student data import completed successfully!")
        
        # Import orphans data
        print(f"Processing civil war orphans data: {orphans_data_file.name}")
        importer.import_orphans_data(str(orphans_data_file))
        print("Civil war orphans data import completed successfully!")
        
        print("\nAll imports completed successfully!")
        
    except Exception as e:
        print(f"Import failed: {str(e)}")
        print("Check the logs for more details.")
        sys.exit(1)

if __name__ == '__main__':
    main()
