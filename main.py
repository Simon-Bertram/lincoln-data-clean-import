"""
Main script to run the data import process.
"""

import os
from pathlib import Path
from data_importer import DataImporter

def main():
    # Get the specific file path for Lincoln_student_data.csv
    data_dir = Path('data')
    lincoln_data_file = data_dir / 'Lincoln_student_data.csv'
    
    # Check if the file exists
    if not lincoln_data_file.exists():
        print(f"Error: {lincoln_data_file} not found!")
        return
        
    print(f"Processing file: {lincoln_data_file.name}")
        
    # Initialize the data importer
    importer = DataImporter()
    
    try:
        # Run the import process for Lincoln_student_data.csv
        importer.run_import(str(lincoln_data_file))
        print("\nImport completed successfully!")
        
    except Exception as e:
        print(f"\nImport failed: {str(e)}")
        print("Check the logs for more details.")

if __name__ == '__main__':
    main() 