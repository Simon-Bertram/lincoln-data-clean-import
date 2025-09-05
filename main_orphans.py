"""
Main script to run the civil war orphans data import process.
"""

import os
from pathlib import Path
from data_importer import DataImporter

def main():
    # Get the specific file path for civil war orphans data CSV
    data_dir = Path('data')
    orphans_data_file = data_dir / 'cleaned_orphans_sept1.csv'
    
    # Check if the file exists
    if not orphans_data_file.exists():
        print(f"Error: {orphans_data_file} not found!")
        print("Please ensure the cleaned_orphans_sept1.csv file is in the data/ directory.")
        return
        
    print(f"Processing civil war orphans file: {orphans_data_file.name}")
        
    # Initialize the data importer
    importer = DataImporter()
    
    try:
        # Run the civil war orphans import process
        importer.run_orphans_import(str(orphans_data_file))
        print("\nCivil war orphans import completed successfully!")
        
    except Exception as e:
        print(f"\nCivil war orphans import failed: {str(e)}")
        print("Check the logs for more details.")

if __name__ == '__main__':
    main()
