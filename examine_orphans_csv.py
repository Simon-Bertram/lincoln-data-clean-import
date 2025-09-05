"""
Simple script to examine the cleaned_orphans_sept1.csv file structure and content.
This helps you understand the data before importing.
"""

import pandas as pd
from pathlib import Path

def examine_csv_file(file_path):
    """Examine the CSV file structure and content."""
    try:
        print(f"Examining file: {file_path}")
        print("=" * 60)
        
        # Read the CSV file
        df = pd.read_csv(file_path)
        
        print(f"Total records: {len(df)}")
        print(f"Total columns: {len(df.columns)}")
        print("\nColumn names:")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i:2d}. {col}")
        
        print("\nData types:")
        for col in df.columns:
            dtype = df[col].dtype
            non_null_count = df[col].count()
            null_count = len(df) - non_null_count
            print(f"  {col}: {dtype} (non-null: {non_null_count}, null: {null_count})")
        
        print("\nFirst 5 rows:")
        print(df.head().to_string())
        
        print("\nSample data from each column (first non-null value):")
        for col in df.columns:
            first_value = df[col].dropna().iloc[0] if not df[col].dropna().empty else "All null"
            print(f"  {col}: {first_value}")
        
        print("\nUnique values in key columns:")
        key_columns = ['family_name', 'given_name', 'birth_date', 'arrival', 'departure']
        for col in key_columns:
            if col in df.columns:
                unique_count = df[col].nunique()
                print(f"  {col}: {unique_count} unique values")
        
    except Exception as e:
        print(f"Error examining file: {str(e)}")

def main():
    """Main function to examine the CSV file."""
    file_path = 'data/cleaned_orphans_sept1.csv'
    
    # Check if file exists
    if not Path(file_path).exists():
        print(f"Error: {file_path} not found!")
        return
    
    examine_csv_file(file_path)

if __name__ == '__main__':
    main()
