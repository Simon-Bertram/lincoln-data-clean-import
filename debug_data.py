import pandas as pd
import numpy as np
from data_importer import DataImporter

def debug_year_values():
    """Debug script to identify problematic year_of_birth values."""
    importer = DataImporter()
    
    # Read the CSV file
    df = pd.read_csv('data/Lincoln_student_data.csv')
    
    print("Original year_of_birth column info:")
    print(f"Data type: {df['Year of birth'].dtype}")
    print(f"Sample values: {df['Year of birth'].head(10).tolist()}")
    print(f"Null count: {df['Year of birth'].isnull().sum()}")
    
    # Check for problematic values
    year_col = df['Year of birth']
    
    print("\nChecking for problematic values:")
    
    # Check for very large numbers (as strings)
    large_values = []
    for val in year_col.dropna():
        try:
            if isinstance(val, str) and val.replace('.', '').replace('-', '').isdigit():
                num_val = float(val)
                if num_val > 10000:
                    large_values.append(val)
        except:
            pass
    
    if large_values:
        print(f"Found {len(large_values)} very large values (>10000)")
        print(f"Large values: {large_values[:10]}")  # Show first 10
    
    # Check for negative numbers
    neg_values = []
    for val in year_col.dropna():
        try:
            if isinstance(val, str) and val.replace('.', '').replace('-', '').isdigit():
                num_val = float(val)
                if num_val < 0:
                    neg_values.append(val)
        except:
            pass
    
    if neg_values:
        print(f"Found {len(neg_values)} negative values")
        print(f"Negative values: {neg_values[:10]}")  # Show first 10
    
    # Test the clean_year function on all values
    print("\nTesting clean_year function on all values:")
    cleaned_values = []
    problematic_values = []
    
    for val in year_col.dropna():
        cleaned = importer.clean_year(val)
        if cleaned is not None:
            cleaned_values.append(cleaned)
            # Check if the cleaned value is out of PostgreSQL integer range
            if cleaned > 2147483647 or cleaned < -2147483648:
                problematic_values.append((val, cleaned))
    
    print(f"Total non-null values: {len(year_col.dropna())}")
    print(f"Successfully cleaned values: {len(cleaned_values)}")
    print(f"Problematic values (out of PostgreSQL range): {len(problematic_values)}")
    
    if problematic_values:
        print("Problematic values:")
        for original, cleaned in problematic_values[:10]:  # Show first 10
            print(f"  Original: {original} -> Cleaned: {cleaned}")
    
    # Check for any values that might cause issues
    print("\nChecking for any values that might cause database issues:")
    for val in year_col.dropna().head(20):
        cleaned = importer.clean_year(val)
        print(f"Original: {val} (type: {type(val)}) -> Cleaned: {cleaned} (type: {type(cleaned)})")

if __name__ == "__main__":
    debug_year_values() 