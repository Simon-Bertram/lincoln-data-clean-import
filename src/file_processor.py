"""
File processing utilities for reading and parsing data files.
"""

import pandas as pd
import chardet
import logging
from pathlib import Path
from typing import Dict, Any, Optional

class FileProcessor:
    """Handles file reading and format detection."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def detect_encoding(self, file_path: str) -> str:
        """Detect the encoding of a file."""
        try:
            with open(file_path, 'rb') as file:
                raw_data = file.read()
                result = chardet.detect(raw_data)
                return result['encoding']
        except Exception as e:
            self.logger.error(f"Error detecting encoding for {file_path}: {str(e)}")
            return 'utf-8'  # Default to UTF-8
    
    def read_file(self, file_path: str) -> pd.DataFrame:
        """Read a data file and return a DataFrame."""
        try:
            # Detect encoding
            encoding = self.detect_encoding(file_path)
            self.logger.info(f"Detected encoding: {encoding} for {file_path}")
            
            # Try to read the file
            if file_path.endswith('.csv'):
                return self._read_csv_file(file_path, encoding)
            elif file_path.endswith(('.xlsx', '.xls')):
                return self._read_excel_file(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_path}")
                
        except Exception as e:
            self.logger.error(f"Error reading file {file_path}: {str(e)}")
            raise
    
    def _read_csv_file(self, file_path: str, encoding: str) -> pd.DataFrame:
        """Read a CSV file with automatic delimiter detection."""
        # Try common delimiters
        delimiters = [',', ';', '\t', '|']
        
        for delimiter in delimiters:
            try:
                df = pd.read_csv(file_path, encoding=encoding, delimiter=delimiter)
                if len(df.columns) > 1:  # Successfully parsed with multiple columns
                    self.logger.info(f"Successfully read CSV with delimiter: '{delimiter}'")
                    return df
            except Exception:
                continue
        
        # If no delimiter worked, try without specifying delimiter
        try:
            df = pd.read_csv(file_path, encoding=encoding)
            self.logger.info("Successfully read CSV with auto-detected delimiter")
            return df
        except Exception as e:
            self.logger.error(f"Failed to read CSV file: {str(e)}")
            raise
    
    def _read_excel_file(self, file_path: str) -> pd.DataFrame:
        """Read an Excel file."""
        try:
            df = pd.read_excel(file_path)
            self.logger.info("Successfully read Excel file")
            return df
        except Exception as e:
            self.logger.error(f"Failed to read Excel file: {str(e)}")
            raise
    
    def get_column_mapping(self, df: pd.DataFrame) -> Dict[str, str]:
        """Get the appropriate column mapping based on the DataFrame columns."""
        # Define column mappings for different formats
        column_mappings = {
            # Format 1: Spaces and proper capitalization
            'spaced': {
                'Census Record 1900': 'census_record_1900',
                'Indian Name': 'indian_name',
                'Tribal Name': 'indian_name',
                'Family Name': 'family_name',
                'English given name': 'english_given_name',
                'Alias': 'alias',
                'Sex': 'sex',
                'Year of birth': 'year_of_birth',
                'Arrival at Lincoln': 'arrival_at_lincoln',
                'Departure from Lincoln': 'departure_from_lincoln',
                'Nation': 'nation',
                'Band': 'band',
                'Agency': 'agency',
                'Trade': 'trade',
                'Source': 'source',
                'Comments': 'comments',
                'Cause of Death': 'cause_of_death',
                'Cemetery / Burial': 'cemetery_burial',
                'Cemetery / Burial with protective quotes': 'cemetery_burial',
                'Relevant Links': 'relevant_links'
            },
            # Format 2: CamelCase without spaces
            'camel': {
                'censusRecord1900': 'census_record_1900',
                'tribalName': 'indian_name',
                'familyName': 'family_name',
                'englishGivenName': 'english_given_name',
                'alias': 'alias',
                'sex': 'sex',
                'yearOfBirth': 'year_of_birth',
                'arrivalAtLincoln': 'arrival_at_lincoln',
                'departureFromLincoln': 'departure_from_lincoln',
                'nation': 'nation',
                'band': 'band',
                'agency': 'agency',
                'trade': 'trade',
                'source': 'source',
                'comments': 'comments',
                'causeOfDeath': 'cause_of_death',
                'cemeteryBurial': 'cemetery_burial',
                'relevantLinks': 'relevant_links'
            },
            # Format 3: Underscore separated
            'underscore': {
                'census_record_1900': 'census_record_1900',
                'indian_name': 'indian_name',
                'family_name': 'family_name',
                'english_given_name': 'english_given_name',
                'alias': 'alias',
                'sex': 'sex',
                'year_of_birth': 'year_of_birth',
                'arrival_at_lincoln': 'arrival_at_lincoln',
                'departure_from_lincoln': 'departure_from_lincoln',
                'nation': 'nation',
                'band': 'band',
                'agency': 'agency',
                'trade': 'trade',
                'source': 'source',
                'comments': 'comments',
                'cause_of_death': 'cause_of_death',
                'cemetery_burial': 'cemetery_burial',
                'relevant_links': 'relevant_links'
            }
        }
        
        # Strip trailing spaces and convert to lowercase for comparison
        df.columns = df.columns.str.strip()
        
        # Find the best matching format
        best_match = None
        best_score = 0
        
        for format_name, mapping in column_mappings.items():
            score = sum(1 for col in df.columns if col in mapping)
            if score > best_score:
                best_score = score
                best_match = mapping
        
        if best_match:
            self.logger.info(f"Using column mapping format with {best_score} matches")
            return best_match
        else:
            self.logger.warning("No column mapping found, using original column names")
            return {col: col for col in df.columns}
    
    def validate_file_exists(self, file_path: str) -> bool:
        """Validate that the file exists and is readable."""
        path = Path(file_path)
        if not path.exists():
            self.logger.error(f"File not found: {file_path}")
            return False
        
        if not path.is_file():
            self.logger.error(f"Path is not a file: {file_path}")
            return False
        
        if not path.stat().st_size > 0:
            self.logger.error(f"File is empty: {file_path}")
            return False
        
        return True
