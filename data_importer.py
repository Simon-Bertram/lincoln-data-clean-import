so"""
Data importer module for importing Lincoln student data from Excel file into Neon Postgres database.
"""

import pandas as pd
import psycopg2
from datetime import datetime
import chardet
import logging
from typing import List, Dict, Any, Optional
import re
from pathlib import Path
from config import DB_CONNECTION_STRING
import unittest

class DataImporter:
    def __init__(self, db_connection_string: str = DB_CONNECTION_STRING):
        self.db_connection_string = db_connection_string
        self.logger = self._setup_logging()
        
    def _setup_logging(self) -> logging.Logger:
        """Set up logging configuration."""
        logger = logging.getLogger('DataImporter')
        logger.setLevel(logging.INFO)
        
        # Create logs directory if it doesn't exist
        Path('logs').mkdir(exist_ok=True)
        
        # File handler
        file_handler = logging.FileHandler('logs/import.log')
        file_handler.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Add handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger

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

    def clean_date(self, date_str: str) -> tuple[Optional[datetime], bool, Optional[str]]:
        """
        Clean and standardize date formats, handling various formats and edge cases.
        
        Args:
            date_str: The date string to clean
            
        Returns:
            tuple[Optional[datetime], bool, Optional[str]]:
                - The cleaned date or None if invalid
                - Boolean indicating if the date is uncertain
                - String describing the type of uncertainty (if any)
        """
        if pd.isna(date_str):
            return None, False, None
        
        try:
            date_str = str(date_str).strip()
            
            if not date_str or date_str.lower() in ['nan', 'none', 'null', '', 'nat']:
                return None, False, None
            
            # Handle multiple dates
            if ';' in date_str:
                date_str = date_str.split(';')[0].strip()
                return self._parse_date(date_str, True, 'multiple_dates')
            
            # Handle date ranges
            if re.match(r'\d{4}-\d{4}', date_str):
                date_str = date_str.split('-')[0]
                return self._parse_date(date_str, True, 'range')
            
            # Handle "about" or "c." approximations
            if 'about' in date_str.lower() or 'c.' in date_str.lower() or 'circa' in date_str.lower():
                return self._parse_date(date_str, True, 'approximate')
            
            # Handle "before" or "after" dates
            if 'before' in date_str.lower():
                return self._parse_date(date_str, True, 'before')
            if 'after' in date_str.lower():
                return self._parse_date(date_str, True, 'after')
            
            # Handle "early", "mid", "late" qualifiers
            if any(qualifier in date_str.lower() for qualifier in ['early', 'mid', 'late']):
                return self._parse_date(date_str, True, 'period_qualifier')
            
            # Try standard formats
            return self._parse_date(date_str, False, None)
            
        except Exception as e:
            self.logger.warning(f"Error parsing date '{date_str}': {str(e)}")
        
        self.logger.warning(f"Could not parse date: {date_str}")
        return None, False, None

    def _parse_date(self, date_str: str, is_uncertain: bool, uncertainty_type: Optional[str]) -> tuple[Optional[datetime], bool, Optional[str]]:
        """Helper method to parse dates with uncertainty tracking."""
        # Clean the date string
        date_str = str(date_str).strip()
        
        # Remove common qualifiers while preserving the date
        qualifiers = ['about', 'c.', 'circa', 'before', 'after', 'early', 'mid', 'late']
        cleaned_date_str = date_str
        for qualifier in qualifiers:
            if qualifier in date_str.lower():
                # Remove the qualifier and clean up
                cleaned_date_str = re.sub(rf'\b{qualifier}\b', '', date_str, flags=re.IGNORECASE).strip()
                break
        
        # Try standard date formats
        date_formats = [
            '%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y',
            '%Y-%m', '%Y/%m', '%Y'
        ]
        
        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(cleaned_date_str, fmt)
                if 1800 <= parsed_date.year <= 2000:
                    return parsed_date, is_uncertain, uncertainty_type
            except ValueError:
                continue
        
        # Try to extract just the year
        year_match = re.search(r'\d{4}', cleaned_date_str)
        if year_match:
            year = int(year_match.group())
            if 1800 <= year <= 2000:
                return datetime(year, 1, 1), is_uncertain, uncertainty_type
        
        # Try pandas parsing for more flexible date formats
        try:
            parsed_date = pd.to_datetime(cleaned_date_str, errors='coerce')
            if pd.isna(parsed_date):
                return None, is_uncertain, uncertainty_type
            if isinstance(parsed_date, pd.Timestamp):
                parsed_date = parsed_date.to_pydatetime()
            if 1800 <= parsed_date.year <= 2000:
                return parsed_date, is_uncertain, uncertainty_type
        except (ValueError, TypeError):
            pass
        
        return None, False, None

    def clean_name(self, name: str) -> Optional[str]:
        """Clean and standardize names."""
        if pd.isna(name):
            return None
            
        # Remove special characters but preserve spaces, hyphens, and periods
        cleaned = re.sub(r'[^\w\s\-\.]', '', str(name))
        return cleaned.strip() if cleaned else None

    def clean_year(self, year: Any) -> Optional[int]:
        """
        Clean and standardize year values, handling various formats and approximations.
        
        Args:
            year: The year value to clean
            
        Returns:
            Optional[int]: The cleaned year or None if invalid
        """
        if pd.isna(year):
            return None
        
        try:
            # Handle numeric types directly first
            if isinstance(year, (int, float)):
                if pd.isna(year):
                    return None
                # Check for infinity and NaN
                if year == float('inf') or year == float('-inf') or year != year:  # NaN check
                    return None
                # Convert to float and check if it's a whole number
                year_float = float(year)
                if year_float.is_integer():
                    year_int = int(year_float)
                    if 1800 <= year_int <= 2000:
                        return year_int
                return None
            
            # Convert to string for consistent handling
            year_str = str(year).strip().lower()
            
            # Handle empty or invalid values
            if not year_str or year_str == 'nan' or year_str in ['inf', '-inf', 'infinity', '-infinity']:
                return None
            
            # Handle age-based entries
            if 'age' in year_str:
                # Try to extract year from combined entries
                year_match = re.search(r'\d{4}', year_str)
                if year_match:
                    year_int = int(year_match.group())
                    if 1800 <= year_int <= 2000:
                        return year_int
                
                # If no year found, try to calculate from age
                age_match = re.search(r'age\s*(\d+)', year_str)
                if age_match:
                    age = int(age_match.group(1))
                    # Assume age is from 1900 census
                    estimated_year = 1900 - age
                    if 1800 <= estimated_year <= 2000:
                        self.logger.debug(f"Estimated birth year {estimated_year} from age {age}")
                        return estimated_year
                return None
            
            # Handle "about" or "c." approximations
            if 'about' in year_str or 'c.' in year_str:
                year_match = re.search(r'\d{4}', year_str)
                if year_match:
                    year_int = int(year_match.group())
                    if 1800 <= year_int <= 2000:
                        return year_int
                return None
            
            # Handle ranges
            if ' or ' in year_str:
                years = year_str.split(' or ')
                year_match = re.search(r'\d{4}', years[0])
                if year_match:
                    year_int = int(year_match.group())
                    if 1800 <= year_int <= 2000:
                        return year_int
                return None
            
            # Handle year ranges with slash
            if '/' in year_str:
                base_year = year_str.split('/')[0]
                if re.match(r'\d{4}', base_year):
                    year_int = int(base_year)
                    if 1800 <= year_int <= 2000:
                        return year_int
                return None
            
            # Handle full dates
            if re.match(r'\d{4}-\d{2}-\d{2}', year_str):
                year_int = int(year_str.split('-')[0])
                if 1800 <= year_int <= 2000:
                    return year_int
                return None
            
            # Handle simple year format (including floats like "1890.0")
            if re.match(r'\d{4}(?:\.0)?$', year_str):
                year_int = int(float(year_str))
                if 1800 <= year_int <= 2000:
                    return year_int
            
        except (ValueError, AttributeError, TypeError) as e:
            self.logger.warning(f"Error parsing year '{year}': {str(e)}")
        
        self.logger.warning(f"Could not parse year: {year}")
        return None

    def check_column_names(self, df: pd.DataFrame) -> bool:
        """
        Check if the DataFrame has all required columns and if they match the expected format.
        
        Args:
            df (pd.DataFrame): The DataFrame to check
            
        Returns:
            bool: True if all columns are valid, False otherwise
            
        Raises:
            ValueError: If required columns are missing or have incorrect names
        """
        # Define column mapping from CSV to database
        column_mapping = {
            'Census Record 1900': 'census_record_1900',
            'Indian Name': 'indian_name',
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
            'Cause of Death': 'cause_of_death',  # Removed trailing space
            'Cemetery / Burial': 'cemetery_burial',
            'Relevant Links': 'relevant_links'
        }
        
        # Check for missing columns
        missing_columns = set(column_mapping.keys()) - set(df.columns)
        if missing_columns:
            self.logger.error(f"Missing required columns: {missing_columns}")
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Check for extra columns
        extra_columns = set(df.columns) - set(column_mapping.keys())
        if extra_columns:
            self.logger.warning(f"Extra columns found: {extra_columns}")
        
        # Rename columns according to mapping
        df.rename(columns=column_mapping, inplace=True)
        
        # Define expected types for each column
        expected_types = {
            'census_record_1900': str,
            'indian_name': str,
            'family_name': str,
            'english_given_name': str,
            'alias': str,
            'sex': str,
            'year_of_birth': (int, float),
            'arrival_at_lincoln': (str, pd.Timestamp),
            'departure_from_lincoln': (str, pd.Timestamp),
            'nation': str,
            'band': str,
            'agency': str,
            'trade': str,
            'source': str,
            'comments': str,
            'cause_of_death': str,
            'cemetery_burial': str,
            'relevant_links': str
        }
        
        # Check column types after renaming
        for column, expected_type in expected_types.items():
            if column in df.columns:
                # Handle nullable columns
                non_null_values = df[column].dropna()
                if len(non_null_values) > 0:
                    actual_type = type(non_null_values.iloc[0])
                    if not isinstance(actual_type, expected_type):
                        self.logger.error(
                            f"Column '{column}' has incorrect type. "
                            f"Expected {expected_type}, got {actual_type}"
                        )
                        raise ValueError(
                            f"Column '{column}' has incorrect type. "
                            f"Expected {expected_type}, got {actual_type}"
                        )
        
        self.logger.info("Column name and type validation successful")
        return True

    def validate_and_clean_dataframe(self, df: pd.DataFrame, file_path: str) -> pd.DataFrame:
        """
        Validate and clean the DataFrame to ensure it meets the required format and data types.
        
        Args:
            df (pd.DataFrame): The DataFrame to validate and clean
            file_path (str): Path to the CSV file being processed
            
        Returns:
            pd.DataFrame: Cleaned and validated DataFrame
            
        Raises:
            ValueError: If the DataFrame cannot be validated or cleaned
        """
        try:
            self.logger.info(f"Validating and cleaning data from {file_path}")
            
            # Clean column names - remove quotes and extra spaces
            df.columns = df.columns.str.strip().str.replace('"', '').str.replace("'", '')
            
            # Remove unnamed columns
            df = df.loc[:, ~df.columns.str.contains('^Unnamed:', na=False)].copy()
            
            # Define column mappings for different formats
            column_mappings = {
                # Format 1: Spaces and proper capitalization
                'spaced': {
                    'Census Record 1900': 'census_record_1900',
                    'Indian Name': 'indian_name',
                    'Tribal Name': 'indian_name',  # Added this mapping
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
                    'Cemetery / Burial with protective quotes': 'cemetery_burial',  # Added this mapping
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
                },
                # Format 4: Short names
                'short': {
                    'census': 'census_record_1900',
                    'tribal': 'indian_name',
                    'family': 'family_name',
                    'english': 'english_given_name',
                    'alias': 'alias',
                    'sex': 'sex',
                    'birth': 'year_of_birth',
                    'arrival': 'arrival_at_lincoln',
                    'departure': 'departure_from_lincoln',
                    'nation': 'nation',
                    'band': 'band',
                    'agency': 'agency',
                    'trade': 'trade',
                    'source': 'source',
                    'comments': 'comments',
                    'death': 'cause_of_death',
                    'burial': 'cemetery_burial',
                    'links': 'relevant_links'
                },
                # Format 5: Rewritten_Data.csv format
                'reworked': {
                    '0': 'indian_name',
                    '1': 'family_name',
                    '2': 'english_given_name',
                    '3': 'alias',
                    '4': 'sex',
                    '5': 'year_of_birth',
                    '6': 'arrival_at_lincoln',
                    '7': 'departure_from_lincoln',
                    '8': 'nation',
                    '9': 'band',
                    '10': 'agency',
                    '11': 'trade',
                    '12': 'source',
                    '13': 'comments',
                    '14': 'cause_of_death',
                    '15': 'cemetery_burial',
                    '16': 'relevant_links'
                }
            }
            
            # Strip trailing spaces and convert to lowercase for comparison
            df.columns = df.columns.str.strip()
            df_lower = df.copy()
            df_lower.columns = df_lower.columns.str.lower()
            
            # Try to determine which format the file uses
            current_mapping = None
            format_used = None
            best_match_count = 0
            
            # First try the reworked format since we know it's from Rewritten_Data.csv
            if file_path.endswith('Rewritten_Data.csv'):
                current_mapping = column_mappings['reworked']
                format_used = 'reworked'
                best_match_count = len(df.columns)
            else:
                for format_name, mapping in column_mappings.items():
                    # Create lowercase version of mapping keys for comparison
                    mapping_lower = {k.lower(): v for k, v in mapping.items()}
                    
                    # Count matches
                    match_count = sum(1 for col in df_lower.columns if col in mapping_lower)
                    
                    if match_count > best_match_count:
                        best_match_count = match_count
                        current_mapping = mapping
                        format_used = format_name
            
            if best_match_count == 0:
                # If no matches found, try partial matching
                for format_name, mapping in column_mappings.items():
                    mapping_lower = {k.lower(): v for k, v in mapping.items()}
                    partial_matches = {}
                    
                    for col in df_lower.columns:
                        for map_key in mapping_lower:
                            if map_key in col or col in map_key:
                                partial_matches[col] = mapping_lower[map_key]
                    
                    if len(partial_matches) > best_match_count:
                        best_match_count = len(partial_matches)
                        current_mapping = {k: v for k, v in mapping.items() if v in partial_matches.values()}
                        format_used = format_name
            
            if best_match_count == 0:
                missing_columns = set(column_mappings['spaced'].keys()) - set(df.columns)
                self.logger.error(f"Missing required columns: {missing_columns}")
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            # Log the mapping being used
            self.logger.info(f"Using column mapping format: {format_used}")
            self.logger.info(f"Found {best_match_count} matching columns")
            
            # Rename columns according to mapping
            df = df.rename(columns=current_mapping)
            
            # Add missing columns with default values
            for col in column_mappings['spaced'].values():
                if col not in df.columns:
                    df[col] = None
                    self.logger.warning(f"Added missing column: {col}")
            
            # Add uncertainty columns
            df['year_of_birth_uncertain'] = False
            df['year_of_birth_uncertainty_type'] = None
            df['year_of_birth_original_text'] = None
            df['arrival_at_lincoln_uncertain'] = False
            df['arrival_at_lincoln_uncertainty_type'] = None
            df['arrival_at_lincoln_original_text'] = None
            df['departure_from_lincoln_uncertain'] = False
            df['departure_from_lincoln_uncertainty_type'] = None
            df['departure_from_lincoln_original_text'] = None
            
            # Define expected types and cleaning functions for each column
            column_specs = {
                'census_record_1900': {'type': str, 'clean': lambda x: str(x).strip() if pd.notna(x) else None},
                'indian_name': {'type': str, 'clean': self.clean_name},
                'family_name': {'type': str, 'clean': self.clean_name},
                'english_given_name': {'type': str, 'clean': self.clean_name},
                'alias': {'type': str, 'clean': self.clean_name},
                'sex': {'type': str, 'clean': lambda x: str(x).strip().upper() if pd.notna(x) else None},
                'year_of_birth': {
                    'type': (int, float, pd.Int64Dtype),
                    'clean': lambda x: self.clean_year(x) if pd.notna(x) else None
                },
                'arrival_at_lincoln': {
                    'type': (str, pd.Timestamp, datetime),
                    'clean': self.clean_date
                },
                'departure_from_lincoln': {
                    'type': (str, pd.Timestamp, datetime),
                    'clean': self.clean_date
                },
                'nation': {'type': str, 'clean': lambda x: str(x).strip() if pd.notna(x) else None},
                'band': {'type': str, 'clean': lambda x: str(x).strip() if pd.notna(x) else None},
                'agency': {'type': str, 'clean': lambda x: str(x).strip() if pd.notna(x) else None},
                'trade': {'type': str, 'clean': lambda x: str(x).strip() if pd.notna(x) else None},
                'source': {'type': str, 'clean': lambda x: str(x).strip() if pd.notna(x) else None},
                'comments': {'type': str, 'clean': lambda x: str(x).strip() if pd.notna(x) else None},
                'cause_of_death': {'type': str, 'clean': lambda x: str(x).strip() if pd.notna(x) else None},
                'cemetery_burial': {'type': str, 'clean': lambda x: str(x).strip() if pd.notna(x) else None},
                'relevant_links': {'type': str, 'clean': lambda x: str(x).strip() if pd.notna(x) else None}
            }
            
            # Clean and validate each column
            for column, spec in column_specs.items():
                if column in df.columns:
                    # Special handling for date columns to track uncertainty
                    if column in ['arrival_at_lincoln', 'departure_from_lincoln']:
                        # Apply date cleaning and track uncertainty
                        cleaned_dates = []
                        uncertainties = []
                        uncertainty_types = []
                        original_texts = []
                        
                        for value in df[column]:
                            # Store original text before cleaning
                            original_text = str(value) if pd.notna(value) else None
                            original_texts.append(original_text)
                            
                            cleaned_date, is_uncertain, uncertainty_type = spec['clean'](value)
                            cleaned_dates.append(cleaned_date)
                            uncertainties.append(is_uncertain)
                            uncertainty_types.append(uncertainty_type)
                        
                        df.loc[:, column] = cleaned_dates
                        df.loc[:, f'{column}_uncertain'] = uncertainties
                        df.loc[:, f'{column}_uncertainty_type'] = uncertainty_types
                        df.loc[:, f'{column}_original_text'] = original_texts
                    elif column == 'year_of_birth':
                        # Special handling for year of birth to track age-based estimates
                        cleaned_years = []
                        uncertainties = []
                        uncertainty_types = []
                        original_texts = []
                        
                        for value in df[column]:
                            # Store original text before cleaning
                            original_text = str(value) if pd.notna(value) else None
                            original_texts.append(original_text)
                            
                            original_value = value
                            cleaned_year = spec['clean'](value)
                            is_uncertain = False
                            uncertainty_type = None
                            
                            # Check if this was estimated from age
                            if pd.notna(original_value) and 'age' in str(original_value).lower():
                                is_uncertain = True
                                uncertainty_type = 'estimated_from_age'
                            
                            cleaned_years.append(cleaned_year)
                            uncertainties.append(is_uncertain)
                            uncertainty_types.append(uncertainty_type)
                        
                        df.loc[:, column] = cleaned_years
                        df.loc[:, f'{column}_uncertain'] = uncertainties
                        df.loc[:, f'{column}_uncertainty_type'] = uncertainty_types
                        df.loc[:, f'{column}_original_text'] = original_texts
                    else:
                        # Clean the data for non-date columns
                        df.loc[:, column] = df[column].apply(spec['clean'])
                    
                    # Skip type checking for string columns
                    if spec['type'] == str:
                        continue
                    
                    # Validate the type
                    non_null_values = df[column].dropna()
                    if len(non_null_values) > 0:
                        actual_type = type(non_null_values.iloc[0])
                        expected_type = spec['type']
                        
                        if isinstance(expected_type, tuple):
                            if not any(isinstance(actual_type, t) for t in expected_type):
                                try:
                                    # Try to convert to the first expected type
                                    df.loc[:, column] = pd.to_numeric(df[column], errors='coerce')
                                except Exception as e:
                                    self.logger.error(
                                        f"Failed to convert column '{column}' to numeric: {str(e)}"
                                    )
                                    raise ValueError(
                                        f"Column '{column}' has incorrect type and cannot be converted"
                                    )
                        elif not isinstance(actual_type, expected_type):
                            self.logger.warning(
                                f"Column '{column}' has type {actual_type}, "
                                f"expected {expected_type}. Attempting conversion."
                            )
                            # Try to convert to the expected type
                            try:
                                df.loc[:, column] = df[column].astype(expected_type)
                            except Exception as e:
                                self.logger.error(
                                    f"Failed to convert column '{column}' to {expected_type}: {str(e)}"
                                )
                                raise ValueError(
                                    f"Column '{column}' has incorrect type and cannot be converted"
                                )
            
            self.logger.info(f"Successfully validated and cleaned data from {file_path}")
            
            # Add data quality metrics
            quality_metrics = {
                'total_rows': len(df),
                'null_counts': df.isnull().sum().to_dict(),
                'parsed_dates': {
                    'year_of_birth': df['year_of_birth'].notna().sum(),
                    'arrival_at_lincoln': df['arrival_at_lincoln'].notna().sum(),
                    'departure_from_lincoln': df['departure_from_lincoln'].notna().sum()
                }
            }
            
            self.logger.info(f"Data quality metrics for {file_path}:")
            self.logger.info(f"Total rows: {quality_metrics['total_rows']}")
            self.logger.info(f"Null counts: {quality_metrics['null_counts']}")
            self.logger.info(f"Parsed dates: {quality_metrics['parsed_dates']}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error validating and cleaning data from {file_path}: {str(e)}")
            raise

    def process_file(self, file_path: str) -> pd.DataFrame:
        """Process a single file (CSV or Excel)."""
        try:
            self.logger.info(f"Processing file: {file_path}")
            
            # Check file extension to determine how to read it
            file_extension = Path(file_path).suffix.lower()
            
            if file_extension == '.xlsx':
                # Read Excel file
                try:
                    df = pd.read_excel(
                        file_path,
                        engine='openpyxl',
                        sheet_name=0  # Read first sheet
                    )
                    self.logger.info(f"Successfully read Excel file: {file_path}")
                except Exception as e:
                    self.logger.error(f"Failed to read Excel file {file_path}: {str(e)}")
                    raise ValueError(f"Could not read Excel file {file_path}: {str(e)}")
                    
            elif file_extension == '.csv':
                # Detect encoding and delimiter for CSV files
                encoding = self.detect_encoding(file_path)
                
                # Try to read the file with different delimiters
                delimiters = ['|', ',', '\t']
                df = None
                
                for delimiter in delimiters:
                    try:
                        df = pd.read_csv(
                            file_path,
                            encoding=encoding,
                            delimiter=delimiter,
                            quotechar='"',
                            on_bad_lines='warn',
                            skipinitialspace=True
                        )
                        # If we successfully read the file and got more than one column, use this delimiter
                        if len(df.columns) > 1:
                            self.logger.info(f"Successfully read CSV file with delimiter: {delimiter}")
                            break
                    except Exception as e:
                        self.logger.warning(f"Failed to read CSV file with delimiter {delimiter}: {str(e)}")
                        continue
                
                if df is None or len(df.columns) <= 1:
                    raise ValueError(f"Could not read CSV file {file_path} with any of the supported delimiters")
            else:
                raise ValueError(f"Unsupported file format: {file_extension}. Only .xlsx and .csv files are supported.")
            
            # Validate and clean the DataFrame
            df = self.validate_and_clean_dataframe(df, file_path)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {str(e)}")
            raise

    def create_database_schema(self) -> None:
        """Create the database schema if it doesn't exist."""
        try:
            with psycopg2.connect(self.db_connection_string) as conn:
                with conn.cursor() as cur:
                    # Drop existing table and indexes
                    cur.execute("""
                        DROP TABLE IF EXISTS students CASCADE;
                    """)
                    
                    # Create table with all required columns
                    cur.execute("""
                        CREATE TABLE students (
                            id SERIAL PRIMARY KEY,
                            census_record_1900 VARCHAR(100),
                            indian_name VARCHAR(500),
                            family_name VARCHAR(200),
                            english_given_name VARCHAR(200),
                            alias VARCHAR(200),
                            sex CHAR(1),
                            year_of_birth INTEGER,
                            year_of_birth_uncertain BOOLEAN DEFAULT FALSE,
                            year_of_birth_uncertainty_type VARCHAR(50),  -- 'about', 'circa', 'range', 'estimated_from_age', etc.
                            year_of_birth_original_text TEXT,  -- Store original text for reference
                            arrival_at_lincoln DATE,
                            arrival_at_lincoln_uncertain BOOLEAN DEFAULT FALSE,
                            arrival_at_lincoln_uncertainty_type VARCHAR(50),
                            arrival_at_lincoln_original_text TEXT,  -- Store original text for reference
                            departure_from_lincoln DATE,
                            departure_from_lincoln_uncertain BOOLEAN DEFAULT FALSE,
                            departure_from_lincoln_uncertainty_type VARCHAR(50),
                            departure_from_lincoln_original_text TEXT,  -- Store original text for reference
                            nation VARCHAR(200),
                            band VARCHAR(200),
                            agency VARCHAR(200),
                            trade VARCHAR(200),
                            source TEXT,
                            comments TEXT,
                            cause_of_death TEXT,
                            cemetery_burial VARCHAR(500),
                            relevant_links TEXT,
                            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                        );
                        
                        CREATE INDEX idx_family_name ON students(family_name);
                        CREATE INDEX idx_nation ON students(nation);
                        CREATE INDEX idx_year_of_birth ON students(year_of_birth);
                        CREATE INDEX idx_arrival_date ON students(arrival_at_lincoln);
                        CREATE INDEX idx_departure_date ON students(departure_from_lincoln);
                    """)
                conn.commit()
                self.logger.info("Database schema recreated successfully")
                
        except Exception as e:
            self.logger.error(f"Error creating database schema: {str(e)}")
            raise

    def import_to_db(self, df: pd.DataFrame) -> None:
        """Import data to the database."""
        try:
            # Add uncertainty columns if they don't exist
            if 'year_of_birth_uncertain' not in df.columns:
                df['year_of_birth_uncertain'] = False
            if 'year_of_birth_uncertainty_type' not in df.columns:
                df['year_of_birth_uncertainty_type'] = None
            if 'year_of_birth_original_text' not in df.columns:
                df['year_of_birth_original_text'] = None
            if 'arrival_at_lincoln_uncertain' not in df.columns:
                df['arrival_at_lincoln_uncertain'] = False
            if 'arrival_at_lincoln_uncertainty_type' not in df.columns:
                df['arrival_at_lincoln_uncertainty_type'] = None
            if 'arrival_at_lincoln_original_text' not in df.columns:
                df['arrival_at_lincoln_original_text'] = None
            if 'departure_from_lincoln_uncertain' not in df.columns:
                df['departure_from_lincoln_uncertain'] = False
            if 'departure_from_lincoln_uncertainty_type' not in df.columns:
                df['departure_from_lincoln_uncertainty_type'] = None
            if 'departure_from_lincoln_original_text' not in df.columns:
                df['departure_from_lincoln_original_text'] = None

            with psycopg2.connect(self.db_connection_string) as conn:
                with conn.cursor() as cur:
                    for index, row in df.iterrows():
                        try:
                            # Validate and clean year_of_birth before database insertion
                            year_of_birth = row.get('year_of_birth')
                            if pd.notna(year_of_birth):
                                try:
                                    # Ensure it's a valid integer in range
                                    year_int = int(year_of_birth)
                                    if not (1800 <= year_int <= 2000):
                                        self.logger.warning(f"Year of birth {year_int} out of range (1800-2000), setting to None")
                                        year_of_birth = None
                                except (ValueError, TypeError, OverflowError):
                                    self.logger.warning(f"Invalid year of birth value: {year_of_birth}, setting to None")
                                    year_of_birth = None
                            else:
                                year_of_birth = None
                            
                            # Handle date columns properly - convert None to NULL for database
                            arrival_date = row.get('arrival_at_lincoln')
                            departure_date = row.get('departure_from_lincoln')
                            
                            # Truncate overly long strings to prevent database errors
                            def truncate_string(value, max_length):
                                if value is None or pd.isna(value):
                                    return None
                                str_value = str(value)
                                if len(str_value) > max_length:
                                    self.logger.warning(f"Truncating string from {len(str_value)} to {max_length} characters: {str_value[:50]}...")
                                    return str_value[:max_length]
                                return str_value
                            
                            # Truncate string fields according to database schema limits
                            census_record_1900 = truncate_string(row.get('census_record_1900'), 100)
                            indian_name = truncate_string(row.get('indian_name'), 500)
                            family_name = truncate_string(row.get('family_name'), 200)
                            english_given_name = truncate_string(row.get('english_given_name'), 200)
                            alias = truncate_string(row.get('alias'), 200)
                            sex = truncate_string(row.get('sex'), 1)
                            nation = truncate_string(row.get('nation'), 200)
                            band = truncate_string(row.get('band'), 200)
                            agency = truncate_string(row.get('agency'), 200)
                            trade = truncate_string(row.get('trade'), 200)
                            cemetery_burial = truncate_string(row.get('cemetery_burial'), 500)
                            
                            # TEXT fields don't need truncation
                            source = row.get('source')
                            comments = row.get('comments')
                            cause_of_death = row.get('cause_of_death')
                            relevant_links = row.get('relevant_links')
                            
                            # Convert pandas NaT to None for proper NULL handling
                            if pd.isna(arrival_date) or arrival_date == 'NaT' or str(arrival_date).lower() == 'nat':
                                arrival_date = None
                            elif isinstance(arrival_date, (int, float)):
                                # Handle timestamp conversion (Unix timestamp)
                                try:
                                    # Validate timestamp range (reasonable dates between 1800-2100)
                                    min_timestamp = pd.Timestamp('1800-01-01').timestamp()
                                    max_timestamp = pd.Timestamp('2100-01-01').timestamp()
                                    
                                    if arrival_date > 1e10:  # Likely milliseconds
                                        # Convert to seconds for validation
                                        arrival_date_seconds = arrival_date / 1000
                                        if min_timestamp <= arrival_date_seconds <= max_timestamp:
                                            arrival_date = pd.to_datetime(arrival_date, unit='ms').date()
                                        else:
                                            self.logger.warning(f"Invalid arrival_date timestamp {arrival_date} (milliseconds) - out of range")
                                            arrival_date = None
                                    else:  # Likely seconds
                                        if min_timestamp <= arrival_date <= max_timestamp:
                                            arrival_date = pd.to_datetime(arrival_date, unit='s').date()
                                        else:
                                            self.logger.warning(f"Invalid arrival_date timestamp {arrival_date} (seconds) - out of range")
                                            arrival_date = None
                                except (ValueError, TypeError, OverflowError):
                                    self.logger.warning(f"Could not convert arrival_date timestamp {arrival_date} to date")
                                    arrival_date = None
                            elif isinstance(arrival_date, datetime):
                                arrival_date = arrival_date.date()
                            elif isinstance(arrival_date, pd.Timestamp):
                                arrival_date = arrival_date.date()
                            elif isinstance(arrival_date, str):
                                # Try to parse string date
                                try:
                                    arrival_date = pd.to_datetime(arrival_date).date()
                                except (ValueError, TypeError):
                                    self.logger.warning(f"Could not parse arrival_date string: {arrival_date}")
                                    arrival_date = None
                            
                            if pd.isna(departure_date) or departure_date == 'NaT' or str(departure_date).lower() == 'nat':
                                departure_date = None
                            elif isinstance(departure_date, (int, float)):
                                # Handle timestamp conversion (Unix timestamp)
                                try:
                                    # Validate timestamp range (reasonable dates between 1800-2100)
                                    min_timestamp = pd.Timestamp('1800-01-01').timestamp()
                                    max_timestamp = pd.Timestamp('2100-01-01').timestamp()
                                    
                                    if departure_date > 1e10:  # Likely milliseconds
                                        # Convert to seconds for validation
                                        departure_date_seconds = departure_date / 1000
                                        if min_timestamp <= departure_date_seconds <= max_timestamp:
                                            departure_date = pd.to_datetime(departure_date, unit='ms').date()
                                        else:
                                            self.logger.warning(f"Invalid departure_date timestamp {departure_date} (milliseconds) - out of range")
                                            departure_date = None
                                    else:  # Likely seconds
                                        if min_timestamp <= departure_date <= max_timestamp:
                                            departure_date = pd.to_datetime(departure_date, unit='s').date()
                                        else:
                                            self.logger.warning(f"Invalid departure_date timestamp {departure_date} (seconds) - out of range")
                                            departure_date = None
                                except (ValueError, TypeError, OverflowError):
                                    self.logger.warning(f"Could not convert departure_date timestamp {departure_date} to date")
                                    departure_date = None
                            elif isinstance(departure_date, datetime):
                                departure_date = departure_date.date()
                            elif isinstance(departure_date, pd.Timestamp):
                                departure_date = departure_date.date()
                            elif isinstance(departure_date, str):
                                # Try to parse string date
                                try:
                                    departure_date = pd.to_datetime(departure_date).date()
                                except (ValueError, TypeError):
                                    self.logger.warning(f"Could not parse departure_date string: {departure_date}")
                                    departure_date = None
                            
                            cur.execute("""
                                INSERT INTO students (
                                    census_record_1900, indian_name, family_name, 
                                    english_given_name, alias, sex, 
                                    year_of_birth, year_of_birth_uncertain, year_of_birth_uncertainty_type, year_of_birth_original_text,
                                    arrival_at_lincoln, arrival_at_lincoln_uncertain, arrival_at_lincoln_uncertainty_type, arrival_at_lincoln_original_text,
                                    departure_from_lincoln, departure_from_lincoln_uncertain, departure_from_lincoln_uncertainty_type, departure_from_lincoln_original_text,
                                    nation, band, agency, trade, source,
                                    comments, cause_of_death, cemetery_burial,
                                    relevant_links
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                )
                            """, (
                                census_record_1900,
                                indian_name,
                                family_name,
                                english_given_name,
                                alias,
                                sex,
                                year_of_birth,
                                row.get('year_of_birth_uncertain'),
                                row.get('year_of_birth_uncertainty_type'),
                                row.get('year_of_birth_original_text'),
                                arrival_date,
                                row.get('arrival_at_lincoln_uncertain'),
                                row.get('arrival_at_lincoln_uncertainty_type'),
                                row.get('arrival_at_lincoln_original_text'),
                                departure_date,
                                row.get('departure_from_lincoln_uncertain'),
                                row.get('departure_from_lincoln_uncertainty_type'),
                                row.get('departure_from_lincoln_original_text'),
                                nation,
                                band,
                                agency,
                                trade,
                                source,
                                comments,
                                cause_of_death,
                                cemetery_burial,
                                relevant_links
                            ))
                        except Exception as e:
                            self.logger.error(f"Error inserting row {index}: {str(e)}")
                            self.logger.error(f"Row data: {row.to_dict()}")
                            raise  # Re-raise to see the full error
                    conn.commit()
                    self.logger.info(f"Successfully imported {len(df)} records")
                
        except Exception as e:
            self.logger.error(f"Error importing to database: {str(e)}")
            raise

    def run_import(self, file_path: str) -> None:
        """Run the import process for the Lincoln student data file."""
        try:
            self.logger.info(f"Starting import process for: {file_path}")
            
            # Create database schema
            self.create_database_schema()
            
            # Process the file
            df = self.process_file(file_path)
            self.import_to_db(df)
            
            self.logger.info("Import process completed successfully")
                    
        except Exception as e:
            self.logger.error(f"Import process failed: {str(e)}")
            raise

    def create_orphans_database_schema(self) -> None:
        """Create the civil war orphans database schema if it doesn't exist."""
        try:
            with psycopg2.connect(self.db_connection_string) as conn:
                with conn.cursor() as cur:
                    # Drop existing table and indexes
                    cur.execute("""
                        DROP TABLE IF EXISTS civil_war_orphans CASCADE;
                    """)
                    
                    # Create table with all required columns
                    cur.execute("""
                        CREATE TABLE civil_war_orphans (
                            id SERIAL PRIMARY KEY,
                            family_name TEXT,
                            given_name TEXT,
                            aliases TEXT,
                            birth_date DATETIME,
                            arrival DATETIME,
                            departure DATETIME,
                            scholarships TEXT,
                            assignments TEXT,
                            situation_1878 TEXT,
                            assignment_scholarship_year TEXT,
                            references TEXT,
                            comments TEXT,
                            birth_date_original_text TEXT,
                            birth_date_uncertain BOOLEAN DEFAULT FALSE,
                            arrival_original_text TEXT,
                            arrival_uncertain BOOLEAN DEFAULT FALSE,
                            arrival_at_lincoln DATETIME,
                            departure_original_text TEXT,
                            departure_uncertain BOOLEAN DEFAULT FALSE,
                            departure_at_lincoln DATETIME,
                            departure_from_lincoln DATETIME,
                            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                        );
                        
                        CREATE INDEX idx_civil_war_orphans_family_name ON civil_war_orphans(family_name);
                        CREATE INDEX idx_civil_war_orphans_given_name ON civil_war_orphans(given_name);
                        CREATE INDEX idx_civil_war_orphans_birth_date ON civil_war_orphans(birth_date);
                        CREATE INDEX idx_civil_war_orphans_arrival ON civil_war_orphans(arrival);
                        CREATE INDEX idx_civil_war_orphans_departure ON civil_war_orphans(departure);
                    """)
                conn.commit()
                self.logger.info("Civil war orphans database schema created successfully")
                
        except Exception as e:
            self.logger.error(f"Error creating civil war orphans database schema: {str(e)}")
            raise

    def process_orphans_file(self, file_path: str) -> pd.DataFrame:
        """Process the orphans CSV file and return a cleaned DataFrame."""
        try:
            # Detect file encoding
            encoding = self.detect_encoding(file_path)
            self.logger.info(f"Detected encoding: {encoding}")
            
            # Read the CSV file
            df = pd.read_csv(file_path, encoding=encoding)
            self.logger.info(f"Loaded {len(df)} records from {file_path}")
            
            # Clean column names
            df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('/', '_')
            
            # Handle the specific column name from the SQL schema
            if 'situation,_1878' in df.columns:
                df = df.rename(columns={'situation,_1878': 'situation_1878'})
            
            if 'assignment_/_scholarship_year' in df.columns:
                df = df.rename(columns={'assignment_/_scholarship_year': 'assignment_scholarship_year'})
            
            # Process date columns
            date_columns = ['birth_date', 'arrival', 'departure', 'arrival_at_lincoln', 'departure_at_lincoln', 'departure_from_lincoln']
            
            for col in date_columns:
                if col in df.columns:
                    df[f'{col}_original_text'] = df[col].astype(str)
                    df[f'{col}_uncertain'] = False
                    
                    # Clean dates
                    cleaned_dates = []
                    uncertainties = []
                    
                    for date_str in df[col]:
                        cleaned_date, is_uncertain, uncertainty_type = self.clean_date(date_str)
                        cleaned_dates.append(cleaned_date)
                        uncertainties.append(is_uncertain)
                    
                    df[col] = cleaned_dates
                    df[f'{col}_uncertain'] = uncertainties
            
            # Ensure all required columns exist
            required_columns = [
                'family_name', 'given_name', 'aliases', 'birth_date', 'arrival', 'departure',
                'scholarships', 'assignments', 'situation_1878', 'assignment_scholarship_year',
                'references', 'comments', 'birth_date_original_text', 'birth_date_uncertain',
                'arrival_original_text', 'arrival_uncertain', 'arrival_at_lincoln',
                'departure_original_text', 'departure_uncertain', 'departure_at_lincoln',
                'departure_from_lincoln'
            ]
            
            for col in required_columns:
                if col not in df.columns:
                    df[col] = None
            
            # Convert text columns to string type
            text_columns = ['family_name', 'given_name', 'aliases', 'scholarships', 'assignments', 
                           'situation_1878', 'assignment_scholarship_year', 'references', 'comments',
                           'birth_date_original_text', 'arrival_original_text', 'departure_original_text']
            
            for col in text_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str).replace('nan', None)
            
            self.logger.info(f"Processed {len(df)} records successfully")
            return df
            
        except Exception as e:
            self.logger.error(f"Error processing orphans file: {str(e)}")
            raise

    def import_orphans_to_db(self, df: pd.DataFrame) -> None:
        """Import civil war orphans data to the database."""
        try:
            with psycopg2.connect(self.db_connection_string) as conn:
                with conn.cursor() as cur:
                    # Prepare data for insertion
                    for index, row in df.iterrows():
                        cur.execute("""
                            INSERT INTO civil_war_orphans (
                                family_name, given_name, aliases, birth_date, arrival, departure,
                                scholarships, assignments, situation_1878, assignment_scholarship_year,
                                references, comments, birth_date_original_text, birth_date_uncertain,
                                arrival_original_text, arrival_uncertain, arrival_at_lincoln,
                                departure_original_text, departure_uncertain, departure_at_lincoln,
                                departure_from_lincoln
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                        """, (
                            row.get('family_name'), row.get('given_name'), row.get('aliases'),
                            row.get('birth_date'), row.get('arrival'), row.get('departure'),
                            row.get('scholarships'), row.get('assignments'), row.get('situation_1878'),
                            row.get('assignment_scholarship_year'), row.get('references'), row.get('comments'),
                            row.get('birth_date_original_text'), row.get('birth_date_uncertain'),
                            row.get('arrival_original_text'), row.get('arrival_uncertain'), row.get('arrival_at_lincoln'),
                            row.get('departure_original_text'), row.get('departure_uncertain'), row.get('departure_at_lincoln'),
                            row.get('departure_from_lincoln')
                        ))
                    
                conn.commit()
                self.logger.info(f"Successfully imported {len(df)} civil war orphans records")
                
        except Exception as e:
            self.logger.error(f"Error importing civil war orphans to database: {str(e)}")
            raise

    def run_orphans_import(self, file_path: str) -> None:
        """Run the import process for the civil war orphans data file."""
        try:
            self.logger.info(f"Starting civil war orphans import process for: {file_path}")
            
            # Create civil war orphans database schema
            self.create_orphans_database_schema()
            
            # Process the file
            df = self.process_orphans_file(file_path)
            self.import_orphans_to_db(df)
            
            self.logger.info("Civil war orphans import process completed successfully")
                    
        except Exception as e:
            self.logger.error(f"Civil war orphans import process failed: {str(e)}")
            raise

def main():
    """Main function to run the import process."""
    try:
        # Define the path to the Lincoln student data file
        lincoln_data_file = "data/Lincoln_student_data.csv"
        
        # Check if the file exists
        if not Path(lincoln_data_file).exists():
            print(f"Error: File not found: {lincoln_data_file}")
            print("Please ensure the Lincoln_student_data.csv file is in the data/ directory.")
            return
        
        # Create importer instance
        importer = DataImporter()
        
        # Run the import process
        importer.run_import(lincoln_data_file)
        
        print("Import completed successfully!")
        
    except Exception as e:
        print(f"Import failed: {str(e)}")
        logging.error(f"Import failed: {str(e)}")

if __name__ == "__main__":
    main()

class TestDataImporter(unittest.TestCase):
    def setUp(self):
        self.importer = DataImporter(db_connection_string='')  # No DB needed for these tests

    def test_clean_year_valid(self):
        self.assertEqual(self.importer.clean_year('1890'), 1890)
        self.assertEqual(self.importer.clean_year(1890), 1890)
        self.assertEqual(self.importer.clean_year(1890.0), 1890)
        self.assertEqual(self.importer.clean_year('about 1890'), 1890)
        self.assertEqual(self.importer.clean_year('c. 1890'), 1890)
        self.assertEqual(self.importer.clean_year('1890-01-01'), 1890)
        self.assertEqual(self.importer.clean_year('1890/1891'), 1890)
        self.assertEqual(self.importer.clean_year('1890 or 1891'), 1890)
        self.assertEqual(self.importer.clean_year('age 10'), 1890)

    def test_clean_year_invalid(self):
        self.assertIsNone(self.importer.clean_year(''))
        self.assertIsNone(self.importer.clean_year(None))
        self.assertIsNone(self.importer.clean_year('nan'))
        self.assertIsNone(self.importer.clean_year('inf'))
        self.assertIsNone(self.importer.clean_year('-inf'))
        self.assertIsNone(self.importer.clean_year('infinity'))
        self.assertIsNone(self.importer.clean_year('-infinity'))
        self.assertIsNone(self.importer.clean_year(1799))
        self.assertIsNone(self.importer.clean_year(2001))
        self.assertIsNone(self.importer.clean_year(9999))
        self.assertIsNone(self.importer.clean_year(float('inf')))
        self.assertIsNone(self.importer.clean_year(float('-inf')))
        self.assertIsNone(self.importer.clean_year(float('nan')))

    def test_clean_date_invalid(self):
        # Should return None for invalid/NaT
        self.assertEqual(self.importer.clean_date('NaT'), (None, False, None))
        self.assertEqual(self.importer.clean_date('nan'), (None, False, None))
        self.assertEqual(self.importer.clean_date(''), (None, False, None))
        self.assertEqual(self.importer.clean_date(None), (None, False, None))

    def test_clean_date_valid(self):
        # Should parse valid dates
        result, uncertain, typ = self.importer.clean_date('1890-01-01')
        self.assertEqual(result.year, 1890)
        self.assertFalse(uncertain)
        self.assertIsNone(typ) 