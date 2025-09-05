"""
Data processing utilities for cleaning and validating historical data.
"""

import pandas as pd
import re
from datetime import datetime
from typing import Optional, Tuple, Any
import logging

class DataProcessor:
    """Handles data cleaning and validation operations."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def clean_date(self, date_str: str) -> Tuple[Optional[datetime], bool, Optional[str]]:
        """
        Clean and standardize date formats, handling various formats and edge cases.
        
        Args:
            date_str: The date string to clean
            
        Returns:
            Tuple containing:
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

    def _parse_date(self, date_str: str, is_uncertain: bool, uncertainty_type: Optional[str]) -> Tuple[Optional[datetime], bool, Optional[str]]:
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
