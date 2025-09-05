"""
Lincoln School Data Importer - Clean Architecture Implementation
"""

import pandas as pd
from typing import Dict, Any, List
import logging

from .data_processor import DataProcessor
from .database_manager import DatabaseManager
from .file_processor import FileProcessor
from .logger import LoggerFactory

class LincolnImporter:
    """
    Main importer class for Lincoln School data.
    
    This class orchestrates the data import process using separate
    components for file processing, data cleaning, and database operations.
    """
    
    def __init__(self, db_connection_string: str, log_level: str = 'INFO'):
        """
        Initialize the Lincoln importer.
        
        Args:
            db_connection_string: Database connection string
            log_level: Logging level
        """
        # Set up logging
        self.logger = LoggerFactory.create_logger(
            'LincolnImporter', 
            log_level, 
            'logs/import.log'
        )
        
        # Initialize components
        self.file_processor = FileProcessor(self.logger)
        self.data_processor = DataProcessor(self.logger)
        self.db_manager = DatabaseManager(db_connection_string, self.logger)
    
    def import_lincoln_data(self, file_path: str) -> None:
        """
        Import Lincoln student data from a file.
        
        Args:
            file_path: Path to the data file
        """
        try:
            self.logger.info(f"Starting Lincoln data import from: {file_path}")
            
            # Validate file exists
            if not self.file_processor.validate_file_exists(file_path):
                raise FileNotFoundError(f"File not found or invalid: {file_path}")
            
            # Create database schema
            self.db_manager.create_lincoln_schema()
            
            # Read and process file
            df = self.file_processor.read_file(file_path)
            column_mapping = self.file_processor.get_column_mapping(df)
            
            # Clean and validate data
            cleaned_records = self._process_lincoln_data(df, column_mapping)
            
            # Import to database
            self.db_manager.insert_lincoln_records(cleaned_records)
            
            self.logger.info("Lincoln data import completed successfully")
            
        except Exception as e:
            self.logger.error(f"Lincoln data import failed: {str(e)}")
            raise
    
    def import_orphans_data(self, file_path: str) -> None:
        """
        Import civil war orphans data from a file.
        
        Args:
            file_path: Path to the data file
        """
        try:
            self.logger.info(f"Starting orphans data import from: {file_path}")
            
            # Validate file exists
            if not self.file_processor.validate_file_exists(file_path):
                raise FileNotFoundError(f"File not found or invalid: {file_path}")
            
            # Create database schema
            self.db_manager.create_orphans_schema()
            
            # Read and process file
            df = self.file_processor.read_file(file_path)
            
            # Clean and validate data
            cleaned_records = self._process_orphans_data(df)
            
            # Import to database
            self.db_manager.insert_orphans_records(cleaned_records)
            
            self.logger.info("Orphans data import completed successfully")
            
        except Exception as e:
            self.logger.error(f"Orphans data import failed: {str(e)}")
            raise
    
    def _process_lincoln_data(self, df: pd.DataFrame, column_mapping: Dict[str, str]) -> List[Dict[str, Any]]:
        """
        Process Lincoln student data.
        
        Args:
            df: Raw DataFrame
            column_mapping: Column name mapping
            
        Returns:
            List of cleaned records
        """
        # Rename columns
        df = df.rename(columns=column_mapping)
        
        # Remove unnamed columns
        df = df.loc[:, ~df.columns.str.contains('^Unnamed:', na=False)].copy()
        
        cleaned_records = []
        
        for _, row in df.iterrows():
            try:
                # Clean year of birth
                year_of_birth = self.data_processor.clean_year(row.get('year_of_birth'))
                
                # Clean dates
                arrival_date, arrival_uncertain, arrival_type = self.data_processor.clean_date(
                    row.get('arrival_at_lincoln')
                )
                departure_date, departure_uncertain, departure_type = self.data_processor.clean_date(
                    row.get('departure_from_lincoln')
                )
                
                # Clean names
                family_name = self.data_processor.clean_name(row.get('family_name'))
                english_given_name = self.data_processor.clean_name(row.get('english_given_name'))
                indian_name = self.data_processor.clean_name(row.get('indian_name'))
                
                # Create cleaned record
                record = {
                    'census_record_1900': str(row.get('census_record_1900', ''))[:100],
                    'indian_name': str(indian_name)[:500] if indian_name else None,
                    'family_name': str(family_name)[:200] if family_name else None,
                    'english_given_name': str(english_given_name)[:200] if english_given_name else None,
                    'alias': str(row.get('alias', ''))[:200],
                    'sex': str(row.get('sex', ''))[:1],
                    'year_of_birth': year_of_birth,
                    'arrival_at_lincoln': arrival_date,
                    'departure_from_lincoln': departure_date,
                    'nation': str(row.get('nation', ''))[:200],
                    'band': str(row.get('band', ''))[:200],
                    'agency': str(row.get('agency', ''))[:200],
                    'trade': str(row.get('trade', ''))[:200],
                    'source': str(row.get('source', ''))[:500],
                    'comments': str(row.get('comments', ''))[:1000],
                    'cause_of_death': str(row.get('cause_of_death', ''))[:500],
                    'cemetery_burial': str(row.get('cemetery_burial', ''))[:500],
                    'relevant_links': str(row.get('relevant_links', ''))[:1000]
                }
                
                cleaned_records.append(record)
                
            except Exception as e:
                self.logger.warning(f"Error processing row: {str(e)}")
                continue
        
        self.logger.info(f"Processed {len(cleaned_records)} Lincoln student records")
        return cleaned_records
    
    def _process_orphans_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Process civil war orphans data.
        
        Args:
            df: Raw DataFrame
            
        Returns:
            List of cleaned records
        """
        cleaned_records = []
        
        for _, row in df.iterrows():
            try:
                # Clean dates
                birth_date, birth_uncertain, birth_type = self.data_processor.clean_date(
                    row.get('birth_date')
                )
                arrival_date, arrival_uncertain, arrival_type = self.data_processor.clean_date(
                    row.get('arrival')
                )
                departure_date, departure_uncertain, departure_type = self.data_processor.clean_date(
                    row.get('departure')
                )
                
                # Clean names
                family_name = self.data_processor.clean_name(row.get('family_name'))
                given_name = self.data_processor.clean_name(row.get('given_name'))
                
                # Create cleaned record
                record = {
                    'family_name': str(family_name)[:200] if family_name else None,
                    'given_name': str(given_name)[:200] if given_name else None,
                    'aliases': str(row.get('aliases', ''))[:500],
                    'birth_date': birth_date,
                    'arrival': arrival_date,
                    'departure': departure_date,
                    'scholarships': str(row.get('scholarships', ''))[:500],
                    'assignments': str(row.get('assignments', ''))[:500],
                    'situation_1878': str(row.get('situation_1878', ''))[:500],
                    'assignment_scholarship_year': self.data_processor.clean_year(
                        row.get('assignment_scholarship_year')
                    ),
                    'references': str(row.get('references', ''))[:1000],
                    'comments': str(row.get('comments', ''))[:1000],
                    'birth_date_original_text': str(row.get('birth_date', '')),
                    'birth_date_uncertain': birth_uncertain,
                    'arrival_original_text': str(row.get('arrival', '')),
                    'arrival_uncertain': arrival_uncertain,
                    'arrival_at_lincoln': arrival_date,
                    'departure_original_text': str(row.get('departure', '')),
                    'departure_uncertain': departure_uncertain,
                    'departure_at_lincoln': departure_date,
                    'departure_from_lincoln': departure_date
                }
                
                cleaned_records.append(record)
                
            except Exception as e:
                self.logger.warning(f"Error processing orphans row: {str(e)}")
                continue
        
        self.logger.info(f"Processed {len(cleaned_records)} orphans records")
        return cleaned_records
