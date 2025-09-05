"""
Tests for the clean architecture implementation.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from datetime import datetime
import logging

from src.lincoln_importer import LincolnImporter
from src.data_processor import DataProcessor
from src.file_processor import FileProcessor
from src.database_manager import DatabaseManager

class TestDataProcessor(unittest.TestCase):
    """Test the DataProcessor class."""
    
    def setUp(self):
        self.logger = Mock(spec=logging.Logger)
        self.processor = DataProcessor(self.logger)
    
    def test_clean_year_valid(self):
        """Test cleaning valid year values."""
        self.assertEqual(self.processor.clean_year('1890'), 1890)
        self.assertEqual(self.processor.clean_year(1890), 1890)
        self.assertEqual(self.processor.clean_year(1890.0), 1890)
        self.assertEqual(self.processor.clean_year('about 1890'), 1890)
        self.assertEqual(self.processor.clean_year('c. 1890'), 1890)
        self.assertEqual(self.processor.clean_year('1890-01-01'), 1890)
        self.assertEqual(self.processor.clean_year('1890/1891'), 1890)
        self.assertEqual(self.processor.clean_year('1890 or 1891'), 1890)
        self.assertEqual(self.processor.clean_year('age 10'), 1890)
    
    def test_clean_year_invalid(self):
        """Test cleaning invalid year values."""
        self.assertIsNone(self.processor.clean_year(''))
        self.assertIsNone(self.processor.clean_year(None))
        self.assertIsNone(self.processor.clean_year('nan'))
        self.assertIsNone(self.processor.clean_year('inf'))
        self.assertIsNone(self.processor.clean_year('-inf'))
        self.assertIsNone(self.processor.clean_year('infinity'))
        self.assertIsNone(self.processor.clean_year('-infinity'))
        self.assertIsNone(self.processor.clean_year(1799))
        self.assertIsNone(self.processor.clean_year(2001))
        self.assertIsNone(self.processor.clean_year(9999))
        self.assertIsNone(self.processor.clean_year(float('inf')))
        self.assertIsNone(self.processor.clean_year(float('-inf')))
        self.assertIsNone(self.processor.clean_year(float('nan')))
    
    def test_clean_date_valid(self):
        """Test cleaning valid date values."""
        result, uncertain, typ = self.processor.clean_date('1890-01-01')
        self.assertEqual(result.year, 1890)
        self.assertFalse(uncertain)
        self.assertIsNone(typ)
    
    def test_clean_date_invalid(self):
        """Test cleaning invalid date values."""
        self.assertEqual(self.processor.clean_date('NaT'), (None, False, None))
        self.assertEqual(self.processor.clean_date('nan'), (None, False, None))
        self.assertEqual(self.processor.clean_date(''), (None, False, None))
        self.assertEqual(self.processor.clean_date(None), (None, False, None))
    
    def test_clean_name(self):
        """Test cleaning name values."""
        self.assertEqual(self.processor.clean_name('John Doe'), 'John Doe')
        self.assertEqual(self.processor.clean_name('John-Doe'), 'John-Doe')
        self.assertEqual(self.processor.clean_name('John.Doe'), 'John.Doe')
        self.assertEqual(self.processor.clean_name('John@Doe'), 'JohnDoe')
        self.assertIsNone(self.processor.clean_name(None))
        self.assertIsNone(self.processor.clean_name(''))

class TestFileProcessor(unittest.TestCase):
    """Test the FileProcessor class."""
    
    def setUp(self):
        self.logger = Mock(spec=logging.Logger)
        self.processor = FileProcessor(self.logger)
    
    def test_validate_file_exists(self):
        """Test file validation."""
        # Test with non-existent file
        self.assertFalse(self.processor.validate_file_exists('nonexistent.txt'))
        
        # Test with existing file (create a temporary one)
        with open('temp_test.txt', 'w') as f:
            f.write('test')
        
        self.assertTrue(self.processor.validate_file_exists('temp_test.txt'))
        
        # Clean up
        import os
        os.remove('temp_test.txt')
    
    def test_get_column_mapping(self):
        """Test column mapping detection."""
        # Test with spaced format
        df = pd.DataFrame({
            'Family Name': ['Smith'],
            'English given name': ['John'],
            'Year of birth': [1890]
        })
        
        mapping = self.processor.get_column_mapping(df)
        self.assertIn('family_name', mapping.values())
        self.assertIn('english_given_name', mapping.values())
        self.assertIn('year_of_birth', mapping.values())

class TestDatabaseManager(unittest.TestCase):
    """Test the DatabaseManager class."""
    
    def setUp(self):
        self.logger = Mock(spec=logging.Logger)
        self.db_manager = DatabaseManager('test_connection_string', self.logger)
    
    @patch('psycopg2.connect')
    def test_create_connection(self, mock_connect):
        """Test database connection creation."""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        result = self.db_manager.create_connection()
        
        mock_connect.assert_called_once_with('test_connection_string')
        self.assertEqual(result, mock_conn)

class TestLincolnImporter(unittest.TestCase):
    """Test the LincolnImporter class."""
    
    def setUp(self):
        self.importer = LincolnImporter('test_connection_string')
    
    def test_initialization(self):
        """Test importer initialization."""
        self.assertIsNotNone(self.importer.logger)
        self.assertIsNotNone(self.importer.file_processor)
        self.assertIsNotNone(self.importer.data_processor)
        self.assertIsNotNone(self.importer.db_manager)
    
    @patch('src.lincoln_importer.FileProcessor')
    @patch('src.lincoln_importer.DatabaseManager')
    def test_import_lincoln_data(self, mock_db_manager, mock_file_processor):
        """Test Lincoln data import process."""
        # Mock file processor
        mock_file_processor.return_value.validate_file_exists.return_value = True
        mock_file_processor.return_value.read_file.return_value = pd.DataFrame({
            'family_name': ['Smith'],
            'english_given_name': ['John'],
            'year_of_birth': [1890]
        })
        mock_file_processor.return_value.get_column_mapping.return_value = {
            'family_name': 'family_name',
            'english_given_name': 'english_given_name',
            'year_of_birth': 'year_of_birth'
        }
        
        # Mock database manager
        mock_db_manager.return_value.create_lincoln_schema.return_value = None
        mock_db_manager.return_value.insert_lincoln_records.return_value = None
        
        # Test import
        self.importer.import_lincoln_data('test_file.csv')
        
        # Verify calls
        mock_file_processor.return_value.validate_file_exists.assert_called_once_with('test_file.csv')
        mock_file_processor.return_value.read_file.assert_called_once_with('test_file.csv')
        mock_db_manager.return_value.create_lincoln_schema.assert_called_once()
        mock_db_manager.return_value.insert_lincoln_records.assert_called_once()

if __name__ == '__main__':
    unittest.main()
