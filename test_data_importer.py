"""
Tests for the data importer module.
"""

import unittest
from datetime import datetime
from data_importer import DataImporter

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

def test_clean_date(importer):
    # Test various date formats
    assert importer.clean_date('2023-01-01') == datetime(2023, 1, 1)
    assert importer.clean_date('2023/01/01') == datetime(2023, 1, 1)
    assert importer.clean_date('01/01/2023') == datetime(2023, 1, 1)
    assert importer.clean_date('2023-01-01; 2023-01-02') == datetime(2023, 1, 1)
    assert importer.clean_date(None) is None
    assert importer.clean_date('invalid') is None

def test_clean_name(importer):
    # Test name cleaning
    assert importer.clean_name('John Doe') == 'John Doe'
    assert importer.clean_name('John-Doe') == 'John-Doe'
    assert importer.clean_name('John.Doe') == 'John.Doe'
    assert importer.clean_name('John@Doe') == 'JohnDoe'
    assert importer.clean_name(None) is None
    assert importer.clean_name('') is None

def test_clean_year(importer):
    # Test year cleaning
    assert importer.clean_year('1870') == 1870
    assert importer.clean_year(1870) == 1870
    assert importer.clean_year('age 20') is not None
    assert importer.clean_year(None) is None
    assert importer.clean_year('invalid') is None
    assert importer.clean_year(3000) is None  # Future year
    assert importer.clean_year(1000) is None  # Too old 

if __name__ == '__main__':
	unittest.main() 