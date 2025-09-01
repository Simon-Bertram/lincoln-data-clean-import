# API Reference

## DataImporter Class

The main class for processing and importing Lincoln School student data.

### Constructor

```python
DataImporter(db_connection_string: str = DB_CONNECTION_STRING)
```

**Parameters:**

- `db_connection_string` (str): PostgreSQL connection string. Defaults to `DB_CONNECTION_STRING` from config.

**Example:**

```python
importer = DataImporter("postgresql://user:pass@localhost:5432/lincoln_db")
```

### Methods

#### `_setup_logging() -> logging.Logger`

Sets up logging configuration for the importer.

**Returns:**

- `logging.Logger`: Configured logger instance

**Features:**

- Creates logs directory if it doesn't exist
- Sets up both file and console handlers
- Uses standard logging format with timestamps

#### `detect_encoding(file_path: str) -> str`

Detects the encoding of a file using chardet.

**Parameters:**

- `file_path` (str): Path to the file to analyze

**Returns:**

- `str`: Detected encoding (defaults to 'utf-8' if detection fails)

**Example:**

```python
encoding = importer.detect_encoding("data/students.csv")
```

#### `clean_date(date_str: str) -> tuple[Optional[datetime], bool, Optional[str]]`

Cleans and standardizes date formats, handling various formats and edge cases.

**Parameters:**

- `date_str` (str): The date string to clean

**Returns:**

- `tuple[Optional[datetime], bool, Optional[str]]`:
  - The cleaned date or None if invalid
  - Boolean indicating if the date is uncertain
  - String describing the type of uncertainty (if any)

**Supported Formats:**

- Standard dates: "1890-01-01", "1890/01/01"
- Ambiguous dates: "about 1890", "c. 1890", "circa 1890"
- Period indicators: "early 1890s", "mid 1890s", "late 1890s"
- Date ranges: "1890-1895", "1890 or 1891"
- Multiple dates: "1890; 1892"

**Example:**

```python
date, uncertain, uncertainty_type = importer.clean_date("about 1890")
# Returns: (datetime(1890, 1, 1), True, "approximate")
```

#### `_parse_date(date_str: str, is_uncertain: bool, uncertainty_type: Optional[str]) -> tuple[Optional[datetime], bool, Optional[str]]`

Helper method to parse dates with uncertainty tracking.

**Parameters:**

- `date_str` (str): The date string to parse
- `is_uncertain` (bool): Whether the date is uncertain
- `uncertainty_type` (Optional[str]): Type of uncertainty

**Returns:**

- `tuple[Optional[datetime], bool, Optional[str]]`: Parsed date with uncertainty info

#### `clean_name(name: str) -> Optional[str]`

Cleans and standardizes names.

**Parameters:**

- `name` (str): The name to clean

**Returns:**

- `Optional[str]`: Cleaned name or None if invalid

**Features:**

- Removes special characters while preserving spaces, hyphens, and periods
- Handles null/empty values

#### `clean_year(year: Any) -> Optional[int]`

Cleans and standardizes year values, handling various formats and approximations.

**Parameters:**

- `year` (Any): The year value to clean

**Returns:**

- `Optional[int]`: The cleaned year or None if invalid

**Supported Formats:**

- Simple years: "1890", 1890, 1890.0
- Age-based: "age 20" (calculates as 1900 - age)
- Ambiguous: "about 1890", "c. 1890"
- Ranges: "1890 or 1891", "1890/1891"
- Full dates: "1890-01-01"

**Validation:**

- Ensures years are between 1800-2000
- Handles infinity, NaN, and invalid values
- Converts floats to integers only if they're whole numbers

**Example:**

```python
year = importer.clean_year("age 20")
# Returns: 1880 (1900 - 20)

year = importer.clean_year("about 1890")
# Returns: 1890
```

#### `check_column_names(df: pd.DataFrame) -> bool`

Checks if the DataFrame has all required columns and validates their format.

**Parameters:**

- `df` (pd.DataFrame): The DataFrame to check

**Returns:**

- `bool`: True if all columns are valid, False otherwise

**Raises:**

- `ValueError`: If required columns are missing or have incorrect names

#### `validate_and_clean_dataframe(df: pd.DataFrame, file_path: str) -> pd.DataFrame`

Validates and cleans the DataFrame to ensure it meets the required format and data types.

**Parameters:**

- `df` (pd.DataFrame): The DataFrame to validate and clean
- `file_path` (str): Path to the CSV file being processed

**Returns:**

- `pd.DataFrame`: Cleaned and validated DataFrame

**Features:**

- Automatic column mapping for different formats
- Data type validation and conversion
- Date and year cleaning with uncertainty tracking
- Data quality metrics generation

**Raises:**

- `ValueError`: If the DataFrame cannot be validated or cleaned

#### `process_file(file_path: str) -> pd.DataFrame`

Processes a single file (CSV or Excel).

**Parameters:**

- `file_path` (str): Path to the file to process

**Returns:**

- `pd.DataFrame`: Processed and cleaned DataFrame

**Supported Formats:**

- CSV files with automatic delimiter detection
- Excel files (.xlsx) using openpyxl engine
- Automatic encoding detection

**Raises:**

- `ValueError`: If file format is unsupported or cannot be read

#### `create_database_schema() -> None`

Creates the database schema if it doesn't exist.

**Features:**

- Drops existing table and recreates with current schema
- Creates all required columns with proper data types
- Sets up indexes for performance
- Handles uncertainty tracking columns

#### `import_to_db(df: pd.DataFrame) -> None`

Imports data to the database.

**Parameters:**

- `df` (pd.DataFrame): The DataFrame to import

**Features:**

- Validates and cleans data before insertion
- Handles date conversion and validation
- Truncates overly long strings to prevent database errors
- Comprehensive error handling with row-level logging
- Transaction safety with proper commit/rollback

**Raises:**

- `Exception`: If database import fails

#### `run_import(file_path: str) -> None`

Runs the complete import process for a Lincoln student data file.

**Parameters:**

- `file_path` (str): Path to the data file to import

**Process:**

1. Creates database schema
2. Processes the file
3. Validates and cleans data
4. Imports to database
5. Generates quality reports

**Raises:**

- `Exception`: If import process fails

## Main Function

### `main() -> None`

Main function to run the import process.

**Features:**

- Checks for required data file
- Creates importer instance
- Runs complete import process
- Handles errors and provides user feedback

## Configuration

### Database Connection

The system uses a PostgreSQL connection string in the format:

```
postgresql://username:password@host:port/database
```

### Environment Variables

- `DB_CONNECTION_STRING`: PostgreSQL connection string
- `LOG_LEVEL`: Logging level (INFO, DEBUG, WARNING, ERROR)

## Data Types

### Column Specifications

| Column                 | Type    | Length | Description              |
| ---------------------- | ------- | ------ | ------------------------ |
| census_record_1900     | VARCHAR | 100    | Census record identifier |
| indian_name            | VARCHAR | 500    | Native/Indigenous name   |
| family_name            | VARCHAR | 200    | Family/surname           |
| english_given_name     | VARCHAR | 200    | English first name       |
| alias                  | VARCHAR | 200    | Alternative names        |
| sex                    | CHAR    | 1      | Gender (M/F)             |
| year_of_birth          | INTEGER | -      | Birth year (1800-2000)   |
| arrival_at_lincoln     | DATE    | -      | Arrival date             |
| departure_from_lincoln | DATE    | -      | Departure date           |
| nation                 | VARCHAR | 200    | Tribal nation            |
| band                   | VARCHAR | 200    | Tribal band              |
| agency                 | VARCHAR | 200    | Government agency        |
| trade                  | VARCHAR | 200    | Occupation/trade         |
| source                 | TEXT    | -      | Data source              |
| comments               | TEXT    | -      | Additional notes         |
| cause_of_death         | TEXT    | -      | Death cause              |
| cemetery_burial        | VARCHAR | 500    | Burial location          |
| relevant_links         | TEXT    | -      | Related links            |

### Uncertainty Types

- `approximate`: "about", "c.", "circa"
- `before`: "before 1890"
- `after`: "after 1890"
- `range`: "1890-1895"
- `multiple_dates`: "1890; 1892"
- `period_qualifier`: "early", "mid", "late"
- `estimated_from_age`: Calculated from age information

## Error Handling

### Exception Types

- `ValueError`: Data validation errors
- `TypeError`: Type conversion errors
- `OverflowError`: Numeric overflow errors
- `psycopg2.Error`: Database errors
- `FileNotFoundError`: Missing files
- `UnicodeDecodeError`: Encoding errors

### Error Recovery

- Invalid dates converted to NULL
- Out-of-range years converted to NULL
- Overly long strings truncated with warnings
- Invalid types handled gracefully
- Comprehensive error logging

## Performance Notes

### Memory Usage

- Processes data in memory-efficient manner
- Uses pandas for efficient data manipulation
- Proper cleanup of temporary objects

### Database Performance

- Uses prepared statements for efficient insertion
- Proper indexing on frequently queried columns
- Transaction-based operations for data integrity

### File Processing

- Automatic encoding detection
- Multiple delimiter support
- Efficient CSV/Excel parsing
