# Lincoln School Data Importer

A comprehensive data processing and import system for historical Lincoln School student records, designed to handle complex, ambiguous, and incomplete historical data with robust error handling and data quality tracking.

## Overview

The Lincoln School Data Importer processes historical student records from various source formats (CSV, Excel) and imports them into a PostgreSQL database with comprehensive data cleaning, validation, and uncertainty tracking. The system is specifically designed to handle the challenges of historical data, including:

- Ambiguous date formats (e.g., "about 1890", "c. 1875", "early 1880s")
- Age-based year estimates (e.g., "age 20" from 1900 census)
- Multiple possible dates (e.g., "1870 or 1871")
- Incomplete or corrupted data
- Various file formats and column naming conventions

## Features

### Data Processing Capabilities

- **Multi-format Support**: Handles CSV and Excel files with automatic format detection
- **Flexible Column Mapping**: Supports multiple column naming conventions (spaced, camelCase, underscore, short names)
- **Encoding Detection**: Automatically detects and handles various file encodings
- **Delimiter Detection**: Automatically identifies CSV delimiters (comma, pipe, tab)

### Date and Year Processing

- **Ambiguous Date Handling**: Processes dates with qualifiers like "about", "circa", "before", "after"
- **Period Indicators**: Handles "early", "mid", "late" century/decade indicators
- **Age-based Estimates**: Calculates birth years from age information (assumes 1900 census)
- **Range Processing**: Handles date ranges and multiple possible dates
- **Uncertainty Tracking**: Records the type and level of uncertainty for each date

### Data Quality Features

- **Comprehensive Validation**: Validates data types, ranges, and formats
- **Error Handling**: Graceful handling of invalid or corrupted data
- **Data Quality Metrics**: Tracks parsing success rates and data completeness
- **Audit Trail**: Maintains original text values for reference and debugging
- **String Truncation**: Prevents database errors from overly long text fields

### Database Features

- **PostgreSQL Integration**: Full PostgreSQL database support with proper schema management
- **Indexing**: Automatic creation of performance indexes on key fields
- **Transaction Safety**: Ensures data integrity with proper transaction handling
- **Schema Management**: Automatic table creation and schema updates

## Installation

### Prerequisites

- Python 3.8 or higher
- PostgreSQL database (local or cloud-based like Neon)
- Required Python packages (see requirements.txt)

### Setup

1. **Clone the repository**:

   ```bash
   git clone <repository-url>
   cd lincoln_school_v2
   ```

2. **Create virtual environment**:

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

4. **Configure database connection**:
   Create a `config.py` file with your database connection string:
   ```python
   DB_CONNECTION_STRING = "postgresql://username:password@host:port/database"
   ```

## Usage

### Basic Import

```bash
python main.py
```

This will:

1. Process the `data/Lincoln_student_data.csv` file
2. Clean and validate all data
3. Import records into the PostgreSQL database
4. Generate comprehensive logs and data quality reports

### Programmatic Usage

```python
from data_importer import DataImporter

# Create importer instance
importer = DataImporter()

# Process a specific file
importer.run_import("path/to/your/data.csv")
```

### Running Tests

```bash
python -m unittest test_data_importer.py
```

## Data Format Support

### Supported Input Formats

- **CSV files**: Comma, pipe (|), or tab delimited
- **Excel files**: .xlsx format
- **Column naming conventions**:
  - Spaced: "Year of birth", "Family Name"
  - CamelCase: "yearOfBirth", "familyName"
  - Underscore: "year_of_birth", "family_name"
  - Short: "birth", "family"
  - Numeric: "0", "1", "2" (for reworked data)

### Required Columns

The system expects the following data fields (with flexible naming):

| Field                  | Database Column        | Type         | Description              |
| ---------------------- | ---------------------- | ------------ | ------------------------ |
| Census Record 1900     | census_record_1900     | VARCHAR(100) | Census record identifier |
| Indian Name            | indian_name            | VARCHAR(500) | Native/Indigenous name   |
| Family Name            | family_name            | VARCHAR(200) | Family/surname           |
| English Given Name     | english_given_name     | VARCHAR(200) | English first name       |
| Alias                  | alias                  | VARCHAR(200) | Alternative names        |
| Sex                    | sex                    | CHAR(1)      | Gender (M/F)             |
| Year of Birth          | year_of_birth          | INTEGER      | Birth year (1800-2000)   |
| Arrival at Lincoln     | arrival_at_lincoln     | DATE         | Arrival date             |
| Departure from Lincoln | departure_from_lincoln | DATE         | Departure date           |
| Nation                 | nation                 | VARCHAR(200) | Tribal nation            |
| Band                   | band                   | VARCHAR(200) | Tribal band              |
| Agency                 | agency                 | VARCHAR(200) | Government agency        |
| Trade                  | trade                  | VARCHAR(200) | Occupation/trade         |
| Source                 | source                 | TEXT         | Data source              |
| Comments               | comments               | TEXT         | Additional notes         |
| Cause of Death         | cause_of_death         | TEXT         | Death cause              |
| Cemetery/Burial        | cemetery_burial        | VARCHAR(500) | Burial location          |
| Relevant Links         | relevant_links         | TEXT         | Related links            |

## Data Processing Details

### Date Cleaning Process

1. **Qualifier Detection**: Identifies temporal qualifiers ("about", "circa", "before", "after")
2. **Period Processing**: Handles "early", "mid", "late" indicators
3. **Range Handling**: Processes date ranges and multiple dates
4. **Format Parsing**: Supports multiple date formats (YYYY-MM-DD, YYYY/MM/DD, etc.)
5. **Validation**: Ensures dates fall within reasonable range (1800-2100)
6. **Uncertainty Tracking**: Records uncertainty type and original text

### Year Processing Process

1. **Age-based Calculation**: Estimates birth year from age (1900 - age)
2. **Qualifier Handling**: Processes "about", "c.", "circa" qualifiers
3. **Range Processing**: Handles "or" and "/" separators
4. **Format Support**: Handles full dates, simple years, and float values
5. **Validation**: Ensures years are within range (1800-2000)
6. **Type Safety**: Converts to integer only for valid whole numbers

### Data Quality Tracking

The system tracks several quality metrics:

- **Parsing Success Rates**: Percentage of successfully parsed dates/years
- **Null Counts**: Number of missing values per field
- **Uncertainty Types**: Categories of data uncertainty
- **Original Text Preservation**: Maintains original values for reference
- **Error Logging**: Comprehensive error tracking and reporting

## Database Schema

### Main Table: students

```sql
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
    year_of_birth_uncertainty_type VARCHAR(50),
    year_of_birth_original_text TEXT,
    arrival_at_lincoln DATE,
    arrival_at_lincoln_uncertain BOOLEAN DEFAULT FALSE,
    arrival_at_lincoln_uncertainty_type VARCHAR(50),
    arrival_at_lincoln_original_text TEXT,
    departure_from_lincoln DATE,
    departure_from_lincoln_uncertain BOOLEAN DEFAULT FALSE,
    departure_from_lincoln_uncertainty_type VARCHAR(50),
    departure_from_lincoln_original_text TEXT,
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
```

### Indexes

- `idx_family_name` on `family_name`
- `idx_nation` on `nation`
- `idx_year_of_birth` on `year_of_birth`
- `idx_arrival_date` on `arrival_at_lincoln`
- `idx_departure_date` on `departure_from_lincoln`

## Error Handling

### Common Error Types

1. **VARCHAR Length Errors**: Automatically truncated with warnings
2. **Date Parsing Errors**: Invalid dates converted to NULL with logging
3. **Year Range Errors**: Out-of-range years converted to NULL
4. **Type Conversion Errors**: Invalid types handled gracefully
5. **Database Connection Errors**: Proper connection management and retry logic

### Logging

The system provides comprehensive logging:

- **File Logging**: All operations logged to `logs/import.log`
- **Console Output**: Real-time progress and error reporting
- **Data Quality Reports**: Summary statistics and parsing results
- **Error Details**: Specific error messages with row-level information

## Configuration

### Environment Variables

- `DB_CONNECTION_STRING`: PostgreSQL connection string
- `LOG_LEVEL`: Logging level (INFO, DEBUG, WARNING, ERROR)

### Configuration File

Create `config.py` with:

```python
DB_CONNECTION_STRING = "postgresql://username:password@host:port/database"
```

## Performance Considerations

### Optimization Features

- **Batch Processing**: Efficient database insertion
- **Indexing**: Automatic index creation for common queries
- **Memory Management**: Proper DataFrame handling and cleanup
- **Transaction Safety**: Efficient transaction management

### Scalability

- **Large File Support**: Handles files with thousands of records
- **Memory Efficient**: Processes data in chunks when needed
- **Database Optimization**: Proper indexing and schema design

## Troubleshooting

### Common Issues

1. **Database Connection**: Verify connection string and network access
2. **File Encoding**: Check file encoding if parsing fails
3. **Column Mapping**: Verify column names match expected format
4. **Memory Issues**: Large files may require chunked processing

### Debug Tools

- **Debug Script**: `python debug_data.py` for data analysis
- **Test Suite**: `python -m unittest test_data_importer.py`
- **Log Files**: Check `logs/import.log` for detailed error information

## Contributing

### Development Setup

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

### Code Standards

- Follow PEP 8 style guidelines
- Add comprehensive docstrings
- Include unit tests for new features
- Update documentation as needed

## License

[Add your license information here]

## Support

For issues and questions:

- Check the troubleshooting section
- Review the log files
- Create an issue in the repository
- Contact the development team

## Version History

- **v1.0.0**: Initial release with basic import functionality
- **v1.1.0**: Added uncertainty tracking and improved date parsing
- **v1.2.0**: Enhanced error handling and data quality metrics
- **v1.3.0**: Added support for multiple file formats and column mappings
