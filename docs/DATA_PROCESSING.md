# Data Processing Documentation

## Overview

The Lincoln School Data Importer uses sophisticated algorithms to handle the complexities of historical data, including ambiguous dates, incomplete information, and various data formats. This document details the processing techniques and algorithms employed.

## Date Processing Algorithm

### 1. Date String Analysis

The system follows a hierarchical approach to date parsing:

```python
def clean_date(date_str: str) -> tuple[Optional[datetime], bool, Optional[str]]:
    # 1. Handle null/empty values
    if pd.isna(date_str) or not date_str:
        return None, False, None

    # 2. Detect qualifiers and uncertainty
    if 'about' in date_str.lower() or 'c.' in date_str.lower():
        return _parse_date(date_str, True, 'approximate')

    # 3. Handle temporal relationships
    if 'before' in date_str.lower():
        return _parse_date(date_str, True, 'before')
    if 'after' in date_str.lower():
        return _parse_date(date_str, True, 'after')

    # 4. Handle period qualifiers
    if any(qualifier in date_str.lower() for qualifier in ['early', 'mid', 'late']):
        return _parse_date(date_str, True, 'period_qualifier')

    # 5. Try standard parsing
    return _parse_date(date_str, False, None)
```

### 2. Date Parsing Techniques

#### Standard Format Parsing

```python
date_formats = [
    '%Y-%m-%d',    # 1890-01-01
    '%Y/%m/%d',    # 1890/01/01
    '%m/%d/%Y',    # 01/01/1890
    '%d/%m/%Y',    # 01/01/1890
    '%Y-%m',       # 1890-01
    '%Y/%m',       # 1890/01
    '%Y'           # 1890
]
```

#### Regex-based Extraction

```python
# Extract year from complex strings
year_match = re.search(r'\d{4}', cleaned_date_str)
if year_match:
    year = int(year_match.group())
    if 1800 <= year <= 2000:
        return datetime(year, 1, 1), is_uncertain, uncertainty_type
```

#### Pandas Flexible Parsing

```python
# Fallback to pandas for complex formats
parsed_date = pd.to_datetime(cleaned_date_str, errors='coerce')
if pd.isna(parsed_date):
    return None, is_uncertain, uncertainty_type
```

### 3. Uncertainty Classification

The system categorizes date uncertainty into specific types:

| Uncertainty Type   | Description            | Example                 |
| ------------------ | ---------------------- | ----------------------- |
| `approximate`      | General approximation  | "about 1890", "c. 1890" |
| `before`           | Temporal relationship  | "before 1890"           |
| `after`            | Temporal relationship  | "after 1890"            |
| `range`            | Date range             | "1890-1895"             |
| `multiple_dates`   | Multiple possibilities | "1890; 1892"            |
| `period_qualifier` | Period indicator       | "early 1890s"           |

## Year Processing Algorithm

### 1. Age-based Calculation

For entries containing age information, the system calculates birth years:

```python
def calculate_birth_year_from_age(age_str: str, census_year: int = 1900) -> Optional[int]:
    age_match = re.search(r'age\s*(\d+)', age_str)
    if age_match:
        age = int(age_match.group(1))
        estimated_year = census_year - age
        if 1800 <= estimated_year <= 2000:
            return estimated_year
    return None
```

**Algorithm:**

1. Extract age using regex pattern `r'age\s*(\d+)'`
2. Calculate birth year: `census_year - age`
3. Validate year is within reasonable range (1800-2000)
4. Mark as uncertain with type `estimated_from_age`

### 2. Year Validation Pipeline

```python
def clean_year(year: Any) -> Optional[int]:
    # 1. Handle null/invalid values
    if pd.isna(year) or year in ['inf', '-inf', 'nan']:
        return None

    # 2. Handle numeric types directly
    if isinstance(year, (int, float)):
        if float(year).is_integer() and 1800 <= int(year) <= 2000:
            return int(year)
        return None

    # 3. String processing
    year_str = str(year).strip().lower()

    # 4. Pattern matching
    if re.match(r'\d{4}(?:\.0)?$', year_str):
        year_int = int(float(year_str))
        if 1800 <= year_int <= 2000:
            return year_int

    # 5. Complex pattern handling
    # ... handle "about", "c.", ranges, etc.
```

### 3. Year Pattern Recognition

| Pattern     | Regex                     | Example        | Result |
| ----------- | ------------------------- | -------------- | ------ |
| Simple year | `r'\d{4}'`                | "1890"         | 1890   |
| Float year  | `r'\d{4}(?:\.0)?$'`       | "1890.0"       | 1890   |
| Age-based   | `r'age\s*(\d+)'`          | "age 20"       | 1880   |
| Approximate | `r'about\s*(\d{4})'`      | "about 1890"   | 1890   |
| Range       | `r'(\d{4})\s*or\s*\d{4}'` | "1890 or 1891" | 1890   |
| Slash range | `r'(\d{4})/\d{4}'`        | "1890/1891"    | 1890   |

## Column Mapping Algorithm

### 1. Format Detection

The system automatically detects column naming conventions:

```python
def detect_column_format(df: pd.DataFrame) -> str:
    column_mappings = {
        'spaced': {'Year of birth': 'year_of_birth', ...},
        'camel': {'yearOfBirth': 'year_of_birth', ...},
        'underscore': {'year_of_birth': 'year_of_birth', ...},
        'short': {'birth': 'year_of_birth', ...},
        'reworked': {'0': 'indian_name', '1': 'family_name', ...}
    }

    best_match_count = 0
    best_format = None

    for format_name, mapping in column_mappings.items():
        match_count = count_matching_columns(df.columns, mapping)
        if match_count > best_match_count:
            best_match_count = match_count
            best_format = format_name

    return best_format
```

### 2. Partial Matching

For cases where exact matches aren't found:

```python
def partial_column_matching(df_columns: List[str], mapping: Dict[str, str]) -> Dict[str, str]:
    partial_matches = {}

    for col in df_columns:
        for map_key in mapping:
            if map_key.lower() in col.lower() or col.lower() in map_key.lower():
                partial_matches[col] = mapping[map_key]

    return partial_matches
```

## Data Quality Assessment

### 1. Quality Metrics Calculation

```python
def calculate_quality_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    return {
        'total_rows': len(df),
        'null_counts': df.isnull().sum().to_dict(),
        'parsed_dates': {
            'year_of_birth': df['year_of_birth'].notna().sum(),
            'arrival_at_lincoln': df['arrival_at_lincoln'].notna().sum(),
            'departure_from_lincoln': df['departure_from_lincoln'].notna().sum()
        },
        'uncertainty_counts': {
            'year_of_birth_uncertain': df['year_of_birth_uncertain'].sum(),
            'arrival_uncertain': df['arrival_at_lincoln_uncertain'].sum(),
            'departure_uncertain': df['departure_from_lincoln_uncertain'].sum()
        }
    }
```

### 2. Data Validation Rules

| Field          | Validation Rule        | Action                     |
| -------------- | ---------------------- | -------------------------- |
| year_of_birth  | 1800 ≤ year ≤ 2000     | Convert to NULL if invalid |
| arrival_date   | Valid date format      | Convert to NULL if invalid |
| departure_date | Valid date format      | Convert to NULL if invalid |
| sex            | Single character (M/F) | Convert to uppercase       |
| string fields  | Length ≤ column limit  | Truncate with warning      |

## Error Handling Strategy

### 1. Graceful Degradation

```python
def safe_parse_date(date_str: str) -> Optional[datetime]:
    try:
        return parse_date(date_str)
    except (ValueError, TypeError, OverflowError):
        logger.warning(f"Could not parse date: {date_str}")
        return None
```

### 2. Error Classification

| Error Type              | Severity | Action                |
| ----------------------- | -------- | --------------------- |
| Missing required column | Critical | Raise ValueError      |
| Invalid data type       | High     | Attempt conversion    |
| Out-of-range value      | Medium   | Convert to NULL       |
| Truncation needed       | Low      | Truncate with warning |
| Parsing failure         | Medium   | Convert to NULL       |

### 3. Recovery Mechanisms

- **Type Conversion**: Attempt to convert invalid types
- **Range Validation**: Ensure values are within acceptable ranges
- **String Truncation**: Prevent database errors from long strings
- **Null Conversion**: Convert invalid values to NULL
- **Logging**: Comprehensive error tracking for debugging

## Performance Optimization

### 1. Memory Management

```python
def process_large_file(file_path: str, chunk_size: int = 1000) -> None:
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        processed_chunk = validate_and_clean_dataframe(chunk)
        import_to_db(processed_chunk)
        del chunk  # Explicit cleanup
```

### 2. Database Optimization

- **Batch Insertion**: Use executemany for bulk inserts
- **Indexing**: Create indexes on frequently queried columns
- **Transaction Management**: Use transactions for data integrity
- **Connection Pooling**: Efficient database connection management

### 3. Algorithm Efficiency

- **Early Exit**: Return early when validation fails
- **Caching**: Cache parsed patterns and mappings
- **Lazy Evaluation**: Process data only when needed
- **Streaming**: Process large files in chunks

## Data Transformation Pipeline

### 1. Input Processing

```
Raw Data → Encoding Detection → Delimiter Detection → DataFrame Creation
```

### 2. Data Cleaning

```
DataFrame → Column Mapping → Type Validation → Data Cleaning → Quality Assessment
```

### 3. Database Import

```
Cleaned Data → Validation → Truncation → Database Insertion → Commit
```

## Uncertainty Tracking

### 1. Uncertainty Metadata

Each uncertain value is tracked with:

- **Uncertainty Flag**: Boolean indicating uncertainty
- **Uncertainty Type**: Specific category of uncertainty
- **Original Text**: Preserved original value for reference

### 2. Uncertainty Propagation

```python
def track_uncertainty(original_value: Any, cleaned_value: Any,
                     uncertainty_type: str) -> Dict[str, Any]:
    return {
        'value': cleaned_value,
        'uncertain': True,
        'uncertainty_type': uncertainty_type,
        'original_text': str(original_value)
    }
```

This comprehensive approach ensures that data quality is maintained while preserving the richness and complexity of historical information.
