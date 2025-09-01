# Installation Guide

## Prerequisites

### System Requirements

- **Python**: 3.8 or higher
- **Operating System**: Windows, macOS, or Linux
- **Memory**: Minimum 4GB RAM (8GB recommended for large datasets)
- **Storage**: 1GB free space for installation and data processing
- **Network**: Internet connection for package installation

### Database Requirements

- **PostgreSQL**: 12.0 or higher
- **Database Access**: Local PostgreSQL server or cloud database (e.g., Neon, AWS RDS)
- **Connection**: Network access to database server

## Installation Steps

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/lincoln_school_v2.git
cd lincoln_school_v2
```

### 2. Create Virtual Environment

**Windows:**

```bash
python -m venv venv
venv\Scripts\activate
```

**macOS/Linux:**

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

**Required Packages:**

- pandas>=1.5.0
- psycopg2-binary>=2.9.0
- openpyxl>=3.1.0
- chardet>=5.0.0
- python-dotenv>=1.0.0

### 4. Database Setup

#### Option A: Local PostgreSQL

1. **Install PostgreSQL**:

   - **Windows**: Download from https://www.postgresql.org/download/windows/
   - **macOS**: `brew install postgresql`
   - **Linux**: `sudo apt-get install postgresql postgresql-contrib`

2. **Create Database**:
   ```bash
   sudo -u postgres psql
   CREATE DATABASE lincoln_school;
   CREATE USER lincoln_user WITH PASSWORD 'your_password';
   GRANT ALL PRIVILEGES ON DATABASE lincoln_school TO lincoln_user;
   \q
   ```

#### Option B: Cloud Database (Neon)

1. **Sign up** at https://neon.tech
2. **Create a new project**
3. **Copy the connection string** from the dashboard

### 5. Configuration

#### Create Configuration File

Create `config.py` in the project root:

```python
# Database Configuration
DB_CONNECTION_STRING = "postgresql://username:password@host:port/database"

# For Neon (example):
# DB_CONNECTION_STRING = "postgresql://user:pass@ep-abc-123.us-east-2.aws.neon.tech/lincoln_school"

# Optional: Environment-specific settings
import os
if os.getenv('ENVIRONMENT') == 'production':
    DB_CONNECTION_STRING = os.getenv('DATABASE_URL')
```

#### Environment Variables (Optional)

Create `.env` file for sensitive configuration:

```bash
# .env
DATABASE_URL=postgresql://username:password@host:port/database
LOG_LEVEL=INFO
ENVIRONMENT=development
```

### 6. Data Preparation

#### Create Data Directory

```bash
mkdir data
```

#### Add Your Data Files

Place your CSV or Excel files in the `data/` directory:

```
data/
├── Lincoln_student_data.csv
├── Most_Data.csv
├── Reworked_Data.csv
└── UTF-8Partial_Data.csv
```

#### Expected File Format

Your data files should contain columns similar to:

| Column Name            | Description       | Example                |
| ---------------------- | ----------------- | ---------------------- |
| Census Record 1900     | Census identifier | "12345"                |
| Indian Name            | Native name       | "Wakȟáŋ Tȟáŋka"        |
| Family Name            | Surname           | "Smith"                |
| English given name     | First name        | "John"                 |
| Sex                    | Gender            | "M" or "F"             |
| Year of birth          | Birth year        | "1890" or "about 1890" |
| Arrival at Lincoln     | Arrival date      | "1890-01-01"           |
| Departure from Lincoln | Departure date    | "1895-06-15"           |
| Nation                 | Tribal nation     | "Sioux"                |
| Band                   | Tribal band       | "Brule"                |
| Agency                 | Government agency | "Rosebud"              |

## Verification

### 1. Test Installation

```bash
python -c "import pandas, psycopg2, openpyxl; print('All packages installed successfully')"
```

### 2. Test Database Connection

```bash
python -c "
import psycopg2
from config import DB_CONNECTION_STRING
conn = psycopg2.connect(DB_CONNECTION_STRING)
print('Database connection successful')
conn.close()
"
```

### 3. Run Tests

```bash
python -m unittest test_data_importer.py
```

### 4. Test Data Processing

```bash
python debug_data.py
```

## Troubleshooting

### Common Installation Issues

#### 1. Python Version Issues

**Problem**: "Python 3.8+ required"
**Solution**:

```bash
python --version  # Check version
# If < 3.8, install newer Python
```

#### 2. Virtual Environment Issues

**Problem**: "venv not found"
**Solution**:

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS/Linux
python3 -m venv venv
source venv/bin/activate
```

#### 3. Package Installation Issues

**Problem**: "Failed to install psycopg2"
**Solution**:

```bash
# Windows: Install Visual C++ Build Tools
# Or use binary package:
pip install psycopg2-binary

# macOS: Install PostgreSQL development headers
brew install postgresql

# Linux: Install development packages
sudo apt-get install python3-dev libpq-dev
```

#### 4. Database Connection Issues

**Problem**: "Connection refused"
**Solutions**:

1. **Check PostgreSQL service**:

   ```bash
   # Windows
   services.msc  # Check PostgreSQL service

   # macOS
   brew services list | grep postgresql

   # Linux
   sudo systemctl status postgresql
   ```

2. **Verify connection string**:

   ```python
   # Test connection
   import psycopg2
   try:
       conn = psycopg2.connect(DB_CONNECTION_STRING)
       print("Connection successful")
       conn.close()
   except Exception as e:
       print(f"Connection failed: {e}")
   ```

3. **Check firewall settings** (for remote databases)

#### 5. Permission Issues

**Problem**: "Permission denied"
**Solution**:

```bash
# Check file permissions
ls -la

# Fix permissions
chmod +x main.py
chmod 644 config.py
```

### Development Setup

#### 1. Install Development Dependencies

```bash
pip install -r requirements-dev.txt
```

#### 2. Set Up Pre-commit Hooks

```bash
pre-commit install
```

#### 3. Configure IDE

**VS Code Settings** (`.vscode/settings.json`):

```json
{
  "python.defaultInterpreterPath": "./venv/bin/python",
  "python.linting.enabled": true,
  "python.linting.pylintEnabled": true,
  "python.formatting.provider": "black"
}
```

## Production Deployment

### 1. Environment Setup

```bash
# Set production environment
export ENVIRONMENT=production
export DATABASE_URL=your_production_db_url
export LOG_LEVEL=WARNING
```

### 2. Security Considerations

- Use environment variables for sensitive data
- Restrict database user permissions
- Enable SSL for database connections
- Use strong passwords
- Regular security updates

### 3. Performance Tuning

```python
# config.py - Production settings
import os

DB_CONNECTION_STRING = os.getenv('DATABASE_URL')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'WARNING')

# Database connection pooling
DB_POOL_SIZE = 10
DB_MAX_OVERFLOW = 20
```

### 4. Monitoring

- Set up log monitoring
- Database performance monitoring
- Error tracking (e.g., Sentry)
- Health checks

## Support

### Getting Help

1. **Check the logs**: `logs/import.log`
2. **Run debug script**: `python debug_data.py`
3. **Review documentation**: Check the docs/ directory
4. **Create issue**: Use the GitHub issue tracker

### Common Questions

**Q: Can I use a different database?**
A: Currently only PostgreSQL is supported. MySQL/MariaDB support could be added.

**Q: How do I handle large files?**
A: The system automatically handles large files. For very large datasets (>100k records), consider chunked processing.

**Q: Can I customize the data cleaning rules?**
A: Yes, modify the cleaning functions in `data_importer.py` to suit your needs.

**Q: How do I backup the database?**
A: Use PostgreSQL backup tools:

```bash
pg_dump lincoln_school > backup.sql
```
