"""
Database configuration settings.
Replace these values with your actual Neon database credentials.
"""

DB_CONFIG = {
    'host': 'ep-plain-sun-a5864yx4-pooler.us-east-2.aws.neon.tech',
    'port': 5432,
    'database': 'neondb',
    'user': 'neondb_owner',
    'password': 'npg_0kXrR6oOCqeQ'
}

# Construct the connection string
DB_CONNECTION_STRING = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}" 