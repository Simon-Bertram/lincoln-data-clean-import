"""
Database connection test script.
Tests the database connection and basic operations.
"""

import os
import sys
import logging
from pathlib import Path

# Add the src directory to the Python path
sys.path.append(str(Path(__file__).parent / 'src'))

from config import DB_CONFIG, DB_CONNECTION_STRING, validate_config
from database_manager import DatabaseManager
from logger import setup_logger

def test_database_connection():
	"""Test database connection and basic operations."""
	
	# Setup logging
	logger = setup_logger('db_test', 'logs/db_test.log')
	
	print("=" * 60)
	print("DATABASE CONNECTION TEST")
	print("=" * 60)
	
	# Test 1: Check environment variables
	print("\n1. Checking environment variables...")
	try:
		validate_config()
		print("✅ Environment variables validation passed")
		print(f"   Host: {DB_CONFIG['host']}")
		print(f"   Port: {DB_CONFIG['port']}")
		print(f"   Database: {DB_CONFIG['database']}")
		print(f"   User: {DB_CONFIG['user']}")
		print(f"   Password: {'*' * len(DB_CONFIG['password']) if DB_CONFIG['password'] else 'Not set'}")
	except ValueError as e:
		print(f"❌ Environment variables validation failed: {e}")
		print("\nPlease create a .env file with the following variables:")
		print("DB_HOST=your_host")
		print("DB_PORT=5432")
		print("DB_NAME=your_database_name")
		print("DB_USER=your_username")
		print("DB_PASSWORD=your_password")
		return False
	
	# Test 2: Test basic connection
	print("\n2. Testing basic database connection...")
	try:
		import psycopg2
		conn = psycopg2.connect(DB_CONNECTION_STRING)
		print("✅ Database connection successful")
		
		# Test basic query
		with conn.cursor() as cursor:
			cursor.execute("SELECT version();")
			version = cursor.fetchone()[0]
			print(f"   PostgreSQL version: {version}")
			
			# Test current database
			cursor.execute("SELECT current_database();")
			current_db = cursor.fetchone()[0]
			print(f"   Current database: {current_db}")
			
			# Test current user
			cursor.execute("SELECT current_user;")
			current_user = cursor.fetchone()[0]
			print(f"   Current user: {current_user}")
		
		conn.close()
		
	except ImportError:
		print("❌ psycopg2 not installed. Please install it with: pip install psycopg2-binary")
		return False
	except Exception as e:
		print(f"❌ Database connection failed: {e}")
		return False
	
	# Test 3: Test DatabaseManager class
	print("\n3. Testing DatabaseManager class...")
	try:
		db_manager = DatabaseManager(DB_CONNECTION_STRING, logger)
		
		# Test connection through DatabaseManager
		with db_manager.create_connection() as conn:
			with conn.cursor() as cursor:
				cursor.execute("SELECT 1;")
				result = cursor.fetchone()[0]
				if result == 1:
					print("✅ DatabaseManager connection test passed")
				else:
					print("❌ DatabaseManager connection test failed")
					return False
		
	except Exception as e:
		print(f"❌ DatabaseManager test failed: {e}")
		return False
	
	# Test 4: Test schema creation (dry run)
	print("\n4. Testing schema creation capabilities...")
	try:
		# Test if we can create tables (without actually creating them)
		with db_manager.create_connection() as conn:
			with conn.cursor() as cursor:
				# Check if tables already exist
				cursor.execute("""
					SELECT table_name 
					FROM information_schema.tables 
					WHERE table_schema = 'public' 
					AND table_name IN ('lincoln_students', 'civil_war_orphans');
				""")
				existing_tables = [row[0] for row in cursor.fetchall()]
				
				if existing_tables:
					print(f"✅ Found existing tables: {', '.join(existing_tables)}")
				else:
					print("ℹ️  No existing tables found (this is normal for a new database)")
				
				# Test if we have CREATE privileges
				cursor.execute("""
					SELECT has_table_privilege(current_user, 'information_schema.tables', 'CREATE');
				""")
				can_create = cursor.fetchone()[0]
				
				if can_create:
					print("✅ User has CREATE privileges")
				else:
					print("⚠️  User may not have CREATE privileges")
		
	except Exception as e:
		print(f"❌ Schema test failed: {e}")
		return False
	
	# Test 5: Test database permissions
	print("\n5. Testing database permissions...")
	try:
		with db_manager.create_connection() as conn:
			with conn.cursor() as cursor:
				# Test SELECT permission
				cursor.execute("SELECT 1;")
				print("✅ SELECT permission: OK")
				
				# Test INSERT permission (create a temporary table)
				cursor.execute("""
					CREATE TEMP TABLE test_permissions (
						id SERIAL PRIMARY KEY,
						test_data VARCHAR(50)
					);
				""")
				cursor.execute("INSERT INTO test_permissions (test_data) VALUES ('test');")
				print("✅ INSERT permission: OK")
				
				# Test UPDATE permission
				cursor.execute("UPDATE test_permissions SET test_data = 'updated' WHERE id = 1;")
				print("✅ UPDATE permission: OK")
				
				# Test DELETE permission
				cursor.execute("DELETE FROM test_permissions WHERE id = 1;")
				print("✅ DELETE permission: OK")
				
				# Test DROP permission
				cursor.execute("DROP TABLE test_permissions;")
				print("✅ DROP permission: OK")
		
	except Exception as e:
		print(f"❌ Permissions test failed: {e}")
		return False
	
	print("\n" + "=" * 60)
	print("✅ ALL DATABASE TESTS PASSED!")
	print("=" * 60)
	print("\nYour database connection is working correctly.")
	print("You can now proceed with data import operations.")
	
	return True

def main():
	"""Main function to run the database connection test."""
	
	# Check if .env file exists
	env_file = Path('.env')
	if not env_file.exists():
		print("⚠️  No .env file found. Creating example .env file...")
		create_example_env_file()
		print("Please edit the .env file with your actual database credentials.")
		return
	
	# Run the database connection test
	success = test_database_connection()
	
	if not success:
		print("\n❌ Database connection test failed.")
		print("Please check your database configuration and try again.")
		sys.exit(1)

def create_example_env_file():
	"""Create an example .env file with placeholder values."""
	env_content = """# Database Configuration
# Replace these values with your actual database credentials

DB_HOST=localhost
DB_PORT=5432
DB_NAME=neondb
DB_USER=postgres
DB_PASSWORD=your_password_here

# Example for Neon database:
# DB_HOST=ep-cool-name-123456.us-east-1.aws.neon.tech
# DB_PORT=5432
# DB_NAME=neondb
# DB_USER=your_username
# DB_PASSWORD=your_password
"""
	
	with open('.env', 'w') as f:
		f.write(env_content)
	
	print("Created .env file with example configuration.")

if __name__ == '__main__':
	main()
