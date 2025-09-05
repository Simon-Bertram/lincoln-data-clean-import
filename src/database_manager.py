"""
Database management utilities for PostgreSQL operations.
"""

import psycopg2
import logging
from typing import Dict, Any, List
from pathlib import Path

class DatabaseManager:
    """Handles database connection and schema operations."""
    
    def __init__(self, connection_string: str, logger: logging.Logger):
        self.connection_string = connection_string
        self.logger = logger
    
    def create_connection(self):
        """Create and return a database connection."""
        try:
            return psycopg2.connect(self.connection_string)
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {str(e)}")
            raise
    
    def create_lincoln_schema(self) -> None:
        """Create the Lincoln student database schema if it doesn't exist."""
        schema_sql = """
        CREATE TABLE IF NOT EXISTS lincoln_students (
            id SERIAL PRIMARY KEY,
            census_record_1900 VARCHAR(100),
            indian_name VARCHAR(500),
            family_name VARCHAR(200),
            english_given_name VARCHAR(200),
            alias VARCHAR(200),
            sex CHAR(1),
            year_of_birth INTEGER,
            arrival_at_lincoln DATE,
            departure_from_lincoln DATE,
            nation VARCHAR(200),
            band VARCHAR(200),
            agency VARCHAR(200),
            trade VARCHAR(200),
            source VARCHAR(500),
            comments TEXT,
            cause_of_death VARCHAR(500),
            cemetery_burial VARCHAR(500),
            relevant_links TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_lincoln_family_name ON lincoln_students(family_name);
        CREATE INDEX IF NOT EXISTS idx_lincoln_year_of_birth ON lincoln_students(year_of_birth);
        CREATE INDEX IF NOT EXISTS idx_lincoln_nation ON lincoln_students(nation);
        """
        
        try:
            with self.create_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(schema_sql)
                conn.commit()
            self.logger.info("Lincoln students schema created successfully")
        except Exception as e:
            self.logger.error(f"Error creating Lincoln schema: {str(e)}")
            raise
    
    def create_orphans_schema(self) -> None:
        """Create the civil war orphans database schema if it doesn't exist."""
        schema_sql = """
        CREATE TABLE IF NOT EXISTS civil_war_orphans (
            id SERIAL PRIMARY KEY,
            family_name VARCHAR(200),
            given_name VARCHAR(200),
            aliases VARCHAR(500),
            birth_date DATE,
            arrival DATE,
            departure DATE,
            scholarships VARCHAR(500),
            assignments VARCHAR(500),
            situation_1878 VARCHAR(500),
            assignment_scholarship_year INTEGER,
            references TEXT,
            comments TEXT,
            birth_date_original_text TEXT,
            birth_date_uncertain BOOLEAN,
            arrival_original_text TEXT,
            arrival_uncertain BOOLEAN,
            arrival_at_lincoln DATE,
            departure_original_text TEXT,
            departure_uncertain BOOLEAN,
            departure_at_lincoln DATE,
            departure_from_lincoln DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_orphans_family_name ON civil_war_orphans(family_name);
        CREATE INDEX IF NOT EXISTS idx_orphans_birth_date ON civil_war_orphans(birth_date);
        """
        
        try:
            with self.create_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(schema_sql)
                conn.commit()
            self.logger.info("Civil war orphans schema created successfully")
        except Exception as e:
            self.logger.error(f"Error creating orphans schema: {str(e)}")
            raise
    
    def insert_lincoln_records(self, records: List[Dict[str, Any]]) -> None:
        """Insert Lincoln student records into the database."""
        insert_sql = """
        INSERT INTO lincoln_students (
            census_record_1900, indian_name, family_name, english_given_name,
            alias, sex, year_of_birth, arrival_at_lincoln, departure_from_lincoln,
            nation, band, agency, trade, source, comments, cause_of_death,
            cemetery_burial, relevant_links
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        try:
            with self.create_connection() as conn:
                with conn.cursor() as cursor:
                    for record in records:
                        cursor.execute(insert_sql, (
                            record.get('census_record_1900'),
                            record.get('indian_name'),
                            record.get('family_name'),
                            record.get('english_given_name'),
                            record.get('alias'),
                            record.get('sex'),
                            record.get('year_of_birth'),
                            record.get('arrival_at_lincoln'),
                            record.get('departure_from_lincoln'),
                            record.get('nation'),
                            record.get('band'),
                            record.get('agency'),
                            record.get('trade'),
                            record.get('source'),
                            record.get('comments'),
                            record.get('cause_of_death'),
                            record.get('cemetery_burial'),
                            record.get('relevant_links')
                        ))
                conn.commit()
            self.logger.info(f"Successfully inserted {len(records)} Lincoln student records")
        except Exception as e:
            self.logger.error(f"Error inserting Lincoln records: {str(e)}")
            raise
    
    def insert_orphans_records(self, records: List[Dict[str, Any]]) -> None:
        """Insert civil war orphans records into the database."""
        insert_sql = """
        INSERT INTO civil_war_orphans (
            family_name, given_name, aliases, birth_date, arrival, departure,
            scholarships, assignments, situation_1878, assignment_scholarship_year,
            references, comments, birth_date_original_text, birth_date_uncertain,
            arrival_original_text, arrival_uncertain, arrival_at_lincoln,
            departure_original_text, departure_uncertain, departure_at_lincoln,
            departure_from_lincoln
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        try:
            with self.create_connection() as conn:
                with conn.cursor() as cursor:
                    for record in records:
                        cursor.execute(insert_sql, (
                            record.get('family_name'),
                            record.get('given_name'),
                            record.get('aliases'),
                            record.get('birth_date'),
                            record.get('arrival'),
                            record.get('departure'),
                            record.get('scholarships'),
                            record.get('assignments'),
                            record.get('situation_1878'),
                            record.get('assignment_scholarship_year'),
                            record.get('references'),
                            record.get('comments'),
                            record.get('birth_date_original_text'),
                            record.get('birth_date_uncertain'),
                            record.get('arrival_original_text'),
                            record.get('arrival_uncertain'),
                            record.get('arrival_at_lincoln'),
                            record.get('departure_original_text'),
                            record.get('departure_uncertain'),
                            record.get('departure_at_lincoln'),
                            record.get('departure_from_lincoln')
                        ))
                conn.commit()
            self.logger.info(f"Successfully inserted {len(records)} civil war orphans records")
        except Exception as e:
            self.logger.error(f"Error inserting orphans records: {str(e)}")
            raise
