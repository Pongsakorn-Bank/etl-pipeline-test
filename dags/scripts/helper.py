import os
import psycopg2
from psycopg2 import OperationalError

class PorstgresFunctions:
    def __init__(self, username, password, host, port, dbname):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.dbname = dbname
    
    def create_connection(self):
        try:
            connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.username,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.connection = connection
            print("Connection successfully")
        except OperationalError as e:
            print(f"Connection failed: {e}")
            return False
    
    def close_connection(self):
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()
            print("Connection closed")
        
    def check_table_exists(self, table_name):
        if not hasattr(self, 'connection') or not self.connection:
            print("No active connection")
            return False
        cursor = self.connection.cursor()
        query = f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='{table_name}');"
        cursor.execute(query)
        exists = cursor.fetchone()[0]
        cursor.close()
        return exists
    
    def execute_query(self, query):
        if not hasattr(self, 'connection') or not self.connection:
            print("No active connection")
            return False
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            self.connection.commit()
            print("Query executed successfully")
            return True
        except Exception as e:
            print(f"Query execution failed: {e}")
            self.connection.rollback()
            return False
    
    def create_partion_table(self, table, unique_dates:set):
        if not hasattr(self, 'connection') or not self.connection:
            self.create_connection()
        cursor = self.connection.cursor()
        for date in unique_dates:
            partition_name = date.strftime("%Y_%m_%d")
            start_date = date.strftime("%Y-%m-%d 00:00:00")
            end_date = date.strftime("%Y-%m-%d 23:59:59")
            try:
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table}_{partition_name} PARTITION OF {table}
                    FOR VALUES FROM ('{start_date}') TO ('{end_date}');
                """)
                print(f"Created partition for {date}")
                self.connection.commit()
            except psycopg2.ProgrammingError as e:
                print(f"Error: {e}")
        cursor.close()