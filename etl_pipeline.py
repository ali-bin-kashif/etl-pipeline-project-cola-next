# Importing important libraries
import pandas as pd
import mysql.connector

# Database server connection config
db_config = {
    'host': 'localhost',
    'database': 'NEXT_COLA_OLTP',
    'user': 'root',               
    'password': 'ali123'           
}

# Making connection with the database
connection = mysql.connector.connect(**db_config)

def test_func(cursor):
    query = "SELECT * FROM salesorder" 
    data = pd.read_sql(query,connection)
    print(data)
    
def main():
    # Connect to the database
    try:
        with mysql.connector.connect(**db_config) as connection:
            print('Connected to MySQL database')

            with connection.cursor() as cursor:
                test_func(cursor)

    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL database: {e}")


class ETLPipeline:
    
    
    def __init__(self):
        self.name='next_cola_pipeline'
        self.tables_dict = {}
        
        
    def extract_data(self):
        # Creating database cursor
        cursor = connection.cursor()
        
        # Executing query to fetch all table names
        cursor.execute('SHOW TABLES')
        tables = cursor.fetchall()
        
        # Creating a dictionary for table name as key and dataframe as value by loop
        for table in tables:
            query = 'SELECT * FROM ' + table[0]
            self.tables_dict[table[0]] = pd.read_sql(query, connection)
            

    def transform_data(self):
        # Creating different data marts with their fact and dimensions tables (star schema)
        
        # First creating all the dimension tables
        dim_supplier = self.tables_dict['supplier']
        
        dim_warehouse = self.tables_dict['warehouse'][['WarehouseID' ,'Capacity', 'OpeningHours', 'ClosingHours', 'ContactInfo']]
        
        dim_product = self.tables_dict['product'][['ProductID', 'Name', 'Price', 'StockLevel', 'ReorderLevel', 'Discontinued']]

        dim_customer = self.tables_dict['customer']
        
        
if __name__ == "__main__":

    pipeline = ETLPipeline()
    pipeline.extract_data()
    pipeline.transform_data()
    
