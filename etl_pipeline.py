# Importing important libraries
import pandas as pd
import mysql.connector


class ETLPipeline:
    
    
    def __init__(self):
        self.name='next_cola_pipeline'
        self.tables_dict = {}
        self.dimensions = []
        self.facts = []
        
        # Database server connection config
        self.db_config = {
            'host': 'next-cola-oltp-database.c32iyywau6nz.ap-south-1.rds.amazonaws.com',
            'database': 'NEXT_COLA_OLTP',
            'user': 'admin',               
            'password': 'orobros123'           
        }
    
    
    def check_connection(self):
        # Connect to the database
        try:
            with mysql.connector.connect(**self.db_config) as connection:
                print('Connected to MySQL database')

        except mysql.connector.Error as e:
            print(f"Error connecting to MySQL database: {e}")
        
        
    def extract_data(self):
        
        # Making connection with the database
        connection = mysql.connector.connect(**self.db_config)

        # Creating database cursor
        cursor = connection.cursor()
        
        # Executing query to fetch all table names
        cursor.execute('SHOW TABLES')
        tables = cursor.fetchall()
        
        # Creating a dictionary for table name as key and dataframe as value by loop
        for table in tables:
            # print(table[0])
            query = 'SELECT * FROM ' + table[0]
            
            self.tables_dict[table[0]] = pd.read_sql(query, connection)
            

    def transform_data(self):
        # Creating different data marts with their fact and dimensions tables (star schema)
        
        # First creating all the dimension tables
        dim_supplier = self.tables_dict['Supplier']
        
        dim_warehouse = self.tables_dict['Warehouse'][['WarehouseID' ,'Capacity', 'OpeningHours', 'ClosingHours', 'ContactInfo']]
        
        dim_product = self.tables_dict['Product'][['ProductID', 'Name', 'Price', 'StockLevel', 'ReorderLevel', 'Discontinued']]

        dim_customer = self.tables_dict['Customer']
        
        # Creating fact tables for different data marts
        
        fact_inventory = self.tables_dict['Inventory']
        
        salesorder = self.tables_dict['SalesOrder']
        sales_order_details = self.tables_dict['SalesOrderDetail'].drop(['TotalAmount', 'DeliveryStatus'], axis=1)
        
        fact_sales = salesorder.merge(sales_order_details, on='OrderID')

        purchaseorder = self.tables_dict['PurchaseOrder']
        purchase_order_details = self.tables_dict['PurchaseOrderDetail'].drop(['TotalAmount', 'DeliveryStatus'], axis=1)
        
        fact_purchases = purchaseorder.merge(purchase_order_details, on='OrderID')
        
        shipment = self.tables_dict['Shipment']
        shipment_details = self.tables_dict['ShipmentDetail']
        
        fact_shipment = shipment.merge(shipment_details , on="ShipmentID")
        
        self.dimensions = [dim_customer, dim_product, dim_supplier, dim_warehouse]
        self.facts = [fact_inventory, fact_purchases, fact_sales, fact_shipment]
        
    def load_data(self):
        
        dim_names = ['dim_customer', 'dim_product', 'dim_supplier', 'dim_warehouse']
        fact_names = ['fact_inventory', 'fact_purchases', 'fact_sales', 'fact_shipment']
        
        for i,name in enumerate(dim_names):
            self.dimensions[i].to_csv('s3://next-cola-data-bucket/'+name+'.csv')
            
        for i,name in enumerate(fact_names):
            self.facts[i].to_csv('s3://next-cola-data-bucket/'+name+'.csv')
              
# if __name__ == "__main__":

pipeline = ETLPipeline()
    # pipeline.extract_data()
    # pipeline.transform_data()
    # pipeline.load_data()
    # pipeline.check_connection()
        
