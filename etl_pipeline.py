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


if __name__ == "__main__":
    main()
