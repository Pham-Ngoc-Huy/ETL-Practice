import pandas as pd 
import numpy as np 
from datetime import datetime 
from sqlalchemy import create_engine
import pyodbc
import logging
import os  
import sys #use to exit the script log when error occur

# Configure logging
logging.basicConfig(
    filename='etl_process.log',
    level=logging.INFO,
    format='%(asctime)s py- %(levelname)s - %(message)s'
)

def log_and_modify(message, level ='info'):
    """Log a message and print notification to console."""
    if level == 'info':
        logging.info(message)
        print(f"INFO: {message  }")
    elif level == 'error':
        logging.error(message)
        print(f"ERROR: {message}")
        sys.exit(1)  # Exit the script on error

def read_csv_files(directory):
    csv_files = {}
    for filename in os.listdir(directory):
        if filename.endswith(".csv"): 
            file_path = os.path.join(directory, filename)
            df = pd.read_csv(file_path)
            file_key = os.path.splitext(filename)[0]
            csv_files[file_key] = df
    return csv_files

def load_to_lake(df, ODBC_type, server_name, database_name, user_name, user_password, table_name_lake):
    try:
        conn = pyodbc.connect(
            f'DRIVER={ODBC_type};'
            f'SERVER={server_name};'
            f'DATABASE={database_name};'
            f'UID={user_name};'
            f'PWD={user_password}'
        )
        cursor = conn.cursor()

        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name_lake} (
                Date datetime,
                Store_ID INT,
                Product_ID VARCHAR(10),
                Quantity_Sold INT,
                Sales_Amount INT
            )
        ''')
        conn.commit()
        df.to_sql(table_name_lake, conn, if_exists='append', index=False, method='multi')
        
    except Exception as e:
        logging.error(f"Error loading data to lake: {e}")
    finally:
        conn.close()

def transform(df):
    column_have_missing = ['Quantity_Sold','Sales_Amount']
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
    df[column_have_missing] = df[column_have_missing].fillna(0)
    df['Total_Sale'] = df['Quantity_Sold'] * df['Sales_Amount']
    df['Sales_Per_Product'] = np.where(df['Quantity_Sold'] != 0, df['Sales_Amount'] / df['Quantity_Sold'], 0)
    return df

def load_data_warehouse(df, ODBC_type, server_name, database_name, user_name, user_password, table_name):
    try:
        conn = pyodbc.connect(
            f'DRIVER={ODBC_type};'
            f'SERVER={server_name};'
            f'DATABASE={database_name};'
            f'UID={user_name};'
            f'PWD={user_password}'
        )
        cursor = conn.cursor()

        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                Date datetime,
                Store_ID INT,
                Product_ID VARCHAR(10),
                Quantity_Sold INT,
                Sales_Amount INT,
                Total_Sale FLOAT,
                Sales_Per_Product FLOAT
            )
        ''')
        conn.commit()
        df.to_sql(table_name, conn, if_exists='append', index=False, method='multi')
        
    except Exception as e:
        logging.error(f"Error loading data to warehouse: {e}")
    finally:
        conn.close()

def main():
    directory = 'path/to/csv/files'
    ODBC_type = 'ODBC Driver 17 for SQL Server'
    server_name = 'your_server_name'
    database_name = 'your_database_name'
    user_name = 'your_username'
    user_password = 'your_password'
    table_name_lake = 'LakeTable'
    table_name = 'DataWarehouse'
    
    csv_data = read_csv_files(directory)
    for file_key, df in csv_data.items():
        load_to_lake(df, ODBC_type, server_name, database_name, user_name, user_password, table_name_lake)
        df_trans = transform(df)
        load_data_warehouse(df_trans, ODBC_type, server_name, database_name, user_name, user_password, table_name)

if __name__ == "__main__":
    main()
