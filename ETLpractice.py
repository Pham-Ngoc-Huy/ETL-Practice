# read the file 
import pandas as pd

# Load the CSV file into a DataFrame
file_path = 'path/to/your/sales_data.csv'
df = pd.read_csv(file_path)

# Preview the data
print(df.head())

# set up server

import pyodbc

# Define the connection string
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=your_server_name;'
    'DATABASE=your_database_name;'
    'UID=your_username;'
    'PWD=your_password'
)

cursor = conn.cursor()

# create data lake server

from sqlalchemy import create_engine

# Create an engine to connect to SQL Server
engine = create_engine('mssql+pyodbc://your_username:your_password@your_server_name/your_database_name?driver=ODBC+Driver+17+for+SQL+Server')

# Load data into the SalesDataLake table
df.to_sql('SalesDataLake', con=engine, if_exists='append', index=False)

