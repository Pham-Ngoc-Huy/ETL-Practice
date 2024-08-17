import pandas as pd
import pyodbc
import logging
from sqlalchemy import create_engine, exc
import sys

# Configure logging
logging.basicConfig(
    filename='etl_process.log',
    level=logging.INFO,
    format='%(asctime)s py- %(levelname)s - %(message)s'
)

def log_and_notify(message, level='info'):
    """Log a message and print notification to console."""
    if level == 'info':
        logging.info(message)
        print(f"INFO: {message  }")
    elif level == 'error':
        logging.error(message)
        print(f"ERROR: {message}")
        sys.exit(1)  # Exit the script on error

def extract_data(file_path):
    """Extract data from a local CSV file."""
    try:
        df = pd.read_csv(file_path)
        log_and_notify(f"Successfully extracted data from {file_path}")
        return df
    except Exception as e:
        log_and_notify(f"Failed to extract data from {file_path}. Error: {e}", level='error')

def log_each_row(df, step_name):
    """Log information for each row during the ETL process."""
    for index, row in df.iterrows():
        log_and_notify(f"{step_name} - Row {index + 1}: {row.to_dict()}")

def load_to_data_lake(df, engine):
    """Load data into the SQL Server data lake (staging table)."""
    try:
        log_each_row(df, "Loading to Data Lake")
        df.to_sql('SalesDataLake', con=engine, if_exists='append', index=False)
        log_and_notify("Successfully loaded data into the SalesDataLake table.")
    except exc.SQLAlchemyError as e:
        log_and_notify(f"Failed to load data into SalesDataLake table. Error: {e}", level='error')

def transform_data(df):
    """Transform the data (e.g., aggregation)."""
    try:
        df['Total_Sales'] = df['Units_Sold'] * df['Unit_Price']
        df_agg = df.groupby(['Date', 'Product_ID']).agg({
            'Units_Sold': 'sum',
            'Total_Sales': 'sum'
        }).reset_index()
        log_and_notify("Successfully transformed the data.")
        log_each_row(df_agg, "Transforming Data")
        return df_agg
    except Exception as e:
        log_and_notify(f"Failed to transform the data. Error: {e}", level='error')

def load_to_data_warehouse(df_agg, engine):
    """Load transformed data into the SQL Server data warehouse."""
    try:
        log_each_row(df_agg, "Loading to Data Warehouse")
        df_agg.to_sql('SalesDataWarehouse', con=engine, if_exists='append', index=False)
        log_and_notify("Successfully loaded data into the SalesDataWarehouse table.")
    except exc.SQLAlchemyError as e:
        log_and_notify(f"Failed to load data into SalesDataWarehouse table. Error: {e}", level='error')

def main():
    # File path to the CSV file
    file_path = '/Users/thaongan/Documents/Python/ETL-PRACTICE/sales_data.csv'

    # Database connection details
    connection_string = (
        'mssql+pyodbc://your_username:your_password@your_server_name/your_database_name'
        '?driver=ODBC+Driver+17+for+SQL+Server'
    )

    # Create a database engine
    try:
        engine = create_engine(connection_string)
        log_and_notify("Successfully connected to the SQL Server database.")
    except exc.SQLAlchemyError as e:
        log_and_notify(f"Failed to connect to SQL Server. Error: {e}", level='error')

    # ETL Process
    df = extract_data(file_path)
    load_to_data_lake(df, engine)
    df_agg = transform_data(df)
    load_to_data_warehouse(df_agg, engine)

if __name__ == "__main__":
    main()
