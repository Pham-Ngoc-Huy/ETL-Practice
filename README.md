Explanation:
Row-Level Logging with log_each_row Function:

log_each_row(df, step_name): This function iterates over each row of the DataFrame df and logs its content. The step_name parameter specifies the current step in the ETL process (e.g., "Loading to Data Lake" or "Transforming Data").
The function uses DataFrame.iterrows() to access each row as a Series and converts it to a dictionary for easier logging.
Logging in Each Step:

load_to_data_lake(): Logs each row before loading the data into the data lake.
transform_data(): Logs each row of the transformed data.
load_to_data_warehouse(): Logs each row before loading the transformed data into the data warehouse.
Logging Output:

Each row's data is logged both in the log file (etl_process.log) and printed to the console for real-time monitoring.
Error Handling:

If any error occurs during the ETL process, it is logged as an error, printed to the console, and the script exits.
Running the Script:
Ensure your CSV file is in the correct path (/Users/thaongan/Documents/Python/ETL-PRACTICE/sales_data.csv).

Replace the placeholders in the connection string with your actual SQL Server details.

Execute the script by running the command:

bash
python /Users/thaongan/Documents/Python/ETL-PRACTICE/etl_script.py
Monitor the console output for real-time row-level logs, and check the etl_process.log file for a full record of the process.

This script gives you detailed insights into the ETL process at a granular level, allowing you to track the status of each row as it moves through the pipeline.