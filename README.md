**FTPS Data Ingestion Script**
This Python script automates the process of downloading, processing, and ingesting data from an FTPS server into a MySQL database. It handles file processing, logging, error handling, and database upserts, making it suitable for ETL (Extract, Transform, Load) operations.
**Features**

**• Secure File Transfer:** Connects to an FTPS server using FTP_TLS for secure file downloads.

**• Automatic File Handling:**
◦ Identifies the latest .csv or .xlsx file in the FTPS directory.
◦ Converts .csv or .xls files to .xlsx format for consistency.

**• Data Transformation:**
◦ Adjusts amounts based on currency conversions (e.g., converts GBP to USD).
◦ Cleans and maps columns using customizable mappings.

• **Batch Upserts**: Inserts or updates data into a MySQL table in batches, ensuring efficient database operations.

• **Retry Logic**: Includes retries for FTPS connections and operations to handle transient failures.

• **Detailed Logging**: Tracks each step, including errors, for easier debugging.

**Prerequisites**
1 Python Dependencies: Install the required libraries using the following command:
bash
**Copy code**

pip install pandas numpy sqlalchemy mysql-connector-python ftplib openpyxl


**MySQL Database:**
◦ A MySQL database instance is required to ingest data.
◦ Ensure the script_updates_tracking table exists for logging ingestion updates.
FTPS Server: The script connects to an FTPS server to fetch data. Ensure valid credentials and correct directory paths.
**Setup and Configuration**
Clone the Repository:
bash
**Copy code**

git clone <repository_url>
cd <repository_name>


Environment Variables: Set the FTPS server and database credentials in a .env file or directly in the script:
bash
**Copy code**

hostname=<FTPS_SERVER_HOST>
username=<DB_USERNAME>
password=<DB_PASSWORD>
host=<FTPS_HOST>
user=<FTPS_USERNAME>
passwd=<FTPS_PASSWORD>
port=<FTPS_PORT>


**Column Mapping**: Update the column_mapping dictionary in the main() function to map your data columns to database columns:
python
**Copy code**

column_mapping = {
   'Record ID': 'record_id',
   'Amount': 'amount',
   'Currency': 'currency',
    'Create Date': 'create_date'
 }


**Database Connection String**: Modify the db_connection_string to match your MySQL configuration:
python
**Copy code**

db_connection_string = 'mysql+mysqlconnector://<username>:<password>@<hostname>:3306/<database>'


**Usage**
Run the script by executing:
bash
Copy code
python <script_name>.py

**Key Functions**
connect_to_ftp()
Establishes a secure FTPS connection with retry logic.
download_file_with_retry()
Downloads a file from the FTPS server with retries.
convert_to_xlsx()
Converts .csv or .xls files to .xlsx format.
read_data()
Reads the downloaded file into a Pandas DataFrame.
adjust_amounts_by_currency()
Adjusts the "Amount" column based on currency, converting GBP to USD.
batch_upsert_data_to_sql()
Performs batch upserts into the specified MySQL table.
log_ingestion()
Logs the ingestion process into the script_updates_tracking table.
