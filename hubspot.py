from db.session import hostname, username, password, host, user, passwd, port
import os
import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np
from ftplib import FTP_TLS
import glob
import time
from datetime import datetime
import logging


def setup_logger():
    """Set up logging with a unique log file based on the current timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f'ingestion_hubspot_{timestamp}.log'
    logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Logging started.")

def download_file(ftp, file_path, save_directory):
    """Download the specified file from the FTPS server to the local directory."""
    local_file_path = os.path.join(save_directory, os.path.basename(file_path))  # Save in the specified directory
    with open(local_file_path, 'wb') as f:
        ftp.retrbinary(f"RETR {file_path}", f.write)
    logging.info(f"Downloaded file: {local_file_path}")
    return local_file_path

def convert_to_xlsx(file_path, save_directory):
    """Convert .csv or .xls to .xlsx and save to the specified directory."""
    file_extension = os.path.splitext(file_path)[1].lower()
    if file_extension == '.csv':
        df = pd.read_csv(file_path)
        new_file_path = os.path.join(save_directory, os.path.basename(file_path).replace('.csv', '.xlsx'))
        df.to_excel(new_file_path, index=False)
        logging.info(f"Converted {file_path} to {new_file_path}")
        return new_file_path
    elif file_extension == '.xls':
        df = pd.read_excel(file_path)
        new_file_path = os.path.join(save_directory, os.path.basename(file_path).replace('.xls', '.xlsx'))
        df.to_excel(new_file_path, index=False)
        logging.info(f"Converted {file_path} to {new_file_path}")
        return new_file_path
    return file_path  # If already .xlsx, return the original path

def read_data(file_path):
    """Read data from a .csv or .xlsx file into a pandas DataFrame and adjust data types."""
    file_extension = os.path.splitext(file_path)[1].lower()

    if file_extension == '.csv':
        df = pd.read_csv(file_path)
        logging.info(f"Read {len(df)} rows from {file_path}.")
    elif file_extension in ['.xls', '.xlsx']:
        df = pd.read_excel(file_path)
        logging.info(f"Read {len(df)} rows from {file_path}.")
    else:
        raise ValueError("Unsupported file type: Only .csv and .xlsx files are supported.")

    # Adjust data types for 'create_date'
    if 'create_date' in df.columns:
        print("Original 'create_date' values:")
        print(df['create_date'].head())  # Display first few values

        # Convert 'create_date' to datetime format
        df['create_date'] = pd.to_datetime(df['create_date'], errors='coerce')
        
        # Check for conversion issues
        if df['create_date'].isnull().any():
            problematic_dates = df['create_date'][df['create_date'].isnull()]

        # Convert to MySQL compatible format if necessary
        df['create_date'] = df['create_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    return df

def adjust_amounts_by_currency(df):
    """Adjust amounts based on currency."""
    # Check if the necessary columns exist
    if 'Amount' in df.columns and 'Currency' in df.columns:
        # Adjust Amount based on Currency
        df['Amount'] = np.where(df['Currency'] == 'GBP', df['Amount'] * 1.25, df['Amount'])
        
        # Replace GBP with USD
        df['Currency'] = df['Currency'].replace('GBP', 'USD')
        logging.info("Adjusted Amounts based on Currency and replaced GBP with USD.")
    else:
        logging.info("Required columns 'Amount' and 'Currency' not found in DataFrame.")
    return df

def create_engine_connection(db_connection_string):
    """Create a SQLAlchemy engine and return the connection."""
    engine = create_engine(db_connection_string)
    return engine

def clean_dataframe(df):
    """Replace 'nan' and empty values with None."""
    df.replace({np.nan: None, '': None, pd.NA: None}, inplace=True)
    return df

def batch_upsert_data_to_sql(engine, df, table_name, unique_identifier, batch_size=100):
    conn = engine.connect()
    columns = ', '.join(f"`{col}`" for col in df.columns)
    updates = ', '.join(f"`{col}` = VALUES(`{col}`)" for col in df.columns if col != unique_identifier)
    
    for start in range(0, len(df), batch_size):
        batch = df.iloc[start:start + batch_size]
        values_list = []
        for _, row in batch.iterrows():
            values = ', '.join(f":{col}" for col in df.columns)
            values_list.append(f"({values})")
        
        upsert_query = f"""
        INSERT INTO `{table_name}` ({columns})
        VALUES {', '.join(values_list)}
        ON DUPLICATE KEY UPDATE {updates};
        """
        batch_data = batch.to_dict(orient='records')

        
        try:
            conn.execute(text(upsert_query), batch_data)
            logging.info(f"Batch upserted {len(batch)} records starting from index {start}.")
        except Exception as e:
            logging.info(f"Error during batch upsert: {e}")  # This will help identify any issues
    conn.close()

def log_ingestion(engine, source, table_name,file_name):
   """Log the ingestion process into the ingestion_log table."""
   conn = engine.connect()
   try:
       # Prepare the SQL insert statement
       insert_query = """
       INSERT INTO script_updates_tracking (source, table_name, last_updated_date, file_name)
       VALUES (:source, :table_name, :last_updated_date, :file_name);
       """
       # Prepare the data
       data = {
           'source': source,
           'table_name': table_name,
           'last_updated_date': datetime.now(),
           'file_name' : file_name
       }
       # Execute the insert statement
       conn.execute(text(insert_query), data)
       logging.info(f"Ingestion log entry added for table: {table_name}, source: {source}")
   except Exception as e:
       logging.info(f"Error logging ingestion: {e}")
   finally:
       conn.close()

def connect_to_ftp(host, user, passwd, port, max_retries=10, delay=5):
    """Connect to the FTPS server with retry logic."""
    port = int(port)
    ftp = FTP_TLS()
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            ftp.connect(host=host, port=port)
            ftp.login(user=user, passwd=passwd)
            ftp.set_pasv(True)
            ftp.prot_p()
            logging.info("Connected to FTPS server successfully.")
            return ftp  # Return the connection if successful
        except Exception as e:
            retry_count += 1
            logging.error(f"Attempt {retry_count} failed to connect to FTPS server: {e}")
            if retry_count < max_retries:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)  # Wait before retrying
            else:
                logging.error("Maximum retries reached. Could not connect to FTPS server.")
                raise ConnectionError(f"Could not connect to FTPS server after {max_retries} attempts.")
            
def reconnect_to_ftp(host, user, passwd, port):
    """Re-establish the FTPS connection."""
    try:
        ftp = connect_to_ftp(host, user, passwd, port)  # Attempt to reconnect
        logging.info("Reconnected to FTPS server.")
        return ftp
    except ConnectionError as e:
        logging.error(f"Error during reconnection: {e}")
        raise

def list_files_with_retry(ftp, max_retries=10, delay=5):
    """List files in the current directory with retries if the connection fails."""
    retry_count = 0
    while retry_count < max_retries:
        try:
            file_list = ftp.nlst()  # List files in the current directory
            logging.info(f"Listed files in the directory: {file_list}")
            return file_list  # Return file list if successful
        except Exception as e:
            retry_count += 1
            logging.error(f"Attempt {retry_count} failed to list files: {e}")
            if retry_count < max_retries:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)  # Wait before retrying
            else:
                logging.error("Maximum retries reached for listing files.")
                raise  # Raise error if maximum retries are reached

def download_file_with_retry(ftp, file_path, save_directory, max_retries=10, delay=5):
    """Download a file with retries if the connection fails during download."""
    retry_count = 0
    while retry_count < max_retries:
        try:
            local_file_path = os.path.join(save_directory, os.path.basename(file_path))  # Save in the specified directory
            with open(local_file_path, 'wb') as f:
                ftp.retrbinary(f"RETR {file_path}", f.write)
            logging.info(f"Downloaded file: {local_file_path}")
            return local_file_path  # Return the local file path if successful
        except Exception as e:
            retry_count += 1
            logging.error(f"Attempt {retry_count} failed to download file {file_path}: {e}")
            if retry_count < max_retries:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)  # Wait before retrying
            else:
                logging.error("Maximum retries reached for downloading file.")
                raise  # Raise error if maximum retries are reached

def filter_columns_by_mapping(df,column_mapping):
    matching_columns = [col for col in df.columns if col in column_mapping]
    excluded_columns = [col for col in df.columns if col not in column_mapping]

    if excluded_columns:
        logging.info(f"Excluded columns: {excluded_columns}")
    else:
        logging.info('No Columns excluded')
    return df[matching_columns]

def main(host, user, passwd, port, db_connection_string, table_name, column_mapping, unique_identifier):
    setup_logger()
    # Specify the save directory (e.g., Desktop)
    save_directory = os.path.join(os.path.expanduser("~"), "Desktop")

    # Connect to FTPS server with retry logic
    try:
        ftp = connect_to_ftp(host, user, passwd, port)
        current_dir = ftp.pwd()
        logging.info(f"Current directory: {current_dir}")
        
        # Attempt to change to the 'data' directory, retrying if it fails
        ftp.cwd('Dashboards/Hubspot')
        logging.info(f"Changed directory: {current_dir}")

        # List files with retries if connection issues occur during listing
        file_list = list_files_with_retry(ftp)

        # Include both .xlsx and .csv files
        excel_files = [file for file in file_list if file.endswith('.xlsx') or file.endswith('.csv')]

        if not excel_files:
            logging.info("No Excel or CSV files found in the FTPS directory.")
            return

        # Get modification times and find the latest file
        latest_file = None
        latest_time = None

        for file in excel_files:
            mdtm_response = ftp.sendcmd(f'MDTM {file}')
            modification_time = mdtm_response[4:].split('.')[0]  # Keep only the first part

            try:
                file_time = datetime.strptime(modification_time, '%Y%m%d%H%M%S')
            except ValueError:
                logging.warning(f"Failed to parse modification time for file: {file}")
                continue

            if latest_time is None or file_time > latest_time:
                latest_time = file_time
                latest_file = file

        if latest_file:
            # Download the latest file to the specified directory with retries
            downloaded_file_path = download_file_with_retry(ftp, latest_file, save_directory)

            if downloaded_file_path:
                # Convert the downloaded file to .xlsx if necessary
                if downloaded_file_path.endswith('.csv') or downloaded_file_path.endswith('.xls'):
                    converted_file_path = convert_to_xlsx(downloaded_file_path, save_directory)
                else:
                    converted_file_path = downloaded_file_path

                # Read the data
                df = read_data(converted_file_path)

                # Continue processing...
                df = filter_columns_by_mapping(df, column_mapping)

                # Adjust amounts based on currency
                df = adjust_amounts_by_currency(df)

                # Rename columns based on mapping
                df.rename(columns=column_mapping, inplace=True)

                # Clean the DataFrame
                df = clean_dataframe(df)

                # Create the SQLAlchemy engine
                engine = create_engine_connection(db_connection_string)

                # Perform batch upsert into the SQL table
                try:
                    batch_upsert_data_to_sql(engine, df, table_name, unique_identifier)
                except Exception as e:
                    logging.error(f"Error during batch upsert: {e}")

                # Log the ingestion process into the ingestion_log table
                log_ingestion(engine, source="Hubspot", table_name=table_name, file_name=latest_file)
                logging.info("Data ingestion completed successfully.")
            else:
                logging.error("File download failed.")
        else:
            logging.warning("No valid files found for processing.")

    except ConnectionError as e:
        logging.error(f"Could not connect to FTPS server: {e}")
        return

if __name__ == "__main__":
    # FTPS server credentials
    host = host
    user = user
    passwd = passwd 
    port = port
    # Database connection string
    db_connection_string = 'mysql+mysqlconnector://{}:{}@{}:3306/{}'.format(username, password, hostname, 'sample')
    # Database table name
    table_name = 'data'
    # Mapping of Excel columns to database columns
    column_mapping = {
    'Record ID'   : 'record_id', 
     

    }
    # Unique identifier for checking existing records
    unique_identifier = 'record_id'
    # Run the main function to ingest data
    main(host, user, passwd,port, db_connection_string, table_name, column_mapping, unique_identifier)

