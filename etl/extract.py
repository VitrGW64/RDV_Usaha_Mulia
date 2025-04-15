import mysql.connector
import os
import schedule
import time
from datetime import datetime
import glob
from etl_config import config
import logging
import pandas as pd

LOG_FILE = "extraction.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def create_db_connection():
    """Establishes a connection to the MySQL database."""

    try:
        connection = mysql.connector.connect(**config.db_config)
        logging.info("Database connection established successfully.")
        return connection
    except mysql.connector.Error as err:
        logging.error(f"Error connecting to database: {err}")
        return None

def extract_transactions(connection):
    """Extracts data from the transaksi table."""

    try:
        df_transaksi = pd.read_sql("SELECT * FROM transaksi", connection)
        logging.info("Data extracted from transaksi table.")
        return df_transaksi
    except mysql.connector.Error as err:
        logging.error(f"Error extracting data from transaksi: {err}")
        return pd.DataFrame()

def extract_isi_transaksi(connection):
    """Extracts data from the isi_transaksi table."""

    try:
        df_isi_transaksi = pd.read_sql("SELECT * FROM isi_transaksi", connection)
        logging.info("Data extracted from isi_transaksi table.")
        return df_isi_transaksi
    except mysql.connector.Error as err:
        logging.error(f"Error extracting data from isi_transaksi: {err}")
        return pd.DataFrame()

def run_extraction():
    """Orchestrates the extraction process."""

    now = datetime.now()
    logging.info(f"Running extraction at {now}")

    connection = create_db_connection()
    if connection:
        df_transaksi = extract_transactions(connection)
        df_isi_transaksi = extract_isi_transaksi(connection)

        if not df_transaksi.empty:
            # For now, let's save them to temporary files
            timestamp = now.strftime("%Y%m%d_%H%M%S")
            temp_file_transaksi = f"extracted_transaksi_{timestamp}.csv"
            df_transaksi.to_csv(temp_file_transaksi, index=False)
            logging.info(f"Saved transaksi data to {temp_file_transaksi}")

        if not df_isi_transaksi.empty:
            temp_file_isi_transaksi = f"extracted_isi_transaksi_{timestamp}.csv"
            df_isi_transaksi.to_csv(temp_file_isi_transaksi, index=False)
            logging.info(f"Saved isi_transaksi data to {temp_file_isi_transaksi}")

        connection.close()
        logging.info("Database connection closed.")
    else:
        logging.error("Database connection failed. Extraction aborted.")

if __name__ == '__main__':
    schedule.every().hour.do(run_extraction)

    while True:
        schedule.run_pending()
        time.sleep(60)