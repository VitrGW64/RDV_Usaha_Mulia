import mysql.connector
import logging
from config import config  # Import database configuration
import pandas as pd

LOG_FILE = "loading.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def create_dw_connection():
    """Establishes a connection to the data warehouse."""
    try:
        dw_config = config.dw_config  # Use separate config for DW
        connection = mysql.connector.connect(**dw_config)
        logging.info("Connected to data warehouse successfully.")
        return connection
    except mysql.connector.Error as err:
        logging.error(f"Error connecting to data warehouse: {err}")
        return None

def create_staging_connection():
    """Establishes a connection to the staging database."""
    try:
        staging_config = config.staging_config
        connection = mysql.connector.connect(**staging_config)
        logging.info("Connected to staging database.")
        return connection
    except mysql.connector.Error as err:
        logging.error(f"Error connecting to staging database: {err}")
        return None

def load_data_to_dw(connection, df, table_name):
    """Loads data from a Pandas DataFrame into a table in the data warehouse."""
    if connection is None:
        logging.error("No database connection. Load operation aborted.")
        return

    cursor = connection.cursor()
    try:
        cols = ",".join(df.columns)
        placeholders = ",".join(['%s'] * len(df.columns))
        sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
        values = [tuple(x) for x in df.to_numpy()]
        cursor.executemany(sql, values)
        connection.commit()
        logging.info(f"{len(df)} records loaded into {table_name}.")
    except mysql.connector.Error as err:
        logging.error(f"Error loading data into {table_name}: {err}")
        connection.rollback()
    finally:
        cursor.close()

def load_from_staging_to_dw(dw_conn, staging_conn):
    """Loads transformed data from staging tables into the data warehouse."""

    try:
        # 1. Load into dim_minimart
        dim_minimart_df = pd.read_sql("SELECT DISTINCT minimart_id, minimart_nama, kota_id, gudang_id, minimart_alamat FROM staging_transaksi", staging_conn)
        load_data_to_dw(dw_conn, dim_minimart_df, 'dim_minimart')
        logging.info("Loaded data into dim_minimart")

        # 2. Load into dim_cashier
        dim_cashier_df = pd.read_sql("SELECT DISTINCT cashier_id, pegawai_nama, minimart_id FROM staging_transaksi", staging_conn)
        dim_cashier_df = dim_cashier_df.rename(columns={'pegawai_nama': 'cashier_nama'})
        load_data_to_dw(dw_conn, dim_cashier_df, 'dim_cashier')
        logging.info("Loaded data into dim_cashier")

        # 3. Load into dim_waktu
        dim_waktu_df = pd.read_sql("SELECT DISTINCT DATE(original_transaction_datetime) as tanggal, HOUR(original_transaction_datetime) as jam, DAYNAME(original_transaction_datetime) as hari, WEEK(original_transaction_datetime) as minggu, MONTH(original_transaction_datetime) as bulan, YEAR(original_transaction_datetime) as tahun, UNIX_TIMESTAMP(original_transaction_datetime) as waktu_id FROM staging_transaksi", staging_conn)
        load_data_to_dw(dw_conn, dim_waktu_df, 'dim_waktu')
        logging.info("Loaded data into dim_waktu")

        # 4. Load into fact_sales
        fact_sales_df = pd.read_sql("SELECT transaction_id, minimart_id, cashier_id, UNIX_TIMESTAMP(original_transaction_datetime) as waktu_id, original_total_amount, payment_amount, change_amount, total_amount, profit, original_transaction_datetime as sales_datetime FROM staging_transaksi", staging_conn)
        load_data_to_dw(dw_conn, fact_sales_df, 'fact_sales')
        logging.info("Loaded data into fact_sales")

    except mysql.connector.Error as err:
        logging.error(f"Error loading data from staging to DW: {err}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

def main():
    """Main function to orchestrate the loading process from staging to DW."""
    dw_conn = create_dw_connection()
    staging_conn = create_staging_connection()

    if dw_conn and staging_conn:
        load_from_staging_to_dw(dw_conn, staging_conn)

        dw_conn.close()
        staging_conn.close()
        logging.info("DW and Staging connections closed.")
    else:
        logging.error("Failed to connect to DW or Staging.")

if __name__ == "__main__":
    main()