import mysql.connector
import pandas as pd
import logging
from config import config  # To get database connection details

LOG_FILE = "transformation.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def create_source_connection():
    """Establishes a connection to the source database (usaha_mulia)."""
    try:
        source_config = config.db_config  # Use db_config for the source
        connection = mysql.connector.connect(**source_config)
        logging.info("Connected to source database (usaha_mulia).")
        return connection
    except mysql.connector.Error as err:
        logging.error(f"Error connecting to source database: {err}")
        return None

def fetch_source_data(connection, table_name):
    """Fetches data from a table in the source database."""
    try:
        df = pd.read_sql(f"SELECT * FROM {table_name}", connection)
        logging.info(f"Fetched data from {table_name}")
        return df
    except mysql.connector.Error as err:
        logging.error(f"Error fetching data from {table_name}: {err}")
        return pd.DataFrame()

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

def load_to_staging(connection, df, table_name):
    """Loads a Pandas DataFrame into a staging table."""

    if connection is None or df.empty:
        logging.warning(f"No connection or empty DataFrame. Not loading to {table_name}")
        return

    cursor = connection.cursor()
    try:
        cols = ",".join(df.columns)
        placeholders = ",".join(['%s'] * len(df.columns))
        sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
        values = [tuple(x) for x in df.to_numpy()]
        cursor.executemany(sql, values)
        connection.commit()
        logging.info(f"{len(df)} rows inserted into {table_name}")
    except mysql.connector.Error as err:
        logging.error(f"Error inserting into {table_name}: {err}")
        connection.rollback()
    finally:
        cursor.close()

def transform_transactions_data(df_transaksi, df_isi_transaksi):
    """Transforms the transactions data and loads it into staging tables."""

    try:
        # 1. Calculate total_amount from isi_transaksi
        df_isi_transaksi['item_total'] = df_isi_transaksi['isi_transaksi_jumlah'] * df_isi_transaksi['harga_satuan']
        df_total_amount = df_isi_transaksi.groupby('transaksi_id')['item_total'].sum().reset_index()
        logging.info("Calculated total_amount from isi_transaksi.")

        # 2. Join with transaksi table
        df_transformed = pd.merge(df_transaksi, df_total_amount, on='transaksi_id', how='left')
        logging.info("Joined transaksi and isi_transaksi data.")

        # 3. Convert 'tanggal_waktu' to datetime objects
        if 'tanggal_waktu' in df_transformed.columns:
            df_transformed['transaction_datetime'] = pd.to_datetime(df_transformed['tanggal_waktu'])
            logging.info("Converted 'tanggal_waktu' to datetime.")
        else:
            logging.warning("Column 'tanggal_waktu' not found. Transformation skipped.")
            return pd.DataFrame()

        # 4. Calculate profit (assuming total_amount is profit)
        df_transformed['profit'] = df_transformed['item_total']
        logging.info("Calculated 'profit'.")

        # 5. Extract 'hour_of_day'
        if 'transaction_datetime' in df_transformed.columns:
            df_transformed['hour_of_day'] = df_transformed['transaction_datetime'].dt.hour
            logging.info("Extracted 'hour_of_day'.")
        else:
            logging.warning("Column 'transaction_datetime' not found. Hour extraction skipped.")
            df_transformed['hour_of_day'] = None

        # 6. Rename columns for clarity
        df_transformed = df_transformed.rename(columns={
            'transaksi_id': 'transaction_id',
            'minimart_id': 'minimart_id',
            'pegawai_id': 'cashier_id',
            'tanggal_waktu': 'original_transaction_datetime',  # Renamed original datetime
            'transaksi_total': 'original_total_amount',  # Renamed original total
            'transaksi_pembayaran': 'payment_amount',
            'transaksi_kembalian': 'change_amount',
            'item_total': 'total_amount'  # The calculated total
        })
        logging.info("Renamed columns.")

        # 7. Handle missing values
        df_transformed = df_transformed.fillna(0)
        logging.warning("Handled missing values by filling with 0.")

        # 8. Remove Duplicates
        df_transformed = df_transformed.drop_duplicates(subset=['transaction_id'])
        logging.info("Removed duplicate transactions.")

        return df_transformed

    except Exception as e:
        logging.error(f"An error occurred during transformation: {e}")
        return pd.DataFrame()

if __name__ == '__main__':
    #  Establish connections to both databases
    source_conn = create_source_connection()
    staging_conn = create_staging_connection()

    if source_conn and staging_conn:
        #  Fetch data from the source database
        df_transaksi = fetch_source_data(source_conn, "transaksi")
        df_isi_transaksi = fetch_source_data(source_conn, "isi_transaksi")

        if not df_transaksi.empty and not df_isi_transaksi.empty:
            #  Transform the data
            transformed_df = transform_transactions_data(df_transaksi, df_isi_transaksi)

            if not transformed_df.empty:
                #  Load the transformed data into staging tables
                load_to_staging(staging_conn, transformed_df[['transaction_id', 'minimart_id', 'cashier_id', 'original_transaction_datetime',
                                                             'original_total_amount', 'payment_amount', 'change_amount', 'total_amount',
                                                             'profit', 'hour_of_day']], 'staging_transaksi')  # Load relevant columns

                load_to_staging(staging_conn, transformed_df[['transaction_id', 'barang_id']], 'staging_isi_transaksi')  # Load relevant columns

                print("Transformed data loaded into staging tables.")
            else:
                print("Transformation resulted in an empty DataFrame. Nothing loaded.")
        else:
            print("Could not fetch data from source tables.")

        #  Close connections
        source_conn.close()
        staging_conn.close()
    else:
        print("Failed to connect to one or both databases.")