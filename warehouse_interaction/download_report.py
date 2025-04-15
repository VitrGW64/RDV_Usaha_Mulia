import mysql.connector
import logging
from config import config  # Import database configuration
import pandas as pd
import os  # For file operations

LOG_FILE = "download_reports.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def create_dw_connection():
    """Establishes a connection to the data warehouse."""
    try:
        dw_config = config.dw_config
        connection = mysql.connector.connect(**dw_config)
        logging.info("Connected to data warehouse.")
        return connection
    except mysql.connector.Error as err:
        logging.error(f"Error connecting to data warehouse: {err}")
        return None

def download_restocking_data(connection, gudang_id, download_path="."):
    """
    Downloads data needed for restocking from the data warehouse.

    Args:
        connection:  MySQL connection to the data warehouse.
        gudang_id:   The ID of the Gudang downloading the data.
        download_path: The directory to save the downloaded data.
    """

    try:
        #  This is a simplified example.  A real-world query would be much more complex,
        #  considering current inventory, sales trends, etc.
        query = """
            SELECT
                d.minimart_id,
                d.minimart_nama,
                b.barang_nama,
                SUM(f.quantity_sold) as total_sold
            FROM
                fact_sales f
            JOIN
                dim_minimart d ON f.minimart_id = d.minimart_id
            JOIN
                dim_barang b ON f.barang_id = b.barang_id
            WHERE
                d.gudang_id = %s  -- Filter by the Gudang
            GROUP BY
                d.minimart_id, d.minimart_nama, b.barang_nama
            ORDER BY
                d.minimart_id, total_sold DESC
        """

        df = pd.read_sql(query, connection, params=(gudang_id,))

        if not df.empty:
            filename = f"restock_data_gudang_{gudang_id}.csv"
            filepath = os.path.join(download_path, filename)
            df.to_csv(filepath, index=False)
            logging.info(f"Downloaded restocking data for Gudang {gudang_id} to {filepath}")
            return filepath
        else:
            logging.info(f"No restocking data found for Gudang {gudang_id}")
            return None

    except mysql.connector.Error as err:
        logging.error(f"Error downloading restocking data: {err}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None

if __name__ == '__main__':
    dw_conn = create_dw_connection()
    if dw_conn:
        #  Simulate running this for a specific Gudang (e.g., Gudang ID 1)
        gudang_id_to_download = 1
        download_path = "reports"  #  Directory to save reports
        os.makedirs(download_path, exist_ok=True)  # Create the directory if it doesn't exist

        downloaded_file = download_restocking_data(dw_conn, gudang_id_to_download, download_path)

        if downloaded_file:
            print(f"Restocking data downloaded to: {downloaded_file}")
        else:
            print(f"No restocking data downloaded for Gudang {gudang_id_to_download}")

        dw_conn.close()
    else:
        print("Failed to connect to the data warehouse.")