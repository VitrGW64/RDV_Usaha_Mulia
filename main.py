import logging
import schedule
import time
import datetime
import subprocess  # To run other Python scripts
from config import config  # Import configurations

LOG_FILE = "main.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def run_extract_transform_load():
    """Runs the extract, transform, and load processes."""
    try:
        logging.info("Starting ETL process.")

        #  Run extract.py
        subprocess.run(["python", "extract.py"], check=True)
        logging.info("Extract completed.")

        #  Run transform.py
        subprocess.run(["python", "transform.py"], check=True)
        logging.info("Transform completed.")

        #  Run load.py
        subprocess.run(["python", "load.py"], check=True)
        logging.info("Load completed.")

        logging.info("ETL process completed successfully.")

    except subprocess.CalledProcessError as e:
        logging.error(f"Error running ETL process: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during ETL: {e}")

def run_download_reports():
    """Runs the download_reports.py script."""
    try:
        logging.info("Running download_reports.py")
        subprocess.run(["python", "download_reports.py"], check=True)
        logging.info("download_reports.py completed.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running download_reports.py: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

def run_generate_delivery():
    """Runs the generate_delivery.py script."""
    try:
        logging.info("Running generate_delivery.py")
        subprocess.run(["python", "generate_delivery.py"], check=True)
        logging.info("generate_delivery.py completed.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running generate_delivery.py: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

def run_send_investor_reports():
    """Runs the send_investor_reports.py script."""
    try:
        logging.info("Running send_investor_reports.py")
        subprocess.run(["python", "send_investor_reports.py"], check=True)
        logging.info("send_investor_reports.py completed.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running send_investor_reports.py: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    #  Schedule the ETL process to run (e.g., daily)
    schedule.every().day.at("02:00").do(run_extract_transform_load)  #  Example: Run at 2 AM

    #  Schedule the download reports process (e.g., every 6 hours)
    schedule.every(6).hours.do(run_download_reports)

    #  Schedule the generate delivery plan (e.g., after download reports)
    schedule.every(6).hours.do(run_generate_delivery) # Run after download

    #  Schedule the investor reports to run at 10 PM
    schedule.every().day.at("22:00").do(run_send_investor_reports)

    while True:
        schedule.run_pending()
        time.sleep(60)  #  Check for scheduled tasks every minute