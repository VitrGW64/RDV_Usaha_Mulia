import mysql.connector
import logging
from config import config  # Import database configuration
import pandas as pd
import smtplib  # For sending emails
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

LOG_FILE = "send_investor_reports.log"
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

def get_investor_emails(connection):
    """
    Retrieves a dictionary of minimart IDs and their corresponding investor emails.
    (This assumes you have investor emails in the dim_minimart table)

    Args:
        connection: MySQL connection to the data warehouse.

    Returns:
        A dictionary where keys are minimart_ids and values are investor emails.
        Returns an empty dictionary on error.
    """
    try:
        query = "SELECT minimart_id, pemilik_email FROM dim_minimart JOIN pemilik ON dim_minimart.pemilik_id = pemilik.pemilik_id"
        df = pd.read_sql(query, connection)
        email_dict = dict(zip(df['minimart_id'], df['pemilik_email']))
        logging.info("Retrieved investor emails.")
        return email_dict
    except mysql.connector.Error as err:
        logging.error(f"Error retrieving investor emails: {err}")
        return {}
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return {}

def generate_daily_summary(connection, minimart_id):
    """
    Generates a daily summary report for a specific minimart.

    Args:
        connection: MySQL connection to the data warehouse.
        minimart_id: The ID of the minimart.

    Returns:
        A string containing the summary report.
    """

    try:
        query = """
            SELECT
                b.barang_nama,
                SUM(f.quantity_sold) as total_quantity_sold,
                SUM(f.total_amount) as total_revenue,
                d.cashier_nama,
                COUNT(DISTINCT f.transaction_id) as total_transactions,
                SUM(f.profit) as total_profit,
                w.jam,
                SUM(f.total_amount) as hourly_revenue
            FROM
                fact_sales f
            JOIN
                dim_barang b ON f.barang_id = b.barang_id
            JOIN
                dim_cashier d ON f.cashier_id = d.cashier_id
            JOIN
                dim_waktu w ON f.waktu_id = w.waktu_id
            WHERE
                f.minimart_id = %s AND DATE(f.sales_datetime) = CURDATE()  -- Today's data
            GROUP BY
                b.barang_nama, d.cashier_nama, w.jam
            ORDER BY
                total_revenue DESC
        """

        df = pd.read_sql(query, connection, params=(minimart_id,))

        if not df.empty:
            report = f"Daily Summary for Minimart {minimart_id} ({pd.to_datetime('today').strftime('%Y-%m-%d')}):\n\n"
            report += "Top 10 Selling Items:\n"
            report += df.head(10)[['barang_nama', 'total_quantity_sold', 'total_revenue']].to_string(index=False) + "\n\n"

            report += "\nCashier Performance:\n"
            report += df.groupby('cashier_nama').agg({'total_transactions': 'sum', 'total_revenue': 'sum'}).to_string() + "\n\n"

            report += "\nHourly Revenue:\n"
            report += df.groupby('jam')['hourly_revenue'].sum().to_string() + "\n\n"

            report += f"\nTotal Profit: {df['total_profit'].sum()}\n"

            logging.info(f"Generated daily summary for Minimart {minimart_id}")
            return report
        else:
            logging.info(f"No sales data found for Minimart {minimart_id} today.")
            return f"No sales data available for Minimart {minimart_id} today."

    except mysql.connector.Error as err:
        logging.error(f"Error generating daily summary: {err}")
        return f"Error generating report: {err}"
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return f"Error generating report: {e}"

def send_email(to_email, subject, body):
    """
    Sends an email.  (Replace with your actual email sending setup)

    Args:
        to_email:  The recipient's email address.
        subject:   The email subject.
        body:      The email body (the report).
    """

    try:
        #  Replace with your email server details
        sender_email = "your_company_email@example.com"
        sender_password = "your_email_password"

        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, to_email, msg.as_string())

        logging.info(f"Sent email to {to_email}")
        print(f"Sent email to {to_email}")  #  Inform the console
    except smtplib.SMTPException as err:
        logging.error(f"SMTP error sending email to {to_email}: {err}")
    except Exception as e:
        logging.error(f"An unexpected error occurred sending email to {to_email}: {e}")

if __name__ == '__main__':
    dw_conn = create_dw_connection()
    if dw_conn:
        investor_emails = get_investor_emails(dw_conn)
        if investor_emails:
            for minimart_id, investor_email in investor_emails.items():
                report = generate_daily_summary(dw_conn, minimart_id)
                subject = f"Daily Summary Report for Minimart {minimart_id}"
                send_email(investor_email, subject, report)
        else:
            logging.warning("Could not retrieve investor emails. Reports not sent.")
            print("Could not retrieve investor emails. Reports not sent.")
        dw_conn.close()
    else:
        logging.error("Failed to connect to the data warehouse.")
        print("Failed to connect to the data warehouse.")