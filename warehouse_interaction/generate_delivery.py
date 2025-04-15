import pandas as pd
import logging

LOG_FILE = "generate_delivery.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def generate_delivery_plan(restock_data_file, gudang_id):
    """
    Generates a delivery plan for minimarts based on restock data.

    Args:
        restock_data_file:  Path to the CSV file containing restock data.
        gudang_id:          The ID of the Gudang generating the plan.

    Returns:
        A Pandas DataFrame representing the delivery plan.
    """

    try:
        df_restock = pd.read_csv(restock_data_file)
        logging.info(f"Read restock data from {restock_data_file}")

        #  ---  Delivery Logic  ---
        #  This is where the core logic for deciding *how much* to deliver goes.
        #  It's highly dependent on your business rules!
        #  Here's a simplified example:

        #  1. Calculate a "target stock" (e.g., based on average sales)
        df_restock['target_stock'] = df_restock['total_sold'] * 2  # Example: 2 times the total sold

        #  2. Determine the quantity to deliver
        df_restock['quantity_to_deliver'] = df_restock['target_stock'] - df_restock['current_stock']  # Assuming 'current_stock' is in restock_data
        df_restock.loc[df_restock['quantity_to_deliver'] < 0, 'quantity_to_deliver'] = 0  # Don't deliver negative quantities

        #  3. Create the delivery plan DataFrame
        delivery_plan = df_restock[['minimart_id', 'barang_nama', 'quantity_to_deliver']].copy()
        delivery_plan['gudang_id'] = gudang_id

        logging.info("Generated delivery plan.")
        return delivery_plan

    except FileNotFoundError:
        logging.error(f"Restock data file not found: {restock_data_file}")
        return pd.DataFrame()
    except KeyError as e:
         logging.error(f"Missing column in restock data: {e}")
         return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error generating delivery plan: {e}")
        return pd.DataFrame()

def save_delivery_plan(delivery_plan, gudang_id, output_path="."):
    """
    Saves the delivery plan to a CSV file.

    Args:
        delivery_plan:  Pandas DataFrame representing the delivery plan.
        gudang_id:      The ID of the Gudang.
        output_path:    The directory to save the delivery plan.
    """

    try:
        filename = f"delivery_plan_gudang_{gudang_id}.csv"
        filepath = os.path.join(output_path, filename)
        delivery_plan.to_csv(filepath, index=False)
        logging.info(f"Saved delivery plan to {filepath}")
        return filepath
    except Exception as e:
        logging.error(f"Error saving delivery plan: {e}")
        return None

if __name__ == '__main__':
    gudang_id = 1  #  Example Gudang ID
    restock_data_file = "reports/restock_data_gudang_1.csv"  #  Path to the downloaded data

    delivery_plan = generate_delivery_plan(restock_data_file, gudang_id)

    if not delivery_plan.empty:
        output_path = "deliveries"
        os.makedirs(output_path, exist_ok=True)
        saved_file = save_delivery_plan(delivery_plan, gudang_id, output_path)
        if saved_file:
            print(f"Delivery plan generated and saved to: {saved_file}")
        else:
            print("Error saving delivery plan.")
    else:
        print("Could not generate delivery plan.")