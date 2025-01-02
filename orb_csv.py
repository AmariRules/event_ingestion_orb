import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta
from orb import Orb

# Load environment variables
load_dotenv()

def create_backfill(orb_client, events):
    """
    Create a backfill for historical events in Orb.

    Parameters:
        orb_client (Orb): The Orb client instance.
        events (list): List of event dictionaries to backfill.

    Returns:
        str: The backfill ID.
    """
    try:
        timeframe_start = min(event["timestamp"] for event in events)
        timeframe_end = (datetime.fromisoformat(timeframe_start.replace("Z", "")) + timedelta(days=9)).strftime("%Y-%m-%dT%H:%M:%SZ")

        backfill = orb_client.events.backfills.create(
            timeframe_start=timeframe_start,
            timeframe_end=timeframe_end,
            close_time=None,  # Optional close_time parameter
            replace_existing_events=True
        )
        backfill_id = backfill.id
        print(f"Backfill created successfully with ID: {backfill_id}")
        return backfill_id
    except Exception as e:
        print(f"Error creating backfill: {e}")
        return None

def create_or_get_customer(orb_client, customer_data, customer_cache):
    """
    Create or fetch a customer in Orb and return the customer ID.

    Parameters:
        orb_client (Orb): The Orb client instance.
        customer_data (dict): A dictionary containing customer attributes.
        customer_cache (dict): Cache of created customers to avoid duplicates.

    Returns:
        str: The customer ID.
    """
    account_id = customer_data["account_id"]
    if account_id in customer_cache:
        return customer_cache[account_id]

    try:
        customer = orb_client.customers.create(
            email=customer_data.get("email", f"{customer_data['account_id']}@example.com"),
            name=customer_data.get("name", f"Customer {customer_data['account_id']}"),
        )
        customer_cache[account_id] = customer.id
        print(f"Customer created with ID: {customer.id}")
        return customer.id
    except Exception as e:
        print(f"Error creating customer: {e}")
        return None

def ingest_csv_to_orb(file_path):
    """
    Ingest data from a CSV file into the Orb platform using the Orb SDK.

    Parameters:
        file_path (str): Path to the CSV file.

    Returns:
        None
    """
    try:
        # Initialize Orb client
        orb_client = Orb(api_key=os.environ.get("ORB_API_KEY"))

        # Read the CSV file into a Pandas DataFrame
        data = pd.read_csv(file_path)

        print("Data Ingested from CSV Successfully!")
        print(f"Rows: {data.shape[0]}, Columns: {data.shape[1]}")
        print(data.head())

        # Replace NaN and clean numeric fields
        data["customer_id"] = data.get("customer_id", None)
        data["standard"] = data["standard"].replace({",": ""}, regex=True).astype(float, errors='ignore').fillna(0)
        data["sameday"] = data["sameday"].replace({",": ""}, regex=True).astype(float, errors='ignore').fillna(0)

        # Convert month column to ISO 8601 format
        data["iso_timestamp"] = pd.to_datetime(data["month"], format="%m-%Y", errors='coerce').dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        customer_cache = {}
        events = []
        for index, row in data.iterrows():
            try:
                # Handle missing or invalid customer_id
                customer_id = row.get("customer_id")
                if not customer_id or pd.isna(customer_id):
                    customer_id = create_or_get_customer(
                        orb_client,
                        {"account_id": row["account_id"], "email": f"{row['account_id']}@example.com"},
                        customer_cache
                    )
                    if not customer_id:
                        print(f"Skipping event {index + 1}: Unable to create customer.")
                        continue

                # Create an event for each row in the DataFrame
                event_params = {
                    "event_name": "ingest_event",
                    "idempotency_key": f"event_{index}",
                    "properties": {
                        "account_id": row["account_id"],
                        "month": row["month"],
                        "transaction_id": row["transaction_id"],
                        "account_type": row["account_type"],
                        "bank_id": row["bank_id"],
                        "standard": row["standard"],
                        "sameday": row["sameday"],
                    },
                    "timestamp": row["iso_timestamp"],  # ISO 8601 formatted date
                    "customer_id": customer_id,
                }
                events.append(event_params)

            except Exception as e:
                print(f"Error preparing event {index + 1}: {e}")

        if events:
            # Create a backfill for historical events
            backfill_id = create_backfill(orb_client, events)
            if backfill_id:
                print(f"Backfill created with ID: {backfill_id}")

            # Add debug mode for ingestion
            try:
                response = orb_client.events.ingest(events=events, debug=True, backfill_id=backfill_id)
                print(f"Debug response: {response}")
            except Exception as e:
                print(f"Error ingesting events: {e}")
        else:
            print("No events were prepared for ingestion.")

    except FileNotFoundError:
        print(f"Error: The file at {file_path} was not found.")
    except pd.errors.EmptyDataError:
        print("Error: The file is empty.")
    except pd.errors.ParserError as e:
        print(f"Error parsing the file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    # Specify the path to your CSV file
    csv_file_path = "Orb_sample_data.csv"  # Replace with your file path

    # Run the ingestion function
    ingest_csv_to_orb(csv_file_path)
