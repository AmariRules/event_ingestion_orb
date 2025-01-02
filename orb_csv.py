import os
from dotenv import load_dotenv
import pandas as pd
import asyncio
from datetime import datetime
from orb import AsyncOrb

# Load environment variables
load_dotenv()

async def create_or_get_customer(orb_client, customer_data):
    """
    Create or fetch a customer in Orb and return the customer ID.

    Parameters:
        orb_client (AsyncOrb): The Orb client instance.
        customer_data (dict): A dictionary containing customer attributes.

    Returns:
        str: The customer ID.
    """
    try:
        # Create a customer using Orb API
        customer = await orb_client.customers.create(
            email=customer_data.get("email", f"{customer_data['account_id']}@example.com"),
            name=customer_data.get("name", f"Customer {customer_data['account_id']}"),
        )
        return customer.id
    except Exception as e:
        print(f"Error creating customer: {e}")
        return None

async def ingest_csv_to_orb(file_path):
    """
    Ingest data from a CSV file into the Orb platform using the AsyncOrb SDK.

    Parameters:
        file_path (str): Path to the CSV file.

    Returns:
        None
    """
    try:
        # Initialize AsyncOrb client
        orb_client = AsyncOrb(api_key=os.environ.get("ORB_API_KEY"))

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

        # Iterate through the DataFrame and send data to Orb
        for index, row in data.iterrows():
            try:
                # Handle missing or invalid customer_id
                customer_id = row.get("customer_id")
                if not customer_id or pd.isna(customer_id):
                    customer_id = await create_or_get_customer(
                        orb_client,
                        {"account_id": row["account_id"], "email": f"{row['account_id']}@example.com"},
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

                await orb_client.events.ingest(events=[event_params])
                print(f"Successfully ingested event {index + 1}")
            except Exception as e:
                print(f"Error ingesting event {index + 1}: {e}")

    except FileNotFoundError:
        print(f"Error: The file at {file_path} was not found.")
    except pd.errors.EmptyDataError:
        print("Error: The file is empty.")
    except pd.errors.ParserError as e:
        print(f"Error parsing the file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    csv_file_path = "Orb_sample_data.csv"

    # Run the asynchronous ingestion function
    asyncio.run(ingest_csv_to_orb(csv_file_path))
