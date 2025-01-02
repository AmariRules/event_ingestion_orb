import os
from orb import Orb
from dotenv import load_dotenv

load_dotenv()

client = Orb(
    api_key=os.environ.get("ORB_API_KEY"),  # This is the default and can be omitted
)

customer = client.customers.create(
    email="example-customer@withorb.com",
    name="My Customer",
)
print(customer.id)