# Orb CSV Data Ingestion

## Overview
`orb_csv.py` is a Python script designed to ingest data from a CSV file into the Orb platform. It handles the creation and retrieval of customers, event ingestion, and backfill creation for historical data. The script also includes debugging capabilities to help ensure seamless integration and identify potential issues during testing.

---

## Features
- **Customer Management**:
  - Caches created customer IDs to avoid redundant API calls.
  - Cached created customers maintain their customer IDs throughout the CSV
  - Future improvements include checking Server-Side Customer ID's inside your Orb account to prevent duplication.

- **Event Ingestion**:
  - Processes data from a CSV file.
  - Converts date formats and cleans numeric fields.
  - Sends events to Orb with debug mode enabled for detailed testing feedback.

- **Backfill Creation**:
  - Creates a backfill for historical events.
  - Automatically calculates the timeframes for the backfill to be Max value. (Orb only allows up to 10 days for backfill data at a time)

---

## Requirements
### Dependencies
Install the required dependencies using `pip`:

```bash
pip install python-dotenv pandas orb
```

### Environment Variables
Create a `.env` file in the project directory and include your Orb API key:

```
ORB_API_KEY=<your_orb_api_key>
```

### CSV File Format
The input CSV file should contain the following columns:
- `account_id`
- `month` (formatted as `MM-YYYY`)
- `transaction_id`
- `account_type`
- `bank_id`
- `standard` (numeric, can include commas which will be cleaned)
- `sameday` (numeric, can include commas which will be cleaned)

Ensure all required columns are present in the CSV file for successful processing.

---

## Usage
### Running the Script
1. Place the CSV file in the same directory as the script or specify its path.
2. Run the script:

```bash
python orb_csv.py
```

### Expected Behavior
1. The script reads the CSV file and processes the data row by row.
2. It checks if each customer exists in Orb:
   - If the customer exists(client side), their ID is used to submit events.
   - If the customer does not exist, it creates the customer in Orb and caches their ID.
3. Events are prepared for ingestion:
   - Timestamps are converted to ISO 8601 format.
   - Numeric fields are cleaned and filled with default values if missing.
4. A backfill is created for historical events:
   - The timeframe is calculated based on the earliest event timestamp.
5. Events are ingested into Orb, with debug mode enabled for detailed response output.

---

## Debugging
Debug mode is enabled during event ingestion to provide verbose feedback on the processing status. The response includes:
- Newly ingested events.
- Duplicate events.
- Validation errors, if any.

---

## Error Handling
The script includes error handling for the following scenarios:
- Missing or malformed CSV file.
- Invalid or missing customer data.
- API errors during customer creation, backfill creation, or event ingestion.

---

## Example Output
### Successful Execution
```
Data Ingested from CSV Successfully!
Rows: 100, Columns: 7
Backfill created successfully with ID: Abc1234XYZ
Debug response: EventIngestResponse(validation_failed=[], debug=Debug(duplicate=[], ingested=['event_48', 'event_74','event_42']))
```

### Errors
```
Error creating customer: Invalid email format
Skipping event 5: Unable to create customer.
Error creating backfill: Request validation did not succeed.
Error ingesting event event_1: Additional properties are not allowed ('unknown_field' was unexpected)
```

---

## Contributing
Contributions are welcome! Feel free to open issues or submit pull requests to improve the script.

---

## Support
For further assistance, refer to the [Orb API Documentation](https://docs.withorb.com/).

