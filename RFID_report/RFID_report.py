import pytz
from tenacity import retry, stop_after_attempt, wait_fixed
import os
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
import concurrent.futures
import time
from dotenv import load_dotenv


# Timezone for EST
EST = pytz.timezone('US/Eastern')

# Load environment variables from .env file
load_dotenv()

API_KEY = os.getenv("NEW_RELIC_API_KEY")
ACCOUNT = os.getenv("NEW_RELIC_ACCOUNT_ID")
API_ENDPOINT = 'https://api.newrelic.com/graphql'

logger = logging.getLogger('json_logger')
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler('newrelic_data.log')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# Retry decorator
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_data(nrql_query):
    """Fetch data from New Relic using NRQL."""
    logger.info(f'Fetching data from New Relic: {nrql_query}')
    headers = {'API-Key': API_KEY, 'Content-Type': 'application/json'}
    query = f"""
    {{
      actor {{
        nrql(
          query:"{nrql_query}"
          accounts: {ACCOUNT}
        ) {{
          results
        }}
      }}
    }}
    """
    payload = {'query': query}
    response = requests.post(API_ENDPOINT, headers=headers, json=payload)
    logger.info(f'Response received. Status Code: {response.status_code}')
    logger.debug(f'Raw Response: {response.text}')
    if 'errors' in response.json():
        logger.error(f"New Relic returned error response: {response.json()['errors'][0]['message']}")
        return []
    response.raise_for_status()
    return response.json()['data']['actor']['nrql']['results']


# Parallel fetching function
def fetch_data_parallel(nrql_query, time_chunks):
    """Fetch data for multiple time chunks in parallel."""
    all_data = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Generate the queries for each chunk
        futures = []
        for chunk_start, chunk_end in time_chunks:
            chunk_query = f"{nrql_query} SINCE '{chunk_start.strftime('%Y-%m-%d %H:%M:%S')}' UNTIL '{chunk_end.strftime('%Y-%m-%d %H:%M:%S')}'"
            futures.append(executor.submit(fetch_data, chunk_query))  # Submit the fetch requests in parallel

        # Collect results as they complete
        for future in concurrent.futures.as_completed(futures):
            chunk_data = future.result()
            all_data.extend(chunk_data)
            logger.info(f"Fetched {len(chunk_data)} events")
    return all_data


def divide_time_range(start_time, end_time, chunk_size_minutes=5):  # Adjusted to 5 minutes or another value
    """Divide the time range into smaller chunks with adjustable time intervals."""
    time_chunks = []
    current_time = start_time
    while current_time < end_time:
        next_time = current_time + timedelta(minutes=chunk_size_minutes)
        if next_time > end_time:
            next_time = end_time
        time_chunks.append((current_time, next_time))
        current_time = next_time
    return time_chunks


def extract_data(nrql_query, start_time, end_time):
    """Fetch data in chunks for the given time range using parallel fetching."""
    all_data = []
    time_chunks = divide_time_range(start_time, end_time)  # Divide into time chunks
    # Fetch data in parallel for all chunks
    all_data = fetch_data_parallel(nrql_query, time_chunks)
    return all_data

def remove_timestamp_columns(df):
    """Remove any timestamp-like columns."""
    for column in df.columns:
        if pd.api.types.is_numeric_dtype(df[column]):
            if df[column].max() > 1e12:
                df.drop(columns=[column], inplace=True)
                logger.info(f"Removed column with timestamp data: {column}")
    return df

def reorder_columns(df, query_name):
    """Reorder columns based on query type."""
    # Define the correct column order for the 3rd and 4th queries
    if query_name == "not-received-stores-low-accuracy":  # 3rd query
        correct_order = ['sbu', 'store', 'accuracy', 'minimumAccuracy']
    elif query_name == "not-received-stores-accepted-after-approval-time-limit":  # 4th query
        correct_order = ['sbu', 'store', 'startDate', 'approvalDate']
    else:
        # Default: use the current column order
        correct_order = df.columns.tolist()

    # Reorder columns
    df = df[correct_order]

    return df

def save_to_xlsx(data, query_name, local_dir, columns=None):
    """Save the extracted data to an Excel file with cleaned data."""
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
        logger.info(f"Created directory {local_dir}")

    if not data:
        logger.info(f"No data found for query: {query_name}. Creating an empty file with a 'No Data Found' message.")
        df = pd.DataFrame([{'Message': 'No Data Found'}])
    else:
        # Create DataFrame from data
        df = pd.DataFrame(data)

        # If columns are provided, ensure the DataFrame has those columns
        if columns:
            df = df[columns]

        # Remove timestamp columns
        df = remove_timestamp_columns(df)

        # Reorder columns based on the query name
        df = reorder_columns(df, query_name)

        # List of columns to check and convert to numeric
        numeric_columns = ['accuracy', 'minimumAccuracy', 'store']

        for col in numeric_columns:
            if col in df.columns:
                # Remove any colons or special characters that might cause Excel to treat numbers as text
                df[col] = df[col].astype(str).str.replace(":", "", regex=False)

                # Convert the column to numeric (forces numbers to numeric format)
                df[col] = pd.to_numeric(df[col], errors='coerce')

                # Replace NaN values with 0 (no inplace)
                df[col] = df[col].fillna(0)

    # If columns are provided, reorder according to them, if necessary
    if columns:
        df = df[columns]

    filename = f"{query_name}_{datetime.now().strftime('%d-%m-%Y')}.xlsx"
    file_path = os.path.join(local_dir, filename)

    try:
        df.to_excel(file_path, index=False)
        logger.info(f"Data saved to Excel file: {file_path}")
    except Exception as e:
        logger.error(f"Error saving Excel file: {e}")
        return None

    return file_path



def convert_to_est(dt):
    """Convert a datetime object to Eastern Standard Time (EST)."""
    # Localize to UTC and then convert to EST
    utc_time = pytz.utc.localize(dt)  # Make sure datetime is UTC first
    return utc_time.astimezone(EST)  # Convert to EST



def get_previous_days_time_range():
    """Calculate the time range for the day before yesterday and yesterday."""
    today = datetime.now(EST).date()
    day_before_yesterday = today - timedelta(days=2)
    yesterday = today - timedelta(days=1)

    start_time = datetime.combine(day_before_yesterday, datetime.min.time(), tzinfo=EST)
    end_time = datetime.combine(yesterday, datetime.max.time(), tzinfo=EST)

    return start_time, end_time


if __name__ == '__main__':
    # Get time range (from 12:00 AM of the day before yesterday to 11:59 PM of yesterday)
    start_time, end_time = get_previous_days_time_range()
    start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')

    logger.info(f"Fetching data from {start_time_str} EST to {end_time_str} EST")

    # Define NRQL queries to execute
    queries = [
        {
            "nrql": f"FROM Log_RFID WITH APARSE(`message`, '%[*:*] Received sku count from TV%') AS (sbu, store) SELECT sbu,store WHERE `entity.name` = 'Prod-rfid-cc-results-subscriber' AND `message` LIKE '%[%:%] Received sku count from TV%' limit max  ORDER BY timestamp ASC",
            "query_name": "received-stores",
            "columns": ['sbu', 'store']
        },

        {
            "nrql": f"FROM Log_RFID WITH APARSE(`message`, '[*:*] Successfully sent message to Tibco with sku count:%') AS (sbu, store)  SELECT sbu,store WHERE `entity.name` = 'Prod-rfid-cc-results-publisher' AND message LIKE '[%:%] Successfully sent message to Tibco with sku count:%' limit max  ORDER BY timestamp ASC",
            "query_name": "sent-to-pmm-stores",
             "columns": ['sbu', 'store']
        },
        {
            "nrql": f"FROM Log_RFID WITH APARSE(message, 'Received TV feedback notification for sbu: * %2.04.0004%\\\"siteCode\\\":\\\"*\\\",\\\"accuracy\\\"*,\\\"minimumAccuracy\\\":*}}%') AS (sbu, store, accuracy,minimumAccuracy) SELECT sbu, store, accuracy,minimumAccuracy WHERE `entity.name` = 'Prod-rfid-tv-feedback-subscriber' AND message LIKE 'Received TV feedback notification for sbu%2.04.0004%' limit max  ORDER BY timestamp ASC",
            "query_name": "not-received-stores-low-accuracy",
            "columns": ['sbu', 'store', 'accuracy', 'minimumAccuracy']
        },
        {
            "nrql": f"FROM Log_RFID WITH APARSE(message, '%Received TV feedback notification for sbu: * %2.04.0003%\\\"siteCode\\\":\\\"*\\\",\\\"startDate\\\":\\\"*\\\",\\\"approvalDate\\\":\\\"*\\\"}}%') AS (sbu, store, startDate, approvalDate) SELECT sbu, store, startDate, approvalDate  WHERE `entity.name` = 'Prod-rfid-tv-feedback-subscriber' AND message LIKE '%Received TV feedback notification for sbu%2.04.0003%' limit max  ORDER BY timestamp ASC",
            "query_name": "not-received-stores-accepted-after-approval-time-limit",
            "columns": ['sbu', 'store', 'accuracy', 'minimumAccuracy']
        }
    ]

    # Loop through each query, extract data, and save it to Excel
    for query in queries:
        nrql_query = query["nrql"]
        query_name = query["query_name"]

        # Extract data for the current query
        try:
            data = extract_data(nrql_query, start_time, end_time)
            if not data:
                logger.warning(f"No data found for {query_name}.")
            else:
                print(f"Total events fetched for {query_name}: {len(data)}")

                # Save the data to an Excel file
                file_path = save_to_xlsx(data, query_name, 'export')
                if file_path:
                    logger.info(f"File saved successfully for {query_name} at: {file_path}")
                else:
                    logger.error(f"File saving failed for {query_name}.")
        except Exception as e:
            logger.error(f"Error while fetching data for {query_name}: {str(e)}")

        # Add a small delay between queries to avoid rate limits or timeouts
        time.sleep(2)
