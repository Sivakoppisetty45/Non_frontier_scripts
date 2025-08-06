import pandas as pd
from RFID_flagged_products_Email_Report.core.api_client import fetch_data
from RFID_flagged_products_Email_Report.config.logging_config import configure_logging

logger = configure_logging()


def recursively_fetch_all_data(nrql_query, start_time, end_time, depth=0):
    """
    Recursively fetch all data from New Relic for a given time window.
    Splits time if 5000 results are returned to avoid truncation.
    """
    indent = "  " * depth
    formatted_start = start_time.strftime('%Y-%m-%d %H:%M:%S')
    formatted_end = end_time.strftime('%Y-%m-%d %H:%M:%S')
    full_query = f"{nrql_query} SINCE '{formatted_start}' UNTIL '{formatted_end}'"

    results = fetch_data(full_query)
    logger.info(f"{indent}Fetched {len(results)} records for: {formatted_start} to {formatted_end}")

    # New Relic truncates silently at 5000, so recursively divide
    if len(results) >= 5000:
        midpoint = start_time + (end_time - start_time) / 2
        logger.warning(f"{indent}Possible truncation. Splitting {formatted_start} to {formatted_end}")

        left = recursively_fetch_all_data(nrql_query, start_time, midpoint, depth + 1)
        right = recursively_fetch_all_data(nrql_query, midpoint, end_time, depth + 1)

        return left + right
    else:
        return results


def extract_data(nrql_query, start_time, end_time):
    """Recursively fetch safe full data from New Relic."""
    return recursively_fetch_all_data(nrql_query, start_time, end_time)


def remove_timestamp_columns(df):
    """Remove any timestamp-like columns from the dataframe."""
    for column in df.columns:
        if pd.api.types.is_numeric_dtype(df[column]):
            if df[column].max() > 1e12:
                df.drop(columns=[column], inplace=True)
                logger.info(f"Removed column with timestamp data: {column}")
    return df
