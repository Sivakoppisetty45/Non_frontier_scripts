import pandas as pd
from Cycle_Count_result_Reports.config.logging_config import configure_logging
from Cycle_Count_result_Reports.Script.api_client import fetch_data

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
    """Remove timestamp-like and unwanted columns such as 'Total'."""
    for column in df.columns:
        if column.lower() == 'total':
            df.drop(columns=[column], inplace=True)
            logger.info("Removed column: Total")
        elif pd.api.types.is_numeric_dtype(df[column]):
            if df[column].max() > 1e12:
                df.drop(columns=[column], inplace=True)
                logger.info(f"Removed column with timestamp data: {column}")
    return df


def reorder_columns(df, query_name):
    """Special column ordering for cycle count reports"""
    if query_name == "not-received-stores-low-accuracy":
        return df[['sbu', 'store', 'accuracy', 'minimumAccuracy']]
    elif query_name == "not-received-stores-accepted-after-approval-time-limit":
        return df[['sbu', 'store', 'startDate', 'approvalDate']]
    return df