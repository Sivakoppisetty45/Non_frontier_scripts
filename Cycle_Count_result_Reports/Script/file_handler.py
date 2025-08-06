import os
import pandas as pd
from Cycle_Count_result_Reports.Script.time_utils import get_report_display_date
from Cycle_Count_result_Reports.config.settings import EXPORT_DIR
from Cycle_Count_result_Reports.config.logging_config import configure_logging
from Cycle_Count_result_Reports.Script.data_processor import remove_timestamp_columns
from Cycle_Count_result_Reports.Script.data_processor import reorder_columns

logger = configure_logging()


def save_to_xlsx(data, query_name):
    """Save the extracted data to an Excel file with proper error handling."""
    if not os.path.exists(EXPORT_DIR):
        os.makedirs(EXPORT_DIR)
        logger.info(f"Created directory {EXPORT_DIR}")

    # Create DataFrame
    if not data:
        logger.info(f"No data found for query: {query_name}. Creating empty file.")
        df = pd.DataFrame([{'Message': 'No Data Found'}])
    else:
        df = pd.DataFrame(data)
        df = remove_timestamp_columns(df)
        df = reorder_columns(df, query_name)

        # Special numeric handling for cycle counts
        for col in ['accuracy', 'minimumAccuracy', 'store']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(":", ""), errors='coerce').fillna(0)


    filename = f"{query_name}_{get_report_display_date()}.xlsx"
    file_path = os.path.join(EXPORT_DIR, filename)

    try:
        df.to_excel(file_path, index=False)
        logger.info(f"{query_name}: Saved Excel file with {len(df)} rows to {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"{query_name}: Excel save failed: {e}")
        return None
