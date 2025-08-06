import time
from RFID_flagged_products_Email_Report.config.settings import QUERIES
from RFID_flagged_products_Email_Report.config.logging_config import configure_logging
from RFID_flagged_products_Email_Report.core.time_utils import get_rfid_flagged_weekly_time_range,get_report_display_date
from RFID_flagged_products_Email_Report.core.data_processor import extract_data
from RFID_flagged_products_Email_Report.core.file_handler import save_to_xlsx
from RFID_flagged_products_Email_Report.core.email_utils import send_email_with_attachments

logger = configure_logging()

def main():
    try:
        start_time, end_time = get_rfid_flagged_weekly_time_range()
        logger.info(f"Fetching data from {start_time} to {end_time}")
        today_str = get_report_display_date()

        any_data_found = False  # <-- NEW

        for query in QUERIES:
            query_name = query['query_name']
            try:
                logger.info(f"Processing query: {query_name}")
                data = extract_data(query["nrql"], start_time, end_time)

                if not data:
                    logger.warning(f"No data found for {query_name}")
                    print(f"No data found for {query_name}")
                else:
                    any_data_found = True  # <-- SET if at least one query has data
                    print(f"Total events fetched for {query_name}: {len(data)}")
                    file_path = save_to_xlsx(data, query_name)

                    if file_path:
                        logger.info(f"Successfully saved {query_name} to {file_path}")
                    else:
                        logger.error(f"Failed to save {query_name}")

                time.sleep(2)

            except Exception as e:
                logger.error(f"Error processing {query_name}: {str(e)}")
                continue


        logger.info("Preparing to send email with attachments...")

        # Prepare body depending on data
        if any_data_found:
            email_body = f"""Hi,

Here is a report of all RFID flagged SKUs during Cycle Count -- {today_str}

Best regards,
StoreTech Automation Team"""
        else:
            email_body = f"""Hi,

There is no data of RFID flagged SKUs during Cycle Count -- {today_str}

Best regards,
StoreTech Automation Team"""

        try:
            send_email_with_attachments(
                subject=f"RFID flagged SKUs during Cycle Count - {today_str}",
                body=email_body,
                sender_email="svc.ST_automation@cantire.com",
                recipient_emails=["sivarakesh.koppisetty@cantire.com"],
                smtp_server="relay.cantire.com",
                smtp_port=25
            )
            logger.info("Email sent successfully.")
            print("Email sent successfully.")
        except Exception as email_err:
            logger.error(f"Failed to send email: {str(email_err)}")
            print(f"Failed to send email: {str(email_err)}")

    except Exception as e:
        logger.error(f"Fatal error in main execution: {str(e)}")
        raise
if __name__ == '__main__':
    main()