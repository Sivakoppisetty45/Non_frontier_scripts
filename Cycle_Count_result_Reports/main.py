from collections import defaultdict
import pandas as pd
import time
from Cycle_Count_result_Reports.config.settings import QUERIES
from Cycle_Count_result_Reports.config.logging_config import configure_logging
from Cycle_Count_result_Reports.Script.time_utils import get_time_range_for_cycle_count, get_report_display_date
from Cycle_Count_result_Reports.Script.data_processor import extract_data
from Cycle_Count_result_Reports.Script.file_handler import save_to_xlsx
from Cycle_Count_result_Reports.Script.email_utils import send_email_with_attachments

logger = configure_logging()
has_errors = False  # Track if any errors occur


def main():
    global has_errors
    try:
        start_time, end_time = get_time_range_for_cycle_count()
        logger.info(f"Fetching data from {start_time} to {end_time}")
        today_str = get_report_display_date()

        summary_counts = defaultdict(lambda: {'FGL': 0, 'MKS': 0})

        for query in QUERIES:
            query_name = query['query_name']
            try:
                logger.info(f"Processing query: {query_name}")
                data = extract_data(query["nrql"], start_time, end_time)

                if not data:
                    logger.warning(f"No data found for {query_name}")
                    print(f"No data found for {query_name}")
                else:
                    print(f"Total events fetched for {query_name}: {len(data)}")

                    for row in data:
                        if 'facet' in row and isinstance(row['facet'], list):
                            if query_name == "not-received-stores-low-accuracy":
                                row['sbu'], row['store'], row['accuracy'], row['minimumAccuracy'] = row['facet']
                            elif query_name == "not-received-stores-accepted-after-approval-time-limit":
                                row['sbu'], row['store'], row['startDate'], row['approvalDate'] = row['facet']
                            elif query_name in ['received-stores', 'sent-to-pmm-stores']:
                                row['sbu'], row['store'] = row['facet']
                            del row['facet']

                    file_path = save_to_xlsx(data, query_name)
                    if file_path:
                        logger.info(f"Successfully saved {query_name} to {file_path}")
                    else:
                        has_errors = True
                        logger.error(f"Failed to save {query_name}")

                    df = pd.DataFrame(data)
                    if 'sbu' in df.columns:
                        df['sbu'] = df['sbu'].astype(str).str.upper()
                        summary_counts[query_name]['FGL'] = df[df['sbu'] == 'FGL'].shape[0]
                        summary_counts[query_name]['MKS'] = df[df['sbu'] == 'MKS'].shape[0]

                        print(f"{query_name} - FGL: {summary_counts[query_name]['FGL']}, "
                              f"MKS: {summary_counts[query_name]['MKS']}")

                time.sleep(2)

            except Exception as e:
                has_errors = True
                logger.error(f"Error processing {query_name}: {str(e)}")
                continue

        lines = [f"Hi,\n\nHere are the results of {today_str} - CC:\n"]

        #  variables inside function to avoid shadowing
        def format_line(title, query_key):
            query_total = sum(summary_counts[query_key].values())
            query_fgl = summary_counts[query_key]["FGL"]
            query_mks = summary_counts[query_key]["MKS"]
            line = f"Total number of stores {title}: {query_total} in total: {query_fgl} for FGL + {query_mks} for MKS"
            if query_total > 0:
                line += f" (please find the list of {query_key} attached)"
            return line

        lines.append(format_line("for which CC results were received", "received-stores"))
        lines.append(format_line("for which CC results were not received due to a low accuracy",
                                 "not-received-stores-low-accuracy"))
        lines.append(format_line("for which CC results were not received due to accepted after defined approval time limit",
                                 "not-received-stores-accepted-after-approval-time-limit"))
        lines.append(format_line("for which CC results were sent to PMM for adjustment", "sent-to-pmm-stores"))


        final_fgl = summary_counts["received-stores"]["FGL"]
        final_mks = summary_counts["received-stores"]["MKS"]
        final_total = final_fgl + final_mks
        lines.append(f"\nTotal number of stores for which CC results were processed successfully without issues: "
                     f"{final_total} in total: {final_fgl} for FGL + {final_mks} for MKS")

        lines.append("\nRegards,\nSiva Koppisetty\nStoreTech Automation Team")
        email_body = "\n\n".join(lines)

        logger.info("Preparing to send email with attachments...")
        try:
            send_email_with_attachments(
                subject=f"[RFID] CC results - {today_str}",
                body=email_body,
                sender_email="svc.ST_automation@cantire.com",
                recipient_emails=["svc.ST_automation@cantire.com","SivaRakesh.Koppisetty@cantire.c"],
                cc_emails=["SivaRakesh.Koppisetty@cantire.com"],
                smtp_server="relay.cantire.com",
                smtp_port=25
            )
            logger.info("Email sent successfully.")
            print("Email sent successfully.")
        except Exception as email_err:
            has_errors = True
            logger.error(f"Failed to send email: {str(email_err)}")
            print(f"Failed to send email: {str(email_err)}")

    except Exception as e:
        has_errors = True
        logger.error(f"Fatal error in main execution: {str(e)}")
        raise

    if has_errors:
        print("Error occurred and logs sent to New Relic.")
    else:
        print("No errors occurred.")


if __name__ == '__main__':
    main()
