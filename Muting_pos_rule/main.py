import datetime
from Muting_pos_rule.utils.calendar_utils import get_first_and_third_tuesdays
from Muting_pos_rule.utils.time_utils import get_utc_range
from Muting_pos_rule.newrelic.muting_rule import create_muting_rule
from Muting_pos_rule.newrelic.cleaning import cleanup_old_rules
from Muting_pos_rule.utils.logger import setup_logger

logger = setup_logger()

def main():
    logger.info("Starting muting rule automation process.")
    print("INFO: Starting muting rule automation process.")

    try:
        cleanup_old_rules()
        logger.info("Cleanup completed.")
        print("INFO: Cleanup completed.")

        today = datetime.date.today()
        if today.day > 25:
            next_month = today.replace(day=28) + datetime.timedelta(days=4)
            target_month = next_month.month
            target_year = next_month.year
        else:
            target_month = today.month
            target_year = today.year

        first_tuesday, third_tuesday = get_first_and_third_tuesdays(target_year, target_month)

        for idx, tuesday in enumerate([first_tuesday, third_tuesday], start=1):
            start_utc, end_utc = get_utc_range(tuesday)
            rule_name = f"AutoMute - {idx} Tuesday - {tuesday.strftime('%Y-%m-%d')}"
            create_muting_rule(rule_name, start_utc, end_utc)
            logger.info(f"Created Muting Rule: {rule_name}")
            print(f"INFO: Created Muting Rule: {rule_name}")

        logger.info("Muting rule automation completed successfully.")
        print("INFO: Muting rule automation completed successfully.")

    except Exception as e:
        logger.exception(f"Unexpected error in muting rule automation: {e}")
        print(f"ERROR: Unexpected error in muting rule automation: {e}")

if __name__ == "__main__":
    main()
