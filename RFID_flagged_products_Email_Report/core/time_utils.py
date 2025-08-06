from datetime import datetime, timedelta, time
import pytz

UTC_TZ = pytz.timezone("Etc/UTC")


def get_rfid_flagged_weekly_time_range():
    now_utc = datetime.now(UTC_TZ)
    today_utc = now_utc.date()
    weekday = today_utc.weekday()  # Monday=0, Sunday=6

    if weekday == 0:  # Monday
        # Last week's Monday to Wednesday
        monday = today_utc - timedelta(days=7)
        end_day = monday + timedelta(days=2)
        end_time = datetime.combine(end_day, time(23, 59, 59))
    elif weekday == 3:  # Thursday
        # This week's Monday to Wednesday
        monday = today_utc - timedelta(days=3)
        end_day = monday + timedelta(days=2)
        end_time = datetime.combine(end_day, time(23, 59, 59))
    elif weekday == 4:  # Friday
        # This week's Monday to Thursday
        monday = today_utc - timedelta(days=4)
        end_day = monday + timedelta(days=3)
        end_time = datetime.combine(end_day, time(23, 59, 59))
    else:
        # Tuesday, Wednesday, Saturday, Sunday
        monday = today_utc - timedelta(days=weekday)
        end_time = now_utc  # Current UTC time

    start_time = datetime.combine(monday, time.min)
    start_dt_utc = UTC_TZ.localize(start_time)
    end_dt_utc = end_time if isinstance(end_time, datetime) and end_time.tzinfo else UTC_TZ.localize(end_time)

    return start_dt_utc, end_dt_utc


def get_report_display_date():
    """
    Returns the Tuesday of the week corresponding to the report's end date.
    For Monday runs, show last week's Tuesday.
    """
    now = datetime.now(UTC_TZ)
    today = now.date()
    weekday = today.weekday()

    if weekday == 0:  # Monday
        # Show last week's Tuesday
        tuesday = today - timedelta(days=7 + (0 - 1))
    else:
        _, end_time = get_rfid_flagged_weekly_time_range()
        end_date = end_time.date()
        tuesday = end_date - timedelta(days=(end_date.weekday() - 1))

    return tuesday.strftime("%d-%m-%Y")
