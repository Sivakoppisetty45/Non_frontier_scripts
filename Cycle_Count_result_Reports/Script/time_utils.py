from datetime import datetime, timedelta, time
import pytz

UTC_TZ = pytz.timezone("Etc/UTC")

def get_time_range_for_cycle_count():
    now = datetime.now(UTC_TZ)
    weekday = now.weekday()

    if weekday == 0:  # Monday
        last_tuesday = now.date() - timedelta(days=6)
        start_time = UTC_TZ.localize(datetime.combine(last_tuesday, time(0, 1, 0)))
        end_time = UTC_TZ.localize(datetime.combine(last_tuesday, time(23, 59, 59)))

    elif weekday == 1:  # Tuesday
        last_tuesday = now.date() - timedelta(days=7)
        start_time = UTC_TZ.localize(datetime.combine(last_tuesday, time(0, 1, 0)))
        end_time = UTC_TZ.localize(datetime.combine(last_tuesday, time(23, 59, 59)))

    elif weekday == 2:  # Wednesday
        this_tuesday = now.date() - timedelta(days=1)
        start_time = UTC_TZ.localize(datetime.combine(this_tuesday, time(0, 1, 0)))
        end_time = UTC_TZ.localize(datetime.combine(this_tuesday, time(23, 59, 59)))

    elif weekday == 3:  # Thursday
        wednesday = now.date() - timedelta(days=1)
        start_time = UTC_TZ.localize(datetime.combine(wednesday, time(0, 0, 0)))
        end_time = now  # current timestamp (dynamic cut-off)

    else:  # Friday, Saturday, Sunday
        wednesday = now.date() - timedelta(days=(weekday - 2))
        start_time = UTC_TZ.localize(datetime.combine(wednesday, time(0, 0, 0)))
        end_time = now

    return start_time, end_time


def get_report_display_date():
    now = datetime.now(UTC_TZ)
    today = now.date()
    weekday = now.weekday()

    if weekday == 2:  # Wednesday
        report_date = today - timedelta(days=1)  # Tuesday
    elif weekday == 3:  # Thursday
        report_date = today - timedelta(days=1)  # Wednesday
    elif weekday in [0, 1]:  # Monday or Tuesday
        report_date = today - timedelta(days=weekday + 6)  # Last week's Tuesday
    else:  # Friday, Saturday, Sunday
        days_since_wed = weekday - 2
        report_date = today - timedelta(days=days_since_wed)  # This week's Wednesday

    return report_date.strftime("%d-%m-%Y")
