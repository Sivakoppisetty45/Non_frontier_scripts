import datetime
import pytz
from Muting_pos_rule.config.settings import TIMEZONE

def get_utc_range(local_date):
    """
    Given a local date, return the UTC start and end time for that whole day
    based on the configured TIMEZONE.
    """

    # Start of the day (00:00 local)
    start_local = TIMEZONE.localize(datetime.datetime.combine(local_date, datetime.time(0, 0)))
    # End of the day (23:59 local)
    end_local = TIMEZONE.localize(datetime.datetime.combine(local_date, datetime.time(23, 59)))

    # Convert to UTC
    start_utc = start_local.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S")
    end_utc = end_local.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S")

    return start_utc, end_utc
