import datetime
import pytz
from Muting_pos_rule.config.settings import TIMEZONE

def get_utc_range(local_date):
    start_local = TIMEZONE.localize(datetime.datetime.combine(local_date, datetime.time(0, 0)))
    end_local = TIMEZONE.localize(datetime.datetime.combine(local_date, datetime.time(23, 59)))
    start_utc = start_local.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S")
    end_utc = end_local.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S")
    return start_utc, end_utc
