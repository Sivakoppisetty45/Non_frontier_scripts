import requests
import datetime
import pytz
import calendar
from dotenv import load_dotenv
import os

# Load .env variables
load_dotenv()

# --- CONFIG ---
API_KEY = os.getenv("NEW_RELIC_API_KEY")
ACCOUNT_ID = os.getenv("NEW_RELIC_ACCOUNT_ID")
ENTITY_GUID = "NDM3MDMyNXxBSU9QU3xDT05ESVRJT058NTI2MDgyNDM"
TIMEZONE = pytz.timezone("US/Eastern")

# --- FUNCTION TO FIND FIRST AND THIRD TUESDAY ---
def get_first_and_third_tuesdays(year, month):
    c = calendar.Calendar()
    tuesdays = [day for day in c.itermonthdates(year, month)
                if day.weekday() == 1 and day.month == month]
    return tuesdays[0], tuesdays[2]

# --- FUNCTION TO FORMAT TIME TO UTC ---
def get_utc_range(local_date):
    start_local = TIMEZONE.localize(datetime.datetime.combine(local_date, datetime.time(0, 0)))
    end_local = TIMEZONE.localize(datetime.datetime.combine(local_date, datetime.time(23, 59)))
    start_utc = start_local.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S")
    end_utc = end_local.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S")
    return start_utc, end_utc

# --- FUNCTION TO CREATE MUTING RULE ---
def create_muting_rule(name, start_utc, end_utc):
    url = "https://api.newrelic.com/graphql"
    headers = {
        "API-Key": API_KEY,
        "Content-Type": "application/json"
    }

    query = """
    mutation {
      alertsMutingRuleCreate(accountId: %s, rule: {
        name: "%s",
        description: "Auto mute for %s",
        enabled: true,
        condition: {
          operator: OR,
          conditions: [
            {
              attribute: "entity.guid",
              operator: EQUALS,
              values: ["%s"]
            }
          ]
        },
        schedule: {
          startTime: "%s",
          endTime: "%s",
          timeZone: "US/Eastern"
        }
      }) {
        id
        name
      }
    }
    """ % (
        ACCOUNT_ID,
        name,
        name,
        ENTITY_GUID,
        start_utc,
        end_utc
    )

    response = requests.post(url, headers=headers, json={"query": query})
    print(f"[✓] Created Muting Rule: {name}")
    print("Response:", response.json())

# --- MAIN EXECUTION ---
today = datetime.date.today()

# Use next month if today is near the end of month
if today.day > 25:
    if today.month == 12:
        year, month = today.year + 1, 1
    else:
        year, month = today.year, today.month + 1
else:
    year, month = today.year, today.month

first_tuesday, third_tuesday = get_first_and_third_tuesdays(year, month)

for idx, tuesday in enumerate([first_tuesday, third_tuesday], start=1):
    start_utc, end_utc = get_utc_range(tuesday)
    rule_name = f"AutoMute - {idx} Tuesday - {tuesday.strftime('%Y-%m-%d')}"
    create_muting_rule(rule_name, start_utc, end_utc)
