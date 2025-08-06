import os
from dotenv import load_dotenv
import pytz

load_dotenv()

API_KEY = os.getenv("NEW_RELIC_API_KEY")
ACCOUNT_ID = os.getenv("NEW_RELIC_ACCOUNT_ID")
NEW_RELIC_INSERT_KEY = os.getenv("NEW_RELIC_INSERT_KEY")  # For logging
CONDITION_ID = "52608243"
TIMEZONE = pytz.timezone("US/Eastern")
