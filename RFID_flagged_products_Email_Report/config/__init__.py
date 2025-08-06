import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

NEW_RELIC_API_KEY = os.getenv("NEW_RELIC_API_KEY")
ACCOUNT = os.getenv("NEW_RELIC_ACCOUNT_ID")
