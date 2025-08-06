import logging
import json
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Configuration
NEW_RELIC_LOG_API_URL = "https://log-api.newrelic.com/log/v1"
NEW_RELIC_INSERT_KEY = os.getenv("NEW_RELIC_INSERT_KEY")
LOG_TYPE = "rfidautomationerror"  # Useful for log filtering/search

if not NEW_RELIC_INSERT_KEY:
    raise ValueError("NEW_RELIC_INSERT_KEY is not set in the .env file.")

def send_to_new_relic(log_message: str):
    """
    Sends a single log message to New Relic Logs API using a flat payload structure.
    """
    headers = {
        "X-Insert-Key": NEW_RELIC_INSERT_KEY,
        "Content-Type": "application/json"
    }

    payload = [
        {
            "message": log_message,
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
            "level": "error",
            "logtype": LOG_TYPE,
            "service": "rfid-cycle-count",
            "env": "dev"
        }
    ]

    try:
        response = requests.post(NEW_RELIC_LOG_API_URL, headers=headers, data=json.dumps(payload))
        if response.status_code == 202:
            print("✅ Log successfully sent to New Relic.")
        else:
            print(f"❌ Failed to send log to New Relic: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ Error sending log to New Relic: {e}")

def configure_logging():
    """
    Configures a logger that sends only ERROR logs to New Relic.
    """
    logger = logging.getLogger('json_logger')
    logger.setLevel(logging.ERROR)

    class NewRelicHandler(logging.Handler):
        def emit(self, record):
            if record.levelno == logging.ERROR:
                log_message = self.format(record)
                send_to_new_relic(log_message)

    nr_handler = NewRelicHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    nr_handler.setFormatter(formatter)
    logger.addHandler(nr_handler)

    return logger
