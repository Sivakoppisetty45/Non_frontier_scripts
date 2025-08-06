import logging
import os
from datetime import datetime, timezone
import requests
import json
from Muting_pos_rule.config.settings import NEW_RELIC_INSERT_KEY

LOG_FILE = "logs/muting_rules.log"
LOG_TYPE = "muting_rule_script"
SERVICE_NAME = "muting-rule-automation"
ENV = "dev"

def send_to_new_relic(message: str):
    if not NEW_RELIC_INSERT_KEY:
        return

    payload = [{
        "message": message,
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        "level": "error",
        "logtype": LOG_TYPE,
        "service": SERVICE_NAME,
        "env": ENV
    }]

    headers = {
        "X-Insert-Key": NEW_RELIC_INSERT_KEY,
        "Content-Type": "application/json"
    }

    try:
        requests.post("https://log-api.newrelic.com/log/v1", headers=headers, data=json.dumps(payload))
    except Exception as e:
        print(f"Failed to send log to New Relic: {e}")

class NewRelicHandler(logging.Handler):
    def emit(self, record):
        if record.levelno >= logging.ERROR:
            send_to_new_relic(self.format(record))

def setup_logger():
    logger = logging.getLogger("muting_rule_logger")
    logger.setLevel(logging.INFO)

    # Avoid adding multiple handlers if already added
    if not logger.handlers:
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)

        file_handler = logging.FileHandler(os.path.join(log_dir, "muting_rules.log"))
        file_handler.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)

    return logger
