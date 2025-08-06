import os
from dotenv import load_dotenv

load_dotenv()

# API Configuration
API_KEY = os.getenv("NEW_RELIC_API_KEY")
ACCOUNT = os.getenv("NEW_RELIC_ACCOUNT_ID")
API_ENDPOINT = 'https://api.newrelic.com/graphql'

# Directory Configuration
EXPORT_DIR = 'export'

# Query Configuration
QUERIES = [
    {
        "nrql": "SELECT `Sku`,`Store` FROM Log_RFID WHERE `entity.name` = 'Prod-rfid-cc-results-subscriber' "
                "and message LIKE '%[MKS:%] SKU % is written to Local RFID DB%' limit max ORDER BY timestamp ASC",
        "query_name": "rfid-flagged-skus-mks"
    },
    {
        "nrql": "SELECT `Sku`,`Store` FROM Log_RFID WHERE `entity.name` = 'Prod-rfid-cc-results-subscriber' "
                "AND message LIKE '%[FGL:%] SKU % is written to Local RFID DB%' limit max ORDER BY timestamp ASC",
        "query_name": "rfid-flagged-skus-fgl"
    }
]