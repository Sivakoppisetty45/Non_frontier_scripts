import os
from dotenv import load_dotenv

load_dotenv()

# API Configuration
API_KEY = os.getenv("NEW_RELIC_API_KEY")
ACCOUNT = os.getenv("NEW_RELIC_ACCOUNT_ID")
API_ENDPOINT = 'https://api.newrelic.com/graphql'


# Directories
EXPORT_DIR = 'export'

# Query Configuration
QUERIES = [
        {
            "nrql": f"FROM Log_RFID WITH APARSE(`message`, '%[*:*] Received sku count from TV%') AS (sbu, store) SELECT count(*) AS Total WHERE `entity.name` = 'Prod-rfid-cc-results-subscriber' AND `message` LIKE '%[%:%] Received sku count from TV%' facet sbu,store limit max  ORDER BY timestamp ASC",
            "query_name": "received-stores",
            "columns": ['sbu', 'store']
        },

        {
            "nrql": f"FROM Log_RFID WITH APARSE(`message`, '[*:*] Successfully sent message to Tibco with sku count:%') AS (sbu, store)   SELECT count(*) AS Total WHERE `entity.name` = 'Prod-rfid-cc-results-publisher' AND message LIKE '[%:%] Successfully sent message to Tibco with sku count:%' facet sbu,store limit max  ORDER BY timestamp ASC",
            "query_name": "sent-to-pmm-stores",
             "columns": ['sbu', 'store']
        },
        {
            "nrql": f"FROM Log_RFID WITH APARSE(message, 'Received TV feedback notification for sbu: * %2.04.0004%\\\"siteCode\\\":\\\"*\\\",\\\"accuracy\\\"*,\\\"minimumAccuracy\\\":*}}%') AS (sbu, store, accuracy,minimumAccuracy) SELECT count(*) AS Total WHERE `entity.name` = 'Prod-rfid-tv-feedback-subscriber' AND message LIKE 'Received TV feedback notification for sbu%2.04.0004%' FACET sbu, store, accuracy, minimumAccuracy limit max   ORDER BY timestamp ASC",
            "query_name": "not-received-stores-low-accuracy",
            "columns": ['sbu', 'store', 'accuracy', 'minimumAccuracy']
        },
        {
            "nrql": f"FROM Log_RFID WITH APARSE(message, '%Received TV feedback notification for sbu: * %2.04.0003%\\\"siteCode\\\":\\\"*\\\",\\\"startDate\\\":\\\"*\\\",\\\"approvalDate\\\":\\\"*\\\"}}%') AS (sbu, store, startDate, approvalDate) SELECT count(*) AS Total  WHERE `entity.name` = 'Prod-rfid-tv-feedback-subscriber' AND message LIKE '%Received TV feedback notification for sbu%2.04.0003%' FACET sbu, store, startDate, approvalDate limit max ORDER BY timestamp ASC",
            "query_name": "not-received-stores-accepted-after-approval-time-limit",
            "columns": ['sbu', 'store', 'startDate', 'approvalDate']
        }
    ]