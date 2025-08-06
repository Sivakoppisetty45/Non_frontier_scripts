import requests
from Muting_pos_rule.config.settings import API_KEY, ACCOUNT_ID, CONDITION_ID
from Muting_pos_rule.utils.logger import setup_logger

logger = setup_logger()

def create_muting_rule(name, start_utc, end_utc):
    url = "https://api.newrelic.com/graphql"
    headers = {
        "API-Key": API_KEY,
        "Content-Type": "application/json"
    }

    query = f"""
    mutation {{
      alertsMutingRuleCreate(accountId: {ACCOUNT_ID}, rule: {{
        name: "{name}",
        description: "Auto mute for {name}",
        enabled: true,
        condition: {{
          operator: OR,
          conditions: [
            {{
              attribute: "conditionId",
              operator: EQUALS,
              values: ["{CONDITION_ID}"]
            }}
          ]
        }},
        schedule: {{
          startTime: "{start_utc}",
          endTime: "{end_utc}",
          timeZone: "US/Eastern"
        }}
      }}) {{
        id
        name
      }}
    }}
    """

    try:
        response = requests.post(url, headers=headers, json={"query": query})
        response.raise_for_status()
        data = response.json()

        if "errors" in data:
            logger.error(f" Error creating muting rule {name}: {data['errors']}")
        else:
            logger.info(f"Created Muting Rule: {name}")
    except Exception as e:
        logger.error(f"Failed to create muting rule {name}: {e}")
