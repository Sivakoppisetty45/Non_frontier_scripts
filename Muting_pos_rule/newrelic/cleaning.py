import requests
from Muting_pos_rule.config.settings import API_KEY, ACCOUNT_ID
from Muting_pos_rule.utils.logger import setup_logger

logger = setup_logger()

def get_muting_rules():
    url = "https://api.newrelic.com/graphql"
    headers = {
        "API-Key": API_KEY,
        "Content-Type": "application/json"
    }

    query = f"""
    {{
      actor {{
        account(id: {ACCOUNT_ID}) {{
          alerts {{
            mutingRules {{
              id
              name
            }}
          }}
        }}
      }}
    }}
    """

    try:
        response = requests.post(url, headers=headers, json={"query": query})
        response.raise_for_status()
        rules = response.json()["data"]["actor"]["account"]["alerts"]["mutingRules"]
        return rules
    except Exception as e:
        logger.error(f"Failed to fetch muting rules: {e}")
        return []

def delete_muting_rule(rule_id, name):
    url = "https://api.newrelic.com/graphql"
    headers = {
        "API-Key": API_KEY,
        "Content-Type": "application/json"
    }

    mutation = f"""
    mutation {{
      alertsMutingRuleDelete(accountId: {ACCOUNT_ID}, id: "{rule_id}") {{
        id
        name
      }}
    }}
    """

    try:
        response = requests.post(url, headers=headers, json={"query": mutation})
        response.raise_for_status()
        data = response.json()

        if "errors" in data:
            logger.error(f"Failed to delete rule {name} (ID: {rule_id}): {data['errors']}")
        else:
            logger.info(f"Deleted Muting Rule: {name} (ID: {rule_id})")
    except Exception as e:
        logger.error(f"Error deleting rule {name} (ID: {rule_id}): {e}")

def cleanup_old_rules():
    rules = get_muting_rules()
    logger.info(f"Found {len(rules)} existing muting rules.")
    delete_keywords = ["1st Tuesday", "3rd Tuesday", "AutoMute"]

    for rule in rules:
        name = rule.get("name", "")
        rule_id = rule.get("id")

        if any(keyword in name for keyword in delete_keywords):
            logger.info(f" Preparing to delete rule: {name} (ID: {rule_id})")
            delete_muting_rule(rule_id, name)
        else:
            logger.debug(f"Skipped rule: {name} (ID: {rule_id})")

if __name__ == "__main__":
    cleanup_old_rules()
