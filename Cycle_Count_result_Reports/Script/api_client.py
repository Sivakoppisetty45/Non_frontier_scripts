import requests
from tenacity import retry, stop_after_attempt, wait_fixed
from Cycle_Count_result_Reports.config.settings import API_KEY, ACCOUNT, API_ENDPOINT
from Cycle_Count_result_Reports.config.logging_config import configure_logging

logger = configure_logging()

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_data(nrql_query):
    """Fetch data from New Relic API with retry logic"""
    logger.info(f'Fetching data from New Relic: {nrql_query}')
    headers = {'API-Key': API_KEY, 'Content-Type': 'application/json'}
    query = f"""
    {{
      actor {{
        nrql(
          query:"{nrql_query}"
          accounts: {ACCOUNT}
        ) {{
          results
        }}
      }}
    }}
    """
    payload = {'query': query}
    response = requests.post(API_ENDPOINT, headers=headers, json=payload)
    logger.info(f'Response received. Status Code: {response.status_code}')

    if 'errors' in response.json():
        logger.error(f"New Relic returned error response: {response.json()['errors'][0]['message']}")
        return []

    response.raise_for_status()
    return response.json()['data']['actor']['nrql']['results']
