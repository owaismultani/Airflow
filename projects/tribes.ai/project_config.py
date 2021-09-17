import logging
import sys
from datetime import datetime
from pathlib import Path


Path("./src/logs").mkdir(parents=True, exist_ok=True)
logging.basicConfig(
                    level=logging.INFO,
                    filename=f"src/logs/{datetime.now().date()}.log",
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    filemode='a')
LOGGER = logging.getLogger()
LOGGER.addHandler(logging.StreamHandler(sys.stdout))


NEO4J_URL = 'bolt://neo4j:test@localhost:7687'

USERS = ["vinit@tribes.ai", "guilermo@tribes.ai", "christian@tribes.ai", "elly@tribes.ai", "owais@tribes.ai"]

HISTORICAL_DATA_SINCE = 30
SCHEDULE_CRON = "0 1 * * *"
DAG_NAME = "tribes.ai"


app_config = {
    "device_info": [{"brand": "apple", "os": "ios"}, {"brand": "samsung", "os": "android"},
                    {"brand": "OnePlus", "os": "android"}],
    "app_info": [{"app_name": "slack", "app_category": "communication"},
                 {"app_name": "gmail", "app_category": "communication"},
                 {"app_name": "jira", "app_category": "task_management"},
                 {"app_name": "google drive", "app_category": "file_management"},
                 {"app_name": "chrome", "app_category": "web_browser"},
                 {"app_name": "spotify", "app_category": "entertainment_music"}],
    "lower_usage_limit": 0,
    "upper_usage_limit": 480
}
