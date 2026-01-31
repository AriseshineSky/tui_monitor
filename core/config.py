import os
from dotenv import load_dotenv
load_dotenv()

ES_HOST = os.getenv("ES_HOST", "localhost")
ES_PORT = int(os.getenv("ES_PORT", 9200))
ES_USER = os.getenv("ES_USER")
ES_PASSWORD = os.getenv("ES_PASSWORD")
ES_SCHEME = os.getenv("ES_SCHEME", "http")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
ES_INDEXES = os.getenv("ES_INDEXES", "").split(",")
ES_INDEXES = [i.strip() for i in ES_INDEXES if i.strip()]

def parse_monitors(raw):
    result = {}
    for item in raw.split(","):
        if ":" in item:
            name, index = item.split(":", 1)
            result[name.strip()] = index.strip()
    return result

ES_MONITORS = parse_monitors(os.getenv("ES_MONITORS", ""))

