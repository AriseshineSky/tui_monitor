from datetime import datetime, timezone, timedelta
from elasticsearch import Elasticsearch

from core.config import ES_HOST, ES_PORT, ES_USER, ES_PASSWORD, ES_SCHEME

class EsService:
    def __init__(self):
        self.es = Elasticsearch(hosts=ES_HOST, port=ES_PORT, http_auth=(ES_USER, ES_PASSWORD))

    def get_hourly_doc_count(self, index, hours=24):
        now = datetime.now(timezone.utc)
        start = now - timedelta(hours=hours)
        body = {
            "size": 0,
            "query": {
                "range": {
                    "time": {
                        "gte": start.isoformat(),
                        "lte": now.isoformat(),
                        "format": "strict_date_optional_time"
                    }
                }
            },
            "aggs": {
                "per_hour": {
                    "date_histogram": {
                        "field": "time",
                        "fixed_interval": "1h",
                        "min_doc_count": 0,
                        "format": "HH:mm"
                    }
                }
            }
        }

        res = self.es.search(index=index, body=body)
        buckets = res["aggregations"]["per_hour"]["buckets"]
        return [(b["key_as_string"], b["doc_count"]) for b in buckets]

