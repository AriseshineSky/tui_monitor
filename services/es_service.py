from datetime import datetime, timezone, timedelta
from elasticsearch import Elasticsearch

from core.config import ES_HOST, ES_PORT, ES_USER, ES_PASSWORD, ES_SCHEME

class EsService:
    def __init__(self):
        self.es = Elasticsearch(hosts=ES_HOST, port=ES_PORT, http_auth=(ES_USER, ES_PASSWORD))

    def get_hourly_task_stats(self, index, hours=24):
        """
        聚合每个 worker|marketplace 的 hourly stats
        返回：
        {
            "worker|marketplace": [
                {"num_asins": 50, "successful_asins": 45, "failed_asins": 5},  # 每小时
                ...
            ]
        }
        """
        now = datetime.now(timezone.utc)
        start = now - timedelta(hours=hours)

        # 查询 ES，按时间范围和 worker+marketplace 聚合
        body = {
            "size": 0,
            "query": {
                "range": {
                    "minute": {  # 你文档里时间字段是 minute
                        "gte": start.isoformat(),
                        "lte": now.isoformat(),
                        "format": "strict_date_optional_time"
                    }
                }
            },
            "aggs": {
                "by_worker": {
                    "terms": {"field": "worker.keyword", "size": 1000},
                    "aggs": {
                        "by_marketplace": {
                            "terms": {"field": "marketplace.keyword", "size": 50},
                            "aggs": {
                                "per_hour": {
                                    "date_histogram": {
                                        "field": "minute",
                                        "fixed_interval": "1h",
                                        "min_doc_count": 0,
                                        "format": "yyyy-MM-dd'T'HH:00:00"
                                    },
                                    "aggs": {
                                        "num_asins_sum": {"sum": {"field": "num_asins"}},
                                        "successful_asins_sum": {"sum": {"field": "successful_asins"}},
                                        "failed_asins_sum": {"sum": {"field": "failed_asins"}}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        res = self.es.search(index=index, body=body)
        result = {}

        for worker_bucket in res["aggregations"]["by_worker"]["buckets"]:
            worker = worker_bucket["key"]
            for mp_bucket in worker_bucket["by_marketplace"]["buckets"]:
                marketplace = mp_bucket["key"]
                key = f"{worker}|{marketplace}"
                stats_list = []
                for hour_bucket in mp_bucket["per_hour"]["buckets"]:
                    stats_list.append({
                        "num_asins": int(hour_bucket["num_asins_sum"]["value"]),
                        "successful_asins": int(hour_bucket["successful_asins_sum"]["value"]),
                        "failed_asins": int(hour_bucket["failed_asins_sum"]["value"])
                    })
                result[key] = stats_list

        return result

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

