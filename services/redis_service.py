import redis


class RedisService:
    def __init__(self, redis_url):
        self.r = redis.from_url(redis_url, decode_responses=True)

    def get_queue_lengths(self):
        keys = self.r.keys("Spapi*")
        data = []

        for k in keys:
            if "_kombu" in k:
                continue

            if self.r.type(k) != "list":
                continue
            size = self.r.llen(k)
            data.append((k, size))

        return sorted(data, key=lambda x: x[1], reverse=True)
