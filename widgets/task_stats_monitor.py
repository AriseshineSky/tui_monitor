from textual.widgets import Static
from widgets.sparkline import sparkline
from collections import defaultdict
from datetime import datetime


class TaskStatsMonitor(Static):
    def __init__(self):
        super().__init__("Loading index stats...")

    def aggregate_hourly(self, docs):
        """
        docs: list of raw ES hits
        返回:
          {
            "celery@t003-eu|de": [ {"num_asins": 20, "successful_asins":10, "failed_asins":10}, ... 每小时列表 ]
          }
        """
        agg = defaultdict(lambda: defaultdict(lambda: {"num_asins":0, "successful_asins":0, "failed_asins":0}))

        for doc in docs:
            src = doc["_source"]
            worker = src["worker"]
            marketplace = src["marketplace"]
            key = f"{worker}|{marketplace}"

            minute_dt = datetime.fromisoformat(src["minute"].replace("Z",""))
            hour_bucket = minute_dt.replace(minute=0, second=0, microsecond=0)

            agg[key][hour_bucket]["num_asins"] += src.get("num_asins",0)
            agg[key][hour_bucket]["successful_asins"] += src.get("successful_asins",0)
            agg[key][hour_bucket]["failed_asins"] += src.get("failed_asins",0)

        # 转成每小时升序列表
        result = {}
        for key, hour_dict in agg.items():
            sorted_hours = sorted(hour_dict.items())
            result[key] = [{"num_asins": v["num_asins"],
                            "successful_asins": v["successful_asins"],
                            "failed_asins": v["failed_asins"]} 
                           for _, v in sorted_hours]
        return result

    def update_rows(self, docs):
        """
        docs: ES hits
        """
        hourly_stats = self.aggregate_hourly(docs)

        lines = ["[b]Task Stats Monitor[/b]\n"]

        for key, hour_list in hourly_stats.items():
            worker, marketplace = key.split("|")
            counts = [h["num_asins"] for h in hour_list]
            success_counts = [h["successful_asins"] for h in hour_list]
            fail_counts = [h["failed_asins"] for h in hour_list]

            line = sparkline(counts)
            total = sum(counts)
            now_speed = counts[-1] if counts else 0

            if len(counts) > 1:
                avg_prev = sum(counts[:-1]) / (len(counts) - 1)
                if now_speed > avg_prev:
                    trend = "[green]⬆[/green]"
                elif now_speed < avg_prev:
                    trend = "[red]⬇[/red]"
                else:
                    trend = "→"
            else:
                trend = "→"

            hourly_str = " ".join(f"{c:3}" for c in counts)

            lines.append(
                f"[b]{worker} | {marketplace:<6}[/b] {line}  "
                f"now:{now_speed}/h  {trend}  total:{total}"
            )
            lines.append(f"{'':20} {hourly_str}")
            lines.append("")  # 空行分隔

        self.update("\n".join(lines))

