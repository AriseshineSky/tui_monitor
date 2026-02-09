from textual.widgets import Static
from widgets.sparkline import sparkline
from collections import defaultdict
from datetime import datetime


class TaskStatsMonitor(Static):
    def __init__(self):
        super().__init__("Loading index stats...")

    def update_rows(self, hourly_stats):
        lines = ["[b]Task Stats Monitor[/b]\n"]

        for key, hour_list in hourly_stats.items():
            worker, marketplace = key.split("|")
            counts = [h["num_asins"] for h in hour_list]
            success_counts = [h["successful_asins"] for h in hour_list]
            fail_counts = [h["failed_asins"] for h in hour_list]

            # sparkline 显示 num_asins
            line = sparkline(counts)
            now_speed = counts[-1] if counts else 0

            # 计算趋势 ↑ ↓ →
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

            # 每小时数字显示
            hourly_str = " ".join(f"{c:3}" for c in counts)

            # 拼接显示内容
            lines.append(
                f"[b]{worker} | {marketplace:<6}[/b] {line}  "
            )
            lines.append(f"{'':20} {hourly_str}")

            # 可选：显示成功和失败数量
            success_str = " ".join(f"{s:3}" for s in success_counts)
            fail_str = " ".join(f"{f:3}" for f in fail_counts)
            lines.append(f"{'':20} success: {success_str}")
            lines.append(f"{'':20} failed:  {fail_str}")
            lines.append("")  # 空行分隔

        # 更新 Textual widget
        self.update("\n".join(lines))

