from textual.widgets import Static
from widgets.sparkline import sparkline


class IndexMonitor(Static):
    def __init__(self):
        super().__init__("Loading index stats...")

    def update_rows(self, stats: dict):
        """
        stats = {
          "uk_catalog": [12, 14, 18...],
          "us_catalog": [5, 8, 10...]
        }
        """
        lines = ["[b]Index Monitor [/b]\n"]

        for name, data in stats.items():
            if data and isinstance(data[0], tuple):
                counts = [v for _, v in data]
            else:
                counts = data

            if not counts:
                lines.append(
                    f"[b]{name.upper():<12}[/b] "
                    f"[red]NO DATA[/red]  "
                    f"[dim]pipeline stopped? mapping issue?[/dim]"
                )
                lines.append("")
                continue

            line = sparkline(counts)
            now_speed = counts[-1]

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
                f"[b]{name.upper():<12}[/b] {line}  "
            )
            lines.append(f"{'':12} {hourly_str}")
            lines.append("")  # 空行分隔

        self.update("\n".join(lines))

