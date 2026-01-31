from textual.widgets import Static
from widgets.sparkline import sparkline

class HourlyChart(Static):
    def update_chart(self, data):
        line = sparkline(data)
        total = sum(data)
        self.update(f"[b]Docs/hour (24h)[/b]\n{line}\nTotal: {total}")
