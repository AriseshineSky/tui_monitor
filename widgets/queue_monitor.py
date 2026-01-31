from textual.widgets import Static
from widgets.sparkline import sparkline


class QueueMonitor(Static):
    def __init__(self):
        super().__init__("Loading queue stats...")

    def update_queues(self, queue_data: dict):
        lines = ["[b]Redis Queue Monitor [/b]\n"]

        for name, length in queue_data:
            if length > 500:
                color = "[red]"
            elif length > 100:
                color = "[yellow]"
            else:
                color = "[green]"

            lines.append(f"{name:<40} {color}{length}{color}")
        self.update("\n".join(lines))

