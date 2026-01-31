from textual.widgets import Static

class HeaderBar(Static):
    def compose(self):
        yield Static("🚀 SP-API MONITOR", classes="title")

