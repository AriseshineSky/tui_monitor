from textual.app import App
from textual.binding import Binding
from screens.queue_screen import QueueScreen

class MonitorApp(App):
    CSS = """
    Screen {
        background: #0f111a;
    }

    HeaderBar {
        background: #1e1f29;
        color: #00d7ff;
        text-style: bold;
        padding: 1;
    }

    DataTable {
        width: 2fr;
        border: round #44475a;
    }

    .stats {
        width: 1fr;
        border: round #6272a4;
        padding: 1;
        color: #f8f8f2;
        background: #1e1f29;
    }
    """

    BINDINGS = [
        Binding(
            "ctrl+c",
            "quit",
            "Quit",
            tooltip="Quit the app and return to the command prompt.",
            show=False,
            priority=True,
        ),
        Binding("ctrl+y", "help_quit", show=False, system=True),
    ]

    def on_mount(self):
        self.push_screen(QueueScreen())

if __name__ == "__main__":
    MonitorApp().run()
