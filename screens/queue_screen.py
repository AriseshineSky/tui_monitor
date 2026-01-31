from textual.screen import Screen
from textual.containers import VerticalScroll, Vertical
from textual.widgets import Static
import asyncio

from widgets.header import HeaderBar
from widgets.index_monitor import IndexMonitor

from services.redis_service import RedisService
from services.es_service import EsService
from core.config import REDIS_URL, ES_MONITORS
from core.state import state


class QueueScreen(Screen):

    def __init__(self):
        super().__init__()
        self.redis = None
        self.es_service = EsService()

        self.monitor = IndexMonitor()
        self.stats = Static("Loading...", classes="stats")

    def compose(self):
        yield Vertical(
            HeaderBar(),
            VerticalScroll(
                self.monitor,
                id="bottom_panel"
            )
        )

    async def on_mount(self):
        self.redis = RedisService(REDIS_URL)

        self.set_interval(2, lambda: asyncio.create_task(self.refresh_queues()))
        self.set_interval(30, lambda: asyncio.create_task(self.refresh_hourly_docs()))

        await self.refresh_hourly_docs()
        await self.refresh_queues()

    async def refresh_queues(self):
        if not self.redis:
            return
        state.queue_data = self.redis.get_queue_lengths()

        total = sum(size for _, size in state.queue_data)
        self.stats.update(
            f"[b]Total Pending:[/b] {total}\n"
            f"[b]Queues:[/b] {len(state.queue_data)}"
        )

    async def refresh_hourly_docs(self):
        stats = {}
        for name, index in ES_MONITORS.items():
            try:
                data = self.es_service.get_hourly_doc_count(index, 5)
                stats[name] = data
            except Exception:
                stats[name] = [0]
        self.monitor.update_rows(stats)
