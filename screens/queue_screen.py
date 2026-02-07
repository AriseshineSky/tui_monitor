from textual.screen import Screen
from textual.containers import VerticalScroll, Vertical, Horizontal
from textual.widgets import Static
import asyncio

from widgets.header import HeaderBar
from widgets.index_monitor import IndexMonitor
from widgets.task_stats_monitor import TaskStatsMonitor
from widgets.queue_monitor import QueueMonitor

from services.redis_service import RedisService
from services.es_service import EsService
from core.config import REDIS_URL, ES_MONITORS
from core.state import state


class QueueScreen(Screen):

    def __init__(self):
        super().__init__()
        self.redis = None
        self.es_service = EsService()
        self.task_stats_monitor = TaskStatsMonitor()
        self.monitor = IndexMonitor()
        self.queue_monitor = QueueMonitor()
        self.stats = Static("Loading...", classes="stats")

    def compose(self):
        yield Vertical(
            HeaderBar(),
            Horizontal(
                VerticalScroll(self.queue_monitor, id="queue_scroll"),
                VerticalScroll(self.monitor, id="monitor_scroll"),
                VerticalScroll(self.task_stats_monitor, id="task_stats_monitor"),
            )
        )

    async def on_mount(self):
        self.redis = RedisService(REDIS_URL)
        self.set_interval(2, lambda: asyncio.create_task(self.refresh_queues()))
        self.set_interval(30, lambda: asyncio.create_task(self.refresh_hourly_docs()))
        self.set_interval(30, lambda: asyncio.create_task(self.refresh_task_stats()))

        await self.refresh_hourly_docs()
        await self.refresh_queues()

    async def refresh_task_stats(self):
        stats = {}
        for name, index in ES_MONITORS.items():
            if not index.startswith("task_stats_"):
                continue
            try:
                data = self.es_service.get_hourly_doc_count(index, hours=1)  # [(minute, doc_count), ...]
                
                display_data = []
                for minute_str, doc_count in data:
                    # 假设我们能获取成功/失败数量，这里用 0 占位
                    successful_asins = 0
                    failed_asins = 0
                    display_data.append(f"{index} {minute_str}: {doc_count} docs (success={successful_asins}, failed={failed_asins})")

                stats[index] = display_data

            except Exception as e:
                stats[index] = [f"Error: {str(e)}"]

        self.task_stats_monitor.update_rows(stats)

    async def refresh_queues(self):
        if not self.redis:
            return

        queue_data = self.redis.get_queue_lengths()
        self.queue_monitor.update_queues(queue_data)

    async def refresh_hourly_docs(self):
        stats = {}
        for name, index in ES_MONITORS.items():
            try:
                data = self.es_service.get_hourly_doc_count(index, 5)
                stats[name] = data
            except Exception:
                stats[name] = [0]
        self.monitor.update_rows(stats)
