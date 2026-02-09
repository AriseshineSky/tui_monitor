from textual.screen import Screen
from textual.containers import VerticalScroll, Vertical, Horizontal
from textual.widgets import Static
import asyncio
from datetime import datetime

from widgets.header import HeaderBar
from widgets.index_monitor import IndexMonitor
from widgets.task_stats_monitor import TaskStatsMonitor
from widgets.queue_monitor import QueueMonitor

from services.redis_service import RedisService
from services.es_service import EsService
from core.config import REDIS_URL, ES_MONITORS, ES_INDEXES
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
                # VerticalScroll(self.queue_monitor, id="queue_scroll"),
                VerticalScroll(
                    VerticalScroll(self.monitor, id="monitor_scroll"),
                ),
                VerticalScroll(
                    VerticalScroll(self.task_stats_monitor, id="task_stats_monitor")
                )
            )
        )

    async def on_mount(self):
        self.redis = RedisService(REDIS_URL)
        self.set_interval(2, lambda: asyncio.create_task(self.refresh_queues()))
        self.set_interval(30, lambda: asyncio.create_task(self.refresh_hourly_docs()))
        self.set_interval(30, lambda: asyncio.create_task(self.refresh_task_stats()))

        await self.refresh_hourly_docs()
        await self.refresh_queues()
        await self.refresh_task_stats()

    async def refresh_task_stats(self):
        try:
            all_stats = {}

            for index in ES_INDEXES:
                with open("task_stats_debug.log", "a") as f:
                    f.write(f"{index}")

                if not index.startswith("task_stats_"):
                    continue

                hourly_stats = self.es_service.get_hourly_task_stats(index, 3)
                all_stats.update(hourly_stats)

                with open("task_stats_debug.log", "a") as f:
                    f.write(f"{datetime.now()} {index}: {hourly_stats}\n")

            self.task_stats_monitor.update_rows(all_stats)

        except Exception as e:
            # 出错时显示占位信息
            self.task_stats_monitor.update_rows({
                "error|error": [{"num_asins": 0, "successful_asins": 0, "failed_asins": 0}]
            })

    async def refresh_queues(self):
        if not self.redis:
            return

        queue_data = self.redis.get_queue_lengths()
        self.queue_monitor.update_queues(queue_data)

    async def refresh_hourly_docs(self):
        stats = {}
        for name, index in ES_MONITORS.items():
            try:
                data = self.es_service.get_hourly_doc_count(index, 3)
                stats[name] = data
            except Exception:
                stats[name] = [0]
        self.monitor.update_rows(stats)
