"""Scheduled Reports — daily report generation with cron scheduling."""

from __future__ import annotations

import asyncio
import random
from datetime import datetime

from gravtory import Gravtory, step


_DB = __import__("pathlib").Path(__file__).parent / "scheduled_reports.db"
grav = Gravtory(f"sqlite:///{_DB}")


@grav.workflow(id="report-{report_date}")
class DailyReport:
    """Query → Charts → Compile → Send. Scheduled for 8 AM weekdays."""

    @step(1)
    async def query_data(self, report_date: str = "2025-01-15") -> dict:
        print(f"[report-{report_date}] Step 1: Querying daily statistics...")
        await asyncio.sleep(0.05)
        stats = {
            "total_orders": random.randint(100, 500),
            "revenue": round(random.uniform(5000, 25000), 2),
            "new_users": random.randint(10, 50),
            "active_users": random.randint(200, 1000),
            "error_rate": round(random.uniform(0.001, 0.05), 4),
            "avg_response_ms": random.randint(50, 200),
            "date": report_date,
        }
        print(f"[report-{report_date}] Step 1: Queried — {stats['total_orders']} orders, ${stats['revenue']} revenue")
        return stats

    @step(2, depends_on=1)
    async def generate_charts(self, report_date: str = "2025-01-15") -> dict:
        print(f"[report-{report_date}] Step 2: Generating charts...")
        await asyncio.sleep(0.05)
        charts = [
            {"name": "orders_trend", "type": "line", "data_points": 24},
            {"name": "revenue_breakdown", "type": "bar", "data_points": 5},
            {"name": "user_activity", "type": "area", "data_points": 24},
            {"name": "error_rate", "type": "line", "data_points": 24},
        ]
        print(f"[report-{report_date}] Step 2: Generated {len(charts)} charts")
        return {"charts": charts}

    @step(3, depends_on=2)
    async def compile_report(self, report_date: str = "2025-01-15") -> dict:
        print(f"[report-{report_date}] Step 3: Compiling HTML report...")
        await asyncio.sleep(0.05)
        report_html = f"<html><body><h1>Daily Report — {report_date}</h1></body></html>"
        print(f"[report-{report_date}] Step 3: Report compiled ({len(report_html)} bytes)")
        return {"html": report_html, "size_bytes": len(report_html)}

    @step(4, depends_on=3)
    async def send_email(self, report_date: str = "2025-01-15") -> dict:
        recipients = ["team@example.com", "manager@example.com"]
        print(f"[report-{report_date}] Step 4: Sending report to {len(recipients)} recipients...")
        await asyncio.sleep(0.05)
        print(f"[report-{report_date}] Step 4: Report sent!")
        return {"sent_to": recipients, "sent_at": datetime.now().isoformat()}


async def main() -> None:
    await grav.start()
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        result = await grav.run(DailyReport, report_date=today)
        print(f"[report-{today}] Workflow completed: {result.status.value}")
    finally:
        await grav.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
