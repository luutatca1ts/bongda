"""Main entry point — starts Telegram bot + scheduled analysis pipeline."""

import asyncio
import logging
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from src.db.models import init_db
from src.bot.telegram_bot import create_bot_app, send_alert, check_quota_alert
from src.pipeline import run_analysis_pipeline, update_results, generate_daily_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def scheduled_analysis(app):
    """Run analysis pipeline and send alerts."""
    logger.info("[Scheduler] Running analysis pipeline...")
    try:
        # Run blocking IO in thread to not block bot
        loop = asyncio.get_event_loop()
        alerts = await loop.run_in_executor(None, run_analysis_pipeline)
        if alerts:
            logger.info(f"[Scheduler] Sending {len(alerts)} alerts...")
            for alert in alerts:
                await send_alert(app, alert)
        else:
            logger.info("[Scheduler] No value bets found this cycle.")
        # Check API quota after analysis and alert if low
        await check_quota_alert(app)
    except Exception as e:
        logger.error(f"[Scheduler] Analysis failed: {e}", exc_info=True)


async def scheduled_results_update(app):
    """Update match results and notify."""
    logger.info("[Scheduler] Updating results...")
    try:
        loop = asyncio.get_event_loop()
        updated = await loop.run_in_executor(None, update_results)
        if updated:
            msg = "\U0001f4ca K\u1ebeT QU\u1ea2 C\u1eacP NH\u1eacT\n\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n"
            msg += "\n".join(updated)
            await send_alert(app, msg)
    except Exception as e:
        logger.error(f"[Scheduler] Results update failed: {e}", exc_info=True)


async def scheduled_daily_report(app):
    """Send daily summary report."""
    logger.info("[Scheduler] Generating daily report...")
    try:
        loop = asyncio.get_event_loop()
        report = await loop.run_in_executor(None, generate_daily_report)
        await send_alert(app, report)
    except Exception as e:
        logger.error(f"[Scheduler] Daily report failed: {e}", exc_info=True)


def main():
    init_db()
    logger.info("Database initialized.")

    app = create_bot_app()
    logger.info("Telegram bot created.")

    async def run():
        async with app:
            # Start polling FIRST so bot responds immediately
            await app.updater.start_polling(drop_pending_updates=True)
            await app.start()
            logger.info("Bot is running and accepting commands!")

            # THEN start scheduler (delayed startup analysis by 10s)
            scheduler = AsyncIOScheduler()

            scheduler.add_job(
                scheduled_analysis,
                "interval",
                minutes=30,
                args=[app],
                id="analysis",
                name="Football Analysis Pipeline",
            )
            # Delay first run by 10 seconds so bot is ready
            scheduler.add_job(
                scheduled_analysis,
                "date",
                run_date=datetime.now() + timedelta(seconds=10),
                args=[app],
                id="analysis_startup",
                name="Startup Analysis",
            )
            scheduler.add_job(
                scheduled_results_update,
                "interval",
                hours=2,
                args=[app],
                id="results_update",
                name="Results Update",
            )
            scheduler.add_job(
                scheduled_daily_report,
                "cron",
                hour=23,
                minute=0,
                args=[app],
                id="daily_report",
                name="Daily Report",
            )
            scheduler.start()
            logger.info("Scheduler started: analysis/30min, results/2h, report/23:00")

            # Keep running
            stop_event = asyncio.Event()
            try:
                await stop_event.wait()
            except asyncio.CancelledError:
                pass
            finally:
                scheduler.shutdown(wait=False)
                await app.updater.stop()
                await app.stop()

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Bot stopped.")


if __name__ == "__main__":
    main()
