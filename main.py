"""Main entry point — starts Telegram bot + scheduled analysis pipeline."""

import asyncio
import logging
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from src.db.models import init_db
from src.bot.telegram_bot import create_bot_app, send_alert, check_quota_alert
from src.pipeline import run_analysis_pipeline, update_results, generate_daily_report
from src.analytics.steam_detector import detect_steam_moves, format_steam_alert
from src.analytics.clv import capture_closing_lines
from src.analytics.line_movement import cleanup_old_history
from src.live_pipeline import run_live_pipeline

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


async def scheduled_steam_check(app):
    """Phát hiện steam move mỗi 15 phút và gửi alert."""
    logger.info("[Scheduler] Running steam check...")
    try:
        loop = asyncio.get_event_loop()
        steams = await loop.run_in_executor(None, detect_steam_moves)
        if steams:
            logger.info(f"[Scheduler] Sending {len(steams)} steam alerts...")
            for s in steams:
                await send_alert(app, format_steam_alert(s))
        else:
            logger.info("[Scheduler] No steam moves this cycle.")
    except Exception as e:
        logger.error(f"[Scheduler] Steam check failed: {e}", exc_info=True)


async def scheduled_clv_capture(app):
    """Capture closing odds cho các trận sắp kickoff (≤45 phút)."""
    logger.info("[Scheduler] Capturing closing lines...")
    try:
        loop = asyncio.get_event_loop()
        n = await loop.run_in_executor(None, capture_closing_lines)
        logger.info(f"[Scheduler] CLV captured for {n} predictions.")
    except Exception as e:
        logger.error(f"[Scheduler] CLV capture failed: {e}", exc_info=True)


async def scheduled_cleanup(app):
    """Xóa OddsHistory cũ hơn 30 ngày (chạy 3:30 sáng)."""
    logger.info("[Scheduler] Running odds_history cleanup...")
    try:
        loop = asyncio.get_event_loop()
        n = await loop.run_in_executor(None, cleanup_old_history, 30)
        logger.info(f"[Scheduler] Cleanup removed {n} old odds_history rows.")
    except Exception as e:
        logger.error(f"[Scheduler] Cleanup failed: {e}", exc_info=True)


async def scheduled_live_analysis(app):
    """Live pipeline — chạy mỗi 2 phút trong cửa sổ giờ có nhiều trận live."""
    # Chỉ chạy trong 00:00-06:00 và 18:00-24:00 (giờ local server)
    hour = datetime.now().hour
    if not (hour < 6 or hour >= 18):
        return
    logger.info("[Scheduler] Running live analysis...")
    try:
        loop = asyncio.get_event_loop()
        alerts = await loop.run_in_executor(None, run_live_pipeline)
        if alerts:
            logger.info(f"[Scheduler] Sending {len(alerts)} live VB alerts...")
            for alert in alerts:
                await send_alert(app, alert)
        else:
            logger.info("[Scheduler] No live value bets this cycle.")
    except Exception as e:
        logger.error(f"[Scheduler] Live analysis failed: {e}", exc_info=True)


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
                scheduled_steam_check,
                "interval",
                minutes=15,
                args=[app],
                id="steam_check",
                name="Steam Move Detection",
            )
            scheduler.add_job(
                scheduled_clv_capture,
                "interval",
                minutes=15,
                args=[app],
                id="clv_capture",
                name="CLV Capture",
            )
            scheduler.add_job(
                scheduled_cleanup,
                "cron",
                hour=3,
                minute=30,
                args=[app],
                id="cleanup_odds_history",
                name="Cleanup Old Odds History",
            )
            scheduler.add_job(
                scheduled_live_analysis,
                "interval",
                minutes=2,
                args=[app],
                id="live_analysis",
                name="Live In-Play Analysis",
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
            logger.info(
                "Scheduler started: analysis/30min, results/2h, steam/15min, "
                "clv/15min, cleanup/03:30, live/2min (18-06h), report/23:00"
            )

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
