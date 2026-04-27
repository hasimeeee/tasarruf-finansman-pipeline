import sys
import os

sys.path.append(os.path.dirname(__file__))
from utils.logger import get_logger
from etl_pipeline import run_pipeline

log = get_logger("scheduler")

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from config_loader import load_config

cfg = load_config()
sched_cfg = cfg["scheduler"]
def scheduled_job():
    log.info("Zamanlanmis gorev tetiklendi.")
    run_pipeline()


if __name__ == "__main__":
    run_time = sched_cfg.get("run_time", "02:00")
    hour, minute = map(int, run_time.split(":"))
    timezone = sched_cfg.get("timezone", "Europe/Istanbul")

    scheduler = BlockingScheduler(timezone=timezone)
    scheduler.add_job(
        scheduled_job,
        trigger=CronTrigger(hour=hour, minute=minute, timezone=timezone),
        id="etl_daily",
        misfire_grace_time=300,
        coalesce=True,
    )

    log.info(f"Scheduler basladi. Her gun {run_time} ({timezone}) calisacak.")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler durduruldu.")