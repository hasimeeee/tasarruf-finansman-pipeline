import os
import sys
"""
Tasarruf Finansman - ETL Pipeline Scheduler
Her gece 02:00'da otomatik çalışır (APScheduler).

Kurulum:
    pip install apscheduler

Çalıştırma:
    python scheduler.py
"""



# etl_pipeline.py ile aynı dizinde olduğu varsayılıyor
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

try:
    from logger import get_logger
    log = get_logger("scheduler")
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    log = logging.getLogger("scheduler")

from etl_pipeline import run_pipeline


def job_listener(event):
    """Job tamamlandığında veya hata olduğunda logla."""
    if event.exception:
        log.error(f"Pipeline JOB HATASI: {event.exception}")
    else:
        log.info("Pipeline job başarıyla tamamlandı.")


def main():
    scheduler = BlockingScheduler(timezone="Europe/Istanbul")

    # Her gece 02:00'da çalış
    scheduler.add_job(
    func=run_pipeline,
    trigger="cron",
    hour=2,
    minute=0,
    id="etl_pipeline_daily",
    name="Tasarruf Finansman ETL Pipeline",
    max_instances=1,
    misfire_grace_time=3600,
)
    scheduler.add_listener(job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    log.info("Scheduler başlatıldı — her gece 02:00 (Europe/Istanbul) pipeline çalışacak.")
    log.info("Durdurmak için Ctrl+C")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler durduruldu.")


if __name__ == "__main__":
    main()