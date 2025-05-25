#!/usr/bin/env python3

import os
import sys
import time
import signal
import logging
import argparse
from datetime import datetime, timedelta

from apscheduler.schedulers.blocking import BlockingScheduler

from image_processor import EnhancedImageProcessor, Config
from batch_manager import BatchManager
from db_handler import DatabaseHandler

# === LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("image_processor.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("ImageProcessor")

# === GLOBALS ===
scheduler = None
processor = None
running_job = False
terminate = False

# === SIGNAL HANDLERS ===
def setup_signal_handlers():
    def handle_signal(signum, frame):
        global terminate, scheduler
        log.info(f"Signal {signal.Signals(signum).name} ({signum}) received")
        terminate = True
        if scheduler:
            log.info("Shutting down scheduler...")
            scheduler.shutdown()
        sys.exit(0)
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

# === UTILS ===
def is_time_to_run():
    return datetime.now().hour >= 9

# === DB PREP ===
def prepare_database():
    config = Config("config.json")
    db_handler = DatabaseHandler(config.get_db_config())
    db_handler.ensure_schema()
    total = 0
    while True:
        migrated = db_handler.migrate_existing_images()
        if not migrated:
            break
        total += migrated
        log.info(f"Migrated {total} images so far")
    log.info(f"Database prep done, total migrated: {total}")
    db_handler.close()

# === JOBS ===
def process_images_job():
    global running_job, processor
    if running_job or not is_time_to_run():
        log.info("Job already running or too early")
        return
    try:
        running_job = True
        log.info("Scheduled image processing job started")
        processor.run()
        log.info("Processing job complete")
    except Exception as e:
        log.error(f"Processing job failed: {str(e)}")
    finally:
        running_job = False

def process_duplicates_job():
    global running_job, processor
    if running_job:
        log.info("Job already running, skipping")
        return
    try:
        running_job = True
        log.info("Scheduled deduplication job started")
        processor.process_all_duplicates()
        log.info("Deduplication job complete")
    except Exception as e:
        log.error(f"Deduplication job failed: {str(e)}")
    finally:
        running_job = False

# === SERVICE RUN ===
def run_service(config_file, run_once=False, dedup_only=False, complete=False):
    global scheduler, processor
    try:
        log.info("Service starting")
        prepare_database()
        processor = EnhancedImageProcessor(config_file)
        if not run_once:
            scheduler = BlockingScheduler()
            if dedup_only:
                scheduler.add_job(process_duplicates_job, 'date', run_date=datetime.now() + timedelta(seconds=10))
                scheduler.add_job(process_duplicates_job, 'cron', hour=0, minute=0)
            elif complete:
                if is_time_to_run():
                    scheduler.add_job(lambda: processor.run_process_and_deduplicate(),
                                     'date', run_date=datetime.now() + timedelta(seconds=10))
                scheduler.add_job(lambda: processor.run_process_and_deduplicate(),
                                 'cron', hour=9, minute=0)
            else:
                if is_time_to_run():
                    scheduler.add_job(process_images_job, 'date', run_date=datetime.now() + timedelta(seconds=10))
                scheduler.add_job(process_images_job, 'cron', hour='*', minute=0)
            log.info("Scheduler started")
            scheduler.start()
        else:
            if dedup_only:
                processor.process_all_duplicates()
            elif complete:
                processor.run_process_and_deduplicate()
            elif is_time_to_run():
                process_images_job()
            else:
                log.info("Not running: too early")
    except (KeyboardInterrupt, SystemExit):
        log.info("Service interrupted, shutting down")
    except Exception as e:
        log.error(f"Service error: {str(e)}")
    finally:
        if scheduler:
            scheduler.shutdown()
        if processor:
            processor.close()
        log.info("Service terminated")

# === MAIN ===
def main():
    parser = argparse.ArgumentParser(description="Image Processing Service")
    parser.add_argument("--config", default="config.json", help="Path to configuration file")
    subparsers = parser.add_subparsers(dest="command", help="Command")

    batch_parser = subparsers.add_parser("batch", help="Process in batches with Redis")
    batch_parser.add_argument("--batch-size", type=int, default=100)
    batch_parser.add_argument("--limit", type=int, default=None)

    run_parser = subparsers.add_parser("run", help="Run service (traditional)")
    run_parser.add_argument("--once", action="store_true")

    test_parser = subparsers.add_parser("test", help="Test mode")
    test_parser.add_argument("--entries", type=int, default=None)

    migrate_parser = subparsers.add_parser("migrate", help="Migrate DB schema")

    args = parser.parse_args()
    if not args.command:
        args.command = "batch"

    if args.command == "batch":
        run_batched_processing(args.config, args.batch_size, args.limit)
    elif args.command == "run":
        setup_signal_handlers()
        run_service(args.config, args.once)
    elif args.command == "test":
        run_test(args.config, args.entries)
    elif args.command == "migrate":
        prepare_database()

if __name__ == "__main__":
    t1 = time.time()
    main()
    print(time.time() - t1)