#!/usr/bin/env python3
import asyncio
import os
import sys
import time
import signal
import logging
import argparse
from datetime import datetime, timedelta
from io import BytesIO

from apscheduler.schedulers.blocking import BlockingScheduler

from image_processor import EnhancedImageProcessor, Config
from batch_manager import BatchManager
from db_handler import DatabaseHandler

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("image_processor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ImageProcessor")

# Global variables
scheduler = None
processor = None
running_job = False
terminate = False


def setup_signal_handlers():
    """Set up signal handlers for graceful shutdown."""

    def handle_signal(signum, frame):
        global terminate, scheduler
        signame = signal.Signals(signum).name
        logger.info(f"Received signal {signame} ({signum})")
        terminate = True
        if scheduler:
            logger.info("Shutting down scheduler...")
            scheduler.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)


def is_time_to_run():
    """Check if it's time to run the job (after 9:00 AM)."""
    now = datetime.now()
    return now.hour >= 9


def prepare_database():
    """Prepare database for optimized processing."""
    config = Config("config.json")
    db_handler = DatabaseHandler(config.get_db_config())

    # Ensure schema is up to date
    db_handler.ensure_schema()

    # Migrate existing images
    total_migrated = 0
    batch_size = 1000
    while True:
        migrated = db_handler.migrate_existing_images()
        if migrated == 0:
            break
        total_migrated += migrated
        logger.info(f"Migrated {total_migrated} images so far")

    logger.info(f"Database preparation complete, migrated {total_migrated} images")
    db_handler.close()


def process_images_job():
    """Run the image processing job if it's after 9 AM."""
    global running_job, processor

    if running_job:
        logger.info("A job is already running, skipping...")
        return

    if not is_time_to_run():
        logger.info("Not yet time to run (before 9:00 AM), skipping...")
        return

    try:
        running_job = True
        logger.info("Starting scheduled image processing job")

        # Run the processor (without deduplication)
        processor.run()

        # Log completion
        logger.info("Scheduled processing job completed successfully")
    except Exception as e:
        logger.error(f"Scheduled processing job failed: {str(e)}")
    finally:
        running_job = False


def process_duplicates_job():
    """Run the deduplication job."""
    global running_job, processor

    if running_job:
        logger.info("A job is already running, skipping...")
        return

    try:
        running_job = True
        logger.info("Starting scheduled duplicate detection job")

        # Run only the duplicate detection phase
        processor.process_all_duplicates()

        # Log completion
        logger.info("Scheduled duplicate detection job completed successfully")
    except Exception as e:
        logger.error(f"Scheduled duplicate detection job failed: {str(e)}")
    finally:
        running_job = False


def run_service(config_file, run_once=False, dedup_only=False, complete=False):
    """Run the service process."""
    global scheduler, processor

    try:
        logger.info("Starting image processor service")

        # Prepare database for optimized processing
        prepare_database()

        # Initialize the processor
        processor = EnhancedImageProcessor(config_file)

        # Set up the scheduler if not in run-once mode
        if not run_once:
            scheduler = BlockingScheduler()

            if dedup_only:
                # Schedule only deduplication jobs
                # Run at startup
                scheduler.add_job(process_duplicates_job, 'date', run_date=datetime.now() + timedelta(seconds=10))

                # Run every day at midnight
                scheduler.add_job(
                    process_duplicates_job,
                    'cron',
                    hour=0,
                    minute=0
                )
            elif complete:
                # Schedule complete jobs (process + dedup)
                # Run at startup if after 9 AM
                if is_time_to_run():
                    # First process images, then run deduplication
                    scheduler.add_job(
                        lambda: processor.run_process_and_deduplicate(),
                        'date',
                        run_date=datetime.now() + timedelta(seconds=10)
                    )

                # Run complete job every day at 9 AM
                scheduler.add_job(
                    lambda: processor.run_process_and_deduplicate(),
                    'cron',
                    hour=9,
                    minute=0
                )
            else:
                # Schedule just image processing jobs
                # Run at startup if after 9 AM
                if is_time_to_run():
                    scheduler.add_job(process_images_job, 'date', run_date=datetime.now() + timedelta(seconds=10))

                # Run every hour
                scheduler.add_job(
                    process_images_job,
                    'cron',
                    hour='*',
                    minute=0
                )

            # Start the scheduler - this will block
            logger.info("Starting scheduler...")
            scheduler.start()
        else:
            # Run once mode
            if dedup_only:
                # Run only deduplication
                processor.process_all_duplicates()
            elif complete:
                # Run complete process (process + dedup)
                processor.run_process_and_deduplicate()
            else:
                # Run only process (if after 9 AM)
                if is_time_to_run():
                    process_images_job()
                else:
                    logger.info("Not running: time is before 9:00 AM")

    except (KeyboardInterrupt, SystemExit):
        logger.info("Service interrupted, shutting down...")
    except Exception as e:
        logger.error(f"Service error: {str(e)}")
    finally:
        if scheduler:
            scheduler.shutdown()
        if processor:
            processor.close()
        logger.info("Service process terminated")


def run_test(config_file, entries=None, dedup_only=False, complete=False):
    """Run in test mode."""
    try:
        setup_signal_handlers()

        # Prepare database for optimized processing
        prepare_database()

        # Initialize processor with test_mode=True
        processor = EnhancedImageProcessor(config_file, test_mode=True)

        if dedup_only:
            # Run only the deduplication phase
            logger.info("Running deduplication in test mode")
            processor.process_all_duplicates()
            processor.log_performance_metrics()
            return

        if entries:
            logger.info(f"Running in test mode with target of {entries} entries")

            # Load configuration
            config = Config(config_file)
            processor_config = config.get_processing_config()
            batch_size = processor_config.get("batch_size", 100)
            required_batches = (entries + batch_size - 1) // batch_size  # Ceiling division

            logger.info(f"Batch size: {batch_size}, estimated batches needed: {required_batches}")

            # Track processed entries
            total_processed = 0
            current_batch = 0

            # Process batches until we reach target entries or run out of data
            while total_processed < entries and current_batch < required_batches:
                # For the last batch, adjust the batch size to hit exactly the target
                if total_processed + batch_size > entries:
                    remaining = entries - total_processed
                    logger.info(f"Adjusting final batch size to {remaining} to reach exactly {entries} entries")

                    # Process a single batch with custom size by temporarily modifying the config
                    old_batch_size = processor.processing_config.get("batch_size")
                    processor.processing_config["batch_size"] = remaining
                    processed = processor.process_batch()
                    processor.processing_config["batch_size"] = old_batch_size
                else:
                    # Process a normal batch
                    processed = processor.process_batch()

                if processed == 0:
                    logger.warning("No more entries to process, database exhausted")
                    break

                total_processed += processed
                current_batch += 1

                logger.info(f"Processed batch {current_batch}: {processed} entries, total: {total_processed}/{entries}")

                # Break if we've reached our target
                if total_processed >= entries:
                    break

            # Log final statistics
            logger.info(f"Test completed. Processed {total_processed} entries in {current_batch} batches")

            # Run deduplication if requested
            if complete:
                logger.info("Processing completed, starting deduplication phase")
                processor.process_all_duplicates()

            processor.log_performance_metrics()

        else:
            # Process just one batch for testing
            logger.info("Running in test mode with default 1 batch")
            if complete:
                processor.run_process_and_deduplicate(max_batches=1)
            else:
                processor.run(max_batches=1)

    except Exception as e:
        logger.error(f"Test error: {str(e)}")
    finally:
        if 'processor' in locals():
            processor.close()


def run_prepare_and_process(config_file, entries, batch_size, include_dedup=False):
    """
    Prepare batches and then process them automatically.

    This function:
    1. Prepares batches of entries from the database
    2. Saves them to a JSON file
    3. Processes each batch sequentially
    4. Optionally runs deduplication after all batches are processed

    Args:
        config_file: Path to the configuration file
        entries: Number of entries to process
        batch_size: Size of each batch
        include_dedup: Whether to run deduplication after processing
    """
    try:
        setup_signal_handlers()

        # Prepare database for optimized processing
        prepare_database()

        logger.info(f"Starting prepare and process with target of {entries} entries and batch size {batch_size}")

        # Initialize config and database handler
        config = Config(config_file)
        db_handler = DatabaseHandler(config.get_db_config())

        # Initialize batch manager
        batch_manager = BatchManager(db_handler, batch_size)

        # Prepare batches
        batch_file, total_batches, total_listings = batch_manager.prepare_batches(entries)

        if not batch_file:
            logger.error("Failed to prepare batches. Exiting.")
            return

        logger.info(f"Prepared {total_batches} batches with {total_listings} total listings")

        # Initialize processor
        processor = EnhancedImageProcessor(config_file, test_mode=True)
        processor.start_time = time.time()

        # Load batches
        batches = batch_manager.load_batch_run(batch_file)
        if not batches:
            logger.error("Failed to load batch file. Exiting.")
            return

        # Process batches
        current_batch = 0
        total_processed = 0

        for batch in batches:
            current_batch += 1

            if terminate:
                logger.info("Termination signal received, stopping processing")
                break

            # Process each listing in the batch
            batch_processed = processor.process_batch_from_listings(batch, current_batch, total_batches)
            total_processed += batch_processed

            logger.info(
                f"Processed batch {current_batch}: {batch_processed} entries, total: {total_processed}/{total_listings}")

            # Pause briefly to allow system resources to recover
            time.sleep(0.1)

        # Run deduplication if requested
        if include_dedup and not terminate:
            logger.info("All batches processed, starting deduplication phase")
            processor.process_all_duplicates()

        # Log final statistics
        processor.end_time = time.time()
        logger.info(f"Processing completed. Processed {total_processed} entries in {current_batch} batches")
        processor.log_performance_metrics()

    except Exception as e:
        logger.error(f"Prepare and process failed: {str(e)}")
    finally:
        if 'processor' in locals():
            processor.close()
        if 'db_handler' in locals():
            db_handler.close()


def run_benchmark(config_file, samples=10):
    """Run a benchmark test to measure hash comparison performance."""
    try:
        logger.info(f"Running benchmark with {samples} samples")

        # Initialize processor
        processor = EnhancedImageProcessor(config_file)

        # Run the benchmark
        avg_time = processor.run_benchmark(samples)

        logger.info(f"Benchmark complete. Average processing time: {avg_time:.3f}s per image")

    except Exception as e:
        logger.error(f"Benchmark error: {str(e)}")
    finally:
        if 'processor' in locals():
            processor.close()


def run_deduplication_only(config_file):
    """Run only the deduplication phase."""
    try:
        logger.info("Starting deduplication-only mode")

        # Initialize processor
        processor = EnhancedImageProcessor(config_file)

        # Reset duplicate processing flags to ensure all images are processed
        db = DatabaseHandler(processor.config.get_db_config())
        db.reset_duplicate_processing_flags()
        db.close()

        # Run the deduplication phase
        processor.process_all_duplicates()

        logger.info("Deduplication completed")
        processor.log_performance_metrics()

    except Exception as e:
        logger.error(f"Deduplication failed: {str(e)}")
    finally:
        if 'processor' in locals():
            processor.close()


def run_simple_batch_processing(config_file, total_entries, batch_size, include_dedup=False):
    """
    Run a simplified batch processing approach that:
    1. Gets unprocessed images directly from DB in batches
    2. Processes each batch using optimized logic
    3. Moves to next batch
    4. Optionally runs deduplication after all batches complete

    Args:
        config_file: Path to configuration file
        total_entries: Total number of entries to process
        batch_size: Size of each batch
        include_dedup: Whether to run deduplication after processing
    """
    try:
        setup_signal_handlers()

        # Prepare database
        prepare_database()

        logger.info(
            f"Starting simplified batch processing with {total_entries} total entries in batches of {batch_size}")

        # Initialize processor
        processor = EnhancedImageProcessor(config_file)
        processor.start_time = time.time()

        # Initialize batch tracking
        processed_total = 0
        current_batch = 0

        # Process batches until we reach total_entries or run out of data
        while processed_total < total_entries:
            current_batch += 1

            # Check for termination signal
            if terminate:
                logger.info("Termination signal received, stopping processing")
                break

            # Calculate remaining entries
            remaining = total_entries - processed_total
            current_batch_size = min(batch_size, remaining)

            logger.info(f"Processing batch {current_batch} with size {current_batch_size}")

            # Process a single batch using the optimized method
            processed = processor.process_batch(current_batch_size)

            if processed == 0:
                logger.info("No more entries to process, database exhausted")
                break

            processed_total += processed
            logger.info(
                f"Batch {current_batch} complete: processed {processed} entries, total: {processed_total}/{total_entries}")

            # Break if we've reached our target
            if processed_total >= total_entries:
                break

        # Run deduplication if requested
        if include_dedup and not terminate:
            logger.info(
                f"All batches processed ({processed_total} entries in {current_batch} batches), starting deduplication phase")
            processor.process_all_duplicates()

        # Log final statistics
        processor.end_time = time.time()
        logger.info(f"Processing completed. Processed {processed_total} entries in {current_batch} batches")
        processor.log_performance_metrics()

    except Exception as e:
        logger.error(f"Batch processing failed: {str(e)}")
    finally:
        if 'processor' in locals():
            processor.close()


async def run_batched_processing_async(config_file, batch_size=100, limit=None):
    """
    Run the batched processing workflow with async parallelism:
    1. Load all entries from table to Redis
    2. Process in batches with parallelism:
       - Download images in parallel
       - Upload to FTP in parallel
       - Generate hashes and store in Redis
    3. Run deduplication
    4. Update image_links table

    Args:
        config_file: Path to configuration file
        batch_size: Size of each batch
        limit: Maximum number of entries to process (default: all)
    """
    try:
        setup_signal_handlers()

        # Initialize components
        config = Config(config_file)
        db_handler = DatabaseHandler(config.get_db_config())
        processor = EnhancedImageProcessor(config_file)

        # Initialize Redis cache
        redis_config = config.config.get("redis", {
            "host": "localhost",
            "port": 6379,
            "db": 0,
            "prefix": "imgproc:"
        })

        from redis_batch import RedisBatchProcessor
        redis_batch = RedisBatchProcessor(redis_config)

        # Step 1: Load all entries from table to Redis (with limit)
        logger.info("===== STEP 1: LOADING ENTRIES =====")
        redis_batch.flush_cache()  # Start fresh

        # Get all entries
        entries = db_handler.get_all_listings_for_processing()

        # Apply limit if specified
        if limit is not None and limit < len(entries):
            logger.info(f"Limiting processing to {limit} entries (out of {len(entries)} available)")
            entries = entries[:limit]

        # Load entries to Redis
        entry_count = redis_batch.load_entries(entries)

        if entry_count == 0:
            logger.info("No entries to process. Exiting.")
            return

        # Step 2: Process images in batches with parallelism
        logger.info("===== STEP 2: BATCH PROCESSING =====")
        total_processed = 0
        total_skipped = 0
        batch_num = 0

        while True:
            # Check for termination signal
            if terminate:
                logger.info("Termination signal received, stopping processing")
                break

            # Get next batch
            batch = redis_batch.get_batch(batch_size)
            if not batch:
                logger.info("No more entries to process")
                break

            batch_num += 1
            batch_start = time.time()
            batch_processed = 0
            batch_skipped = 0

            logger.info(f"Processing batch {batch_num} with {len(batch)} entries")

            # Define the async function to process one entry
            async def process_entry(internal_id):
                nonlocal batch_processed, batch_skipped

                # Get URLs for this entry
                entry_urls = redis_batch.get_entry_urls(internal_id)

                # Skip if no URLs
                if not entry_urls:
                    logger.warning(f"No URLs found for entry {internal_id}")
                    redis_batch.mark_entry_processed(internal_id)
                    return 0, 0  # processed, skipped

                entry_processed = 0
                entry_skipped = 0

                # Process all URLs in this entry in parallel
                url_tasks = []

                # Prepare tasks for each URL
                for source, urls in entry_urls.items():
                    for idx, url in enumerate(urls):
                        # Skip if hash already exists
                        if redis_batch.check_hash_exists(internal_id, source, idx):
                            entry_skipped += 1
                            continue

                        # Add task to process this URL
                        url_tasks.append(process_url(internal_id, source, idx, url))

                # Run all URL tasks in parallel
                if url_tasks:
                    results = await asyncio.gather(*url_tasks, return_exceptions=True)

                    # Count processed URLs
                    for result in results:
                        if isinstance(result, Exception):
                            logger.error(f"Error processing URL for {internal_id}: {str(result)}")
                        elif result is True:
                            entry_processed += 1

                # Mark entry as processed
                redis_batch.mark_entry_processed(internal_id)

                # Update batch counters
                batch_processed += entry_processed
                batch_skipped += entry_skipped

                return entry_processed, entry_skipped

            # Define the async function to process one URL
            async def process_url(internal_id, source, idx, url):
                try:
                    # Download image asynchronously
                    image_data = await processor.download_image_async(url, internal_id, source)
                    if not image_data:
                        return False

                    # Create destination path
                    root_path = config.get_storage_config().get("root_path", "/ftp_storage/images/")
                    filename = f"{internal_id}_{source}_{idx}.jpg"
                    destination = os.path.join(root_path, filename)

                    # Process image and get hash
                    image_copy = BytesIO(image_data.getvalue())
                    hash_str, _, _, _ = processor.analyzer.process_image(image_copy)

                    # Upload to storage
                    # Note: FTP upload is synchronous, but we can't easily make it async
                    if processor.storage.upload_image(image_data, destination):
                        # Store hash in Redis
                        redis_batch.store_hash(internal_id, source, idx, url, hash_str)
                        return True
                    else:
                        logger.error(f"Failed to upload image to storage: {destination}")
                        return False

                except Exception as e:
                    logger.error(f"Error processing URL {url}: {str(e)}")
                    return False
                finally:
                    # Clean up image data
                    if 'image_data' in locals() and image_data:
                        image_data.close()
                        del image_data

            # Create an async semaphore to limit concurrency
            # Use max_concurrent_processes to limit overall parallelism
            semaphore = asyncio.Semaphore(processor.max_concurrent_processes)

            # Define an async function to process an entry with semaphore
            async def process_entry_with_semaphore(internal_id):
                async with semaphore:
                    return await process_entry(internal_id)

            # Process all entries in the batch in parallel
            entry_tasks = [process_entry_with_semaphore(internal_id) for internal_id in batch]
            entry_results = await asyncio.gather(*entry_tasks, return_exceptions=True)

            # Count processed and skipped from results
            for result in entry_results:
                if isinstance(result, Exception):
                    logger.error(f"Error processing entry: {str(result)}")
                elif isinstance(result, tuple) and len(result) == 2:
                    processed, skipped = result
                    total_processed += processed
                    total_skipped += skipped

            # Calculate batch metrics
            batch_duration = time.time() - batch_start
            remaining_entries = redis_batch.get_remaining_entries()
            est_remaining_time = (batch_duration / len(batch)) * remaining_entries if len(batch) > 0 else 0

            logger.info(f"Batch {batch_num} completed: processed {batch_processed} images, skipped {batch_skipped}")
            logger.info(
                f"Batch duration: {batch_duration:.2f}s, rate: {batch_processed / max(1, batch_duration):.2f} img/s")
            logger.info(f"Progress: {redis_batch.get_processed_entries()}/{redis_batch.get_total_entries()} entries")
            logger.info(f"Estimated remaining time: {est_remaining_time / 60:.1f} minutes")

            # Force garbage collection
            import gc
            gc.collect()

        logger.info(f"Processing complete: {total_processed} images processed, {total_skipped} skipped")

        # Step 3: Run deduplication
        logger.info("===== STEP 3: DEDUPLICATION =====")
        duplicates = redis_batch.run_deduplication()
        logger.info(f"Found duplicates for {len(duplicates)} entries")

        # Step 4: Update database
        logger.info("===== STEP 4: DATABASE UPDATE =====")

        # Prepare updates
        hash_updates = {}
        duplicate_updates = {}

        # Get entries with hashes
        processed_entries = redis_batch.r.smembers(redis_batch.key("processed_entries"))
        logger.info(f"Preparing to update {len(processed_entries)} entries in database")

        for internal_id in processed_entries:
            # Get hashes for this entry
            entry_hashes = redis_batch.get_entry_hashes(internal_id)
            if entry_hashes:
                hash_updates[internal_id] = entry_hashes

            # Get duplicates for this entry
            entry_duplicates = duplicates.get(internal_id, None)
            if entry_duplicates:
                duplicate_updates[internal_id] = entry_duplicates

        # Update database
        updated = db_handler.batch_update_listings(hash_updates, duplicate_updates)
        logger.info(f"Updated {updated} entries in database")

        logger.info("===== PROCESSING COMPLETE =====")

    except Exception as e:
        logger.error(f"Batched processing failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        if 'processor' in locals():
            processor.close()
        if 'db_handler' in locals():
            db_handler.close()
        if 'redis_batch' in locals():
            redis_batch.close()


def run_batched_processing(config_file, batch_size=100, limit=None):
    """Run the batched processing workflow with async parallelism."""
    import asyncio

    # Try to import psutil, install if needed
    try:
        import psutil
    except ImportError:
        logger.info("Installing psutil package for system resource detection...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "psutil"])
        import psutil

    # Now run the async function
    asyncio.run(run_batched_processing_async(config_file, batch_size, limit))


# def run_batched_processing(config_file, batch_size=100, limit=None):
#     """
#     Run the batched processing workflow:
#     1. Load all entries from table to Redis
#     2. Process in batches:
#        - Download the image
#        - Upload to FTP (local in test mode)
#        - Generate hashes and store in Redis
#     3. Run deduplication
#     4. Update image_links table
#
#     Args:
#         config_file: Path to configuration file
#         batch_size: Size of each batch
#         limit: Maximum number of entries to process (default: all)
#     """
#     try:
#         setup_signal_handlers()
#
#         # Initialize components
#         config = Config(config_file)
#         db_handler = DatabaseHandler(config.get_db_config())
#         processor = EnhancedImageProcessor(config_file)
#
#         # Initialize Redis cache
#         redis_config = config.config.get("redis", {
#             "host": "localhost",
#             "port": 6379,
#             "db": 0,
#             "prefix": "imgproc:"
#         })
#
#         from redis_batch import RedisBatchProcessor
#         redis_batch = RedisBatchProcessor(redis_config)
#
#         # Step 1: Load all entries from table to Redis (with limit)
#         logger.info("===== STEP 1: LOADING ENTRIES =====")
#         redis_batch.flush_cache()  # Start fresh
#
#         # Get all entries
#         entries = db_handler.get_all_listings_for_processing()
#
#         # Apply limit if specified
#         if limit is not None and limit < len(entries):
#             logger.info(f"Limiting processing to {limit} entries (out of {len(entries)} available)")
#             entries = entries[:limit]
#
#         # Load entries to Redis
#         entry_count = redis_batch.load_entries(entries)
#
#         if entry_count == 0:
#             logger.info("No entries to process. Exiting.")
#             return
#
#         # Step 2: Process images in batches
#         logger.info("===== STEP 2: BATCH PROCESSING =====")
#         total_processed = 0
#         total_skipped = 0
#         batch_num = 0
#
#         while True:
#             # Check for termination signal
#             if terminate:
#                 logger.info("Termination signal received, stopping processing")
#                 break
#
#             # Get next batch
#             batch = redis_batch.get_batch(batch_size)
#             if not batch:
#                 logger.info("No more entries to process")
#                 break
#
#             batch_num += 1
#             batch_start = time.time()
#             batch_processed = 0
#             batch_skipped = 0
#
#             logger.info(f"Processing batch {batch_num} with {len(batch)} entries")
#
#             # Process each entry in the batch
#             for internal_id in batch:
#                 # Get URLs for this entry
#                 entry_urls = redis_batch.get_entry_urls(internal_id)
#
#                 # Skip if no URLs
#                 if not entry_urls:
#                     logger.warning(f"No URLs found for entry {internal_id}")
#                     redis_batch.mark_entry_processed(internal_id)
#                     continue
#
#                 entry_processed = 0
#                 entry_skipped = 0
#
#                 # Process each source and URL
#                 for source, urls in entry_urls.items():
#                     for idx, url in enumerate(urls):
#                         # Skip if hash already exists
#                         if redis_batch.check_hash_exists(internal_id, source, idx):
#                             logger.debug(f"Skipping already processed URL: {url}")
#                             entry_skipped += 1
#                             batch_skipped += 1
#                             total_skipped += 1
#                             continue
#
#                         try:
#                             # Download image
#                             image_data = processor.download_image(url, internal_id, source)
#                             if not image_data:
#                                 logger.warning(f"Failed to download image: {url}")
#                                 continue
#
#                             # Create destination path
#                             root_path = config.get_storage_config().get("root_path", "/ftp_storage/images/")
#                             filename = f"{internal_id}_{source}_{idx}.jpg"
#                             destination = os.path.join(root_path, filename)
#
#                             # Process image and get hash
#                             image_copy = BytesIO(image_data.getvalue())
#                             hash_str, _, _, _ = processor.analyzer.process_image(image_copy)
#
#                             # Upload to storage
#                             if processor.storage.upload_image(image_data, destination):
#                                 # Store hash in Redis
#                                 redis_batch.store_hash(internal_id, source, idx, url, hash_str)
#
#                                 entry_processed += 1
#                                 batch_processed += 1
#                                 total_processed += 1
#                             else:
#                                 logger.error(f"Failed to upload image to storage: {destination}")
#
#                             # Clean up image data
#                             image_data.close()
#                             del image_data
#                         except Exception as e:
#                             logger.error(f"Error processing URL {url}: {str(e)}")
#
#                 # Mark entry as processed
#                 redis_batch.mark_entry_processed(internal_id)
#                 logger.debug(f"Processed {entry_processed} images, skipped {entry_skipped} for entry {internal_id}")
#
#             # Calculate batch metrics
#             batch_duration = time.time() - batch_start
#             remaining_entries = redis_batch.get_remaining_entries()
#             est_remaining_time = (batch_duration / len(batch)) * remaining_entries if len(batch) > 0 else 0
#
#             logger.info(f"Batch {batch_num} completed: processed {batch_processed} images, skipped {batch_skipped}")
#             logger.info(
#                 f"Batch duration: {batch_duration:.2f}s, rate: {batch_processed / max(1, batch_duration):.2f} img/s")
#             logger.info(f"Progress: {redis_batch.get_processed_entries()}/{redis_batch.get_total_entries()} entries")
#             logger.info(f"Estimated remaining time: {est_remaining_time / 60:.1f} minutes")
#
#             # Force garbage collection
#             import gc
#             gc.collect()
#
#         logger.info(f"Processing complete: {total_processed} images processed, {total_skipped} skipped")
#
#         # Step 3: Run deduplication
#         logger.info("===== STEP 3: DEDUPLICATION =====")
#         duplicates = redis_batch.run_deduplication()
#         logger.info(f"Found duplicates for {len(duplicates)} entries")
#
#         # Step 4: Update database
#         logger.info("===== STEP 4: DATABASE UPDATE =====")
#
#         # Prepare updates
#         hash_updates = {}
#         duplicate_updates = {}
#
#         # Get entries with hashes
#         processed_entries = redis_batch.r.smembers(redis_batch.key("processed_entries"))
#         logger.info(f"Preparing to update {len(processed_entries)} entries in database")
#
#         for internal_id in processed_entries:
#             # Get hashes for this entry
#             entry_hashes = redis_batch.get_entry_hashes(internal_id)
#             if entry_hashes:
#                 hash_updates[internal_id] = entry_hashes
#
#             # Get duplicates for this entry
#             entry_duplicates = duplicates.get(internal_id, None)
#             if entry_duplicates:
#                 duplicate_updates[internal_id] = entry_duplicates
#
#         # Update database
#         updated = db_handler.batch_update_listings(hash_updates, duplicate_updates)
#         logger.info(f"Updated {updated} entries in database")
#
#         logger.info("===== PROCESSING COMPLETE =====")
#
#     except Exception as e:
#         logger.error(f"Batched processing failed: {str(e)}")
#         import traceback
#         logger.error(traceback.format_exc())
#     finally:
#         if 'processor' in locals():
#             processor.close()
#         if 'db_handler' in locals():
#             db_handler.close()
#         if 'redis_batch' in locals():
#             redis_batch.close()

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Image Processing Service")
    parser.add_argument("--config", default="config.json", help="Path to configuration file")

    # Command to execute
    subparsers = parser.add_subparsers(dest="command", help="Command")

    # Batched processing
    batch_parser = subparsers.add_parser("batch", help="Process in batches with Redis")
    batch_parser.add_argument("--batch-size", type=int, default=100,
                              help="Size of each batch (default: 100)")
    batch_parser.add_argument("--limit", type=int, default=None,
                              help="Limit the number of entries to process (default: all)")

    # Other commands (run, test, etc.)
    run_parser = subparsers.add_parser("run", help="Run the service (traditional method)")
    run_parser.add_argument("--once", action="store_true", help="Run once and exit")

    test_parser = subparsers.add_parser("test", help="Test processing (traditional method)")
    test_parser.add_argument("--entries", type=int, default=None,
                             help="Number of entries to process (default: process one batch)")

    migrate_parser = subparsers.add_parser("migrate", help="Migrate database schema")

    args = parser.parse_args()

    # Default to batch if no command given
    if not args.command:
        args.command = "batch"

    # Process commands
    if args.command == "batch":
        # Use the new Redis-based batched processing
        run_batched_processing(args.config, args.batch_size, args.limit)
    elif args.command == "run":
        # Traditional processing
        setup_signal_handlers()
        run_service(args.config, args.once)
    elif args.command == "test":
        # Traditional test mode
        run_test(args.config, args.entries)
    elif args.command == "migrate":
        # Just update the database schema
        prepare_database()


if __name__ == "__main__":
    t1 = time.time()
    main()
    print(time.time()-t1)