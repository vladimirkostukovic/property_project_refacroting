import os
import sys
import json
import time
import asyncio
import logging
from io import BytesIO
from datetime import datetime
import gc

import aiohttp
import ftplib

from db_handler import DatabaseHandler
from image_analyzer import ImageAnalyzer
from local_storage import LocalStorageHandler
from failed_downloads_tracker import FailedDownloadsTracker

# === LOGGING SETUP ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("image_processor.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("ImageProcessor")

# === CONFIG CLASS ===
class Config:
    def __init__(self, config_path="config.json"):
        log.info(f"Loading config from {config_path}")
        with open(config_path, 'r') as f:
            self.config = json.load(f)
    def get_db_config(self):
        return self.config.get("database", {})
    def get_storage_config(self):
        return self.config.get("storage", {})
    def get_processing_config(self):
        return self.config.get("processing", {})
    def get_analyzer_config(self):
        return self.config.get("analyzer", {})

# === STORAGE HANDLER ===
class StorageHandler:
    def __init__(self, storage_config):
        self.config = storage_config
        self.ftp = None

    def connect(self):
        if not self.ftp:
            log.info("Connecting to FTP server...")
            self.ftp = ftplib.FTP(self.config.get("host"))
            self.ftp.login(
                user=self.config.get("username"),
                passwd=self.config.get("password")
            )
            self.ftp.set_pasv(True)
        return self.ftp

    def upload_image(self, source, destination):
        ftp = self.connect()
        source.seek(0)
        try:
            ftp.storbinary(f'STOR {destination}', source)
            log.info(f"Uploaded to {destination}")
            return True
        except Exception as e:
            log.error(f"FTP upload error: {str(e)}")
            return False

    def close(self):
        if self.ftp:
            try:
                self.ftp.quit()
            except:
                self.ftp.close()
            self.ftp = None
            log.info("FTP connection closed")

# === IMAGE PROCESSOR ===
class EnhancedImageProcessor:
    def __init__(self, config_path="config.json", test_mode=False):
        self.config = Config(config_path)
        self.db = DatabaseHandler(self.config.get_db_config())
        self.processing_config = self.config.get_processing_config()
        self.analyzer = ImageAnalyzer(self.config.get_analyzer_config())

        self.test_mode = test_mode or self.processing_config.get("test_mode", False)
        self.storage = LocalStorageHandler(self.config.get_storage_config()) if self.test_mode \
            else StorageHandler(self.config.get_storage_config())

        logs_dir = self.processing_config.get("logs_dir", "./logs")
        self.failed_downloads_tracker = FailedDownloadsTracker(logs_dir)

        self.max_concurrent_downloads = self.processing_config.get("max_concurrent_downloads", 10)
        self.max_concurrent_uploads = self.processing_config.get("max_concurrent_uploads", 5)
        self.max_concurrent_processes = self.processing_config.get("max_concurrent_processes", 10)
        self.set_max_parallelism()

        self.start_time = None
        self.end_time = None
        self.processed_count = 0
        self.duplicate_count = 0
        self.failed_downloads = 0
        self.processed_listings = set()
        self.processed_urls = set()

    def extract_image_urls(self, listing):
        internal_id = listing['internal_id']
        for column in ['hyper_reality', 'sreality_image_urls', 'bezrealitky_image_urls', 'idnes']:
            data = listing[column]
            urls = []
            if data:
                if isinstance(data, str):
                    try:
                        urls = json.loads(data)
                    except json.JSONDecodeError:
                        urls = [data]
                elif isinstance(data, list):
                    urls = data[0] if urls and isinstance(urls[0], list) else data
            unique_urls = []
            seen = set()
            for url in urls:
                if url and url not in self.processed_urls and url not in seen:
                    seen.add(url)
                    unique_urls.append(url)
            if unique_urls:
                return unique_urls, column
        return [], None

    async def download_image_async(self, url, internal_id=None, source_column=None):
        if url in self.processed_urls:
            return None
        self.processed_urls.add(url)
        timeout = aiohttp.ClientTimeout(total=self.processing_config.get("timeout", 30))
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        return BytesIO(await response.read())
                    else:
                        if internal_id:
                            self.failed_downloads_tracker.record_failure(
                                internal_id=str(internal_id),
                                url=url,
                                source_column=source_column or "unknown",
                                error_message=f"HTTP {response.status}"
                            )
                            self.failed_downloads += 1
                        return None
        except Exception as e:
            if internal_id:
                self.failed_downloads_tracker.record_failure(
                    internal_id=str(internal_id),
                    url=url,
                    source_column=source_column or "unknown",
                    error_message=str(e)
                )
                self.failed_downloads += 1
            return None

    def download_image(self, url, internal_id=None, source_column=None):
        return asyncio.run(self.download_image_async(url, internal_id, source_column))

    async def process_image_async(self, internal_id, url, idx, source_column):
        image_data = await self.download_image_async(url, internal_id, source_column)
        if not image_data:
            return None
        root_path = self.config.get_storage_config().get("root_path", "/ftp_storage/images/")
        filename = f"{internal_id}_{source_column}_{idx}.jpg"
        destination = os.path.join(root_path, filename)
        try:
            hash_str, processed_image, hash_binary, hash_bucket = self.analyzer.process_image(BytesIO(image_data.getvalue()))
            if not self.storage.upload_image(image_data, destination):
                return None
            self.processed_count += 1
            return destination
        except Exception as e:
            log.error(f"Processing error: {str(e)}")
            return None

    async def process_listing_async(self, listing):
        internal_id = listing['internal_id']
        urls, source_column = self.extract_image_urls(listing)
        if not urls:
            return 0
        semaphore = asyncio.Semaphore(self.max_concurrent_downloads)
        async def process_with_semaphore(url, idx):
            async with semaphore:
                return await self.process_image_async(internal_id, url, idx, source_column)
        tasks = [process_with_semaphore(url, idx) for idx, url in enumerate(urls)]
        local_paths = await asyncio.gather(*tasks)
        local_paths = [p for p in local_paths if p]
        if local_paths:
            self.db.update_listing(internal_id, local_paths)
        gc.collect()
        return len(local_paths)

    def process_listing(self, listing):
        return asyncio.run(self.process_listing_async(listing))

    async def process_batch_async(self, batch_size=100):
        listings = self.db.get_all_unprocessed_listings(batch_size)
        if not listings:
            return 0
        id_list = [str(listing['internal_id']) for listing in listings]
        filtered_listings = [l for l in listings if l['internal_id'] not in self.processed_listings]
        for l in filtered_listings:
            self.processed_listings.add(l['internal_id'])
        if not filtered_listings:
            return 0
        semaphore = asyncio.Semaphore(self.max_concurrent_processes)
        async def process_listing_with_semaphore(listing):
            async with semaphore:
                return await self.process_listing_async(listing)
        tasks = [process_listing_with_semaphore(listing) for listing in filtered_listings]
        results = await asyncio.gather(*tasks)
        gc.collect()
        return len(filtered_listings)

    def process_batch(self, batch_size=None):
        if batch_size is None:
            batch_size = self.processing_config.get("batch_size", 100)
        return asyncio.run(self.process_batch_async(batch_size))

    def process_all_duplicates(self):
        log.info("Starting duplicate detection")
        total_images = self.db.get_total_image_count()
        batch_size = 1000
        for offset in range(0, total_images, batch_size):
            images = self.db.get_image_batch_for_deduplication(batch_size, offset)
            for img in images:
                duplicates = self.analyzer.find_duplicates(self.db, img['id'], img['image_hash'])
                for dup_id, similarity in duplicates:
                    self.db.record_duplicate(
                        original_id=img['id'],
                        duplicate_id=dup_id,
                        similarity_score=similarity
                    )
            gc.collect()
        log.info("Duplicate detection complete")

    def run(self, max_batches=None):
        self.start_time = time.time()
        batch_count = 0
        processed_count = 0
        batch_size = self.processing_config.get("batch_size", 100)
        while max_batches is None or batch_count < max_batches:
            processed = self.process_batch(batch_size)
            processed_count += processed
            batch_count += 1
            if processed < batch_size:
                break
        self.end_time = time.time()
        self.log_performance_metrics()
        log.info(f"Processed {processed_count} listings in {batch_count} batches.")

    def run_process_and_deduplicate(self, max_batches=None):
        self.run(max_batches)
        log.info("Starting deduplication")
        self.process_all_duplicates()
        self.end_time = time.time()
        self.log_performance_metrics()
        log.info("Process and deduplication complete")

    def run_benchmark(self, sample_count=10):
        avg_time = self.analyzer.benchmark_find_duplicates(self.db, sample_count)
        log.info(f"Benchmark avg time: {avg_time:.3f}s per image")
        return avg_time

    def log_performance_metrics(self):
        if not self.start_time or not self.end_time:
            return
        total_duration = self.end_time - self.start_time
        analyzer_metrics = self.analyzer.get_performance_metrics()
        db_duplicate_stats = self.db.get_duplicate_statistics()
        failed_downloads_count = self.failed_downloads_tracker.get_failure_count()
        log.info("=== Performance Metrics ===")
        log.info(f"Total runtime: {total_duration:.2f} seconds")
        log.info(f"Listings processed: {len(self.processed_listings)}")
        log.info(f"Images processed: {self.processed_count}")
        log.info(f"Unique URLs processed: {len(self.processed_urls)}")
        log.info(f"Processing rate: {self.processed_count / max(1, total_duration):.2f} images/second")
        log.info(f"Total image comparisons: {analyzer_metrics['total_comparisons']}")
        log.info(f"Average comparison time: {analyzer_metrics['avg_comparison_time']:.2f} ms")
        log.info(f"Total duplicates found: {self.duplicate_count}")
        log.info(f"Total duplicates in database: {db_duplicate_stats['total_duplicates']}")
        log.info(f"Average similarity score: {db_duplicate_stats['average_similarity']:.4f}")
        log.info(f"Failed downloads: {failed_downloads_count}")
        log.info(f"Failed downloads log: {self.failed_downloads_tracker.csv_file}")
        log.info("===========================")

    def set_max_parallelism(self):
        import psutil
        cpu_count = os.cpu_count() or 4
        total_memory_gb = psutil.virtual_memory().total / (1024 * 1024 * 1024)
        optimal_cpu_parallelism = max(4, int(cpu_count * 0.75))
        memory_based_parallelism = max(4, int(total_memory_gb * 0.8 * 10))
        network_based_parallelism = 30
        self.max_concurrent_processes = min(optimal_cpu_parallelism, memory_based_parallelism, network_based_parallelism)
        self.max_concurrent_downloads = self.max_concurrent_processes
        self.max_concurrent_uploads = max(4, self.max_concurrent_processes // 2)
        log.info(f"Set max parallelism: processes={self.max_concurrent_processes}, downloads={self.max_concurrent_downloads}, uploads={self.max_concurrent_uploads}")
        return self.max_concurrent_processes

    def close(self):
        self.db.close()
        self.storage.close()

# === ENTRY POINT ===
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Process and upload images from database to storage.")
    parser.add_argument("--config", default="config.json", help="Path to configuration file")
    parser.add_argument("--batches", type=int, help="Maximum number of batches to process")
    parser.add_argument("--benchmark", action="store_true", help="Run hash comparison benchmark")
    parser.add_argument("--samples", type=int, default=10, help="Number of samples for benchmark")
    parser.add_argument("--dedup-only", action="store_true", help="Run only the deduplication phase")
    parser.add_argument("--complete", action="store_true", help="Run complete process with deduplication")
    args = parser.parse_args()

    processor = EnhancedImageProcessor(args.config)
    if args.benchmark:
        processor.run_benchmark(args.samples)
    elif args.dedup_only:
        processor.process_all_duplicates()
    elif args.complete:
        processor.run_process_and_deduplicate(args.batches)
    else:
        processor.run(args.batches)

if __name__ == "__main__":
    main()