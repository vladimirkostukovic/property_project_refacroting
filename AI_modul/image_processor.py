import os
import json
import time
import urllib.request
import logging
import asyncio
import aiohttp
import concurrent.futures
from datetime import datetime
from io import BytesIO
import ftplib  # Using standard library ftplib instead of pysftp
import time
import gc

from db_handler import DatabaseHandler
from image_analyzer import ImageAnalyzer
from local_storage import LocalStorageHandler
from failed_downloads_tracker import FailedDownloadsTracker

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


class Config:
    """Load and provide access to configuration settings."""

    def __init__(self, config_path="config.json"):
        logger.info(f"Loading configuration from {config_path}")
        try:
            with open(config_path, 'r') as f:
                self.config = json.load(f)
            logger.info("Configuration loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load configuration: {str(e)}")
            raise

    def get_db_config(self):
        return self.config.get("database", {})

    def get_storage_config(self):
        return self.config.get("storage", {})

    def get_processing_config(self):
        return self.config.get("processing", {})

    def get_analyzer_config(self):
        return self.config.get("analyzer", {})


class StorageHandler:
    """Handle remote storage operations via FTP."""

    def __init__(self, storage_config):
        self.config = storage_config
        self.ftp = None

    def connect(self):
        """Establish an FTP connection."""
        if self.ftp is None:
            logger.info("Connecting to FTP server...")
            try:
                self.ftp = ftplib.FTP(self.config.get("host"))
                self.ftp.login(
                    user=self.config.get("username"),
                    passwd=self.config.get("password")
                )
                self.ftp.set_pasv(True)  # Use passive mode for better compatibility
                logger.info("FTP connection established")
            except Exception as e:
                logger.error(f"FTP connection failed: {str(e)}")
                raise
        return self.ftp

    def ensure_directory(self, path):
        """Ensure the directory exists on the remote server."""
        ftp = self.connect()
        try:
            # Split the path into parts
            parts = path.split('/')
            current_dir = ""

            # Try to navigate through each directory level
            for part in parts:
                if not part:  # Skip empty parts (happens with leading/trailing slashes)
                    continue

                current_dir = current_dir + "/" + part if current_dir else part

                # Try to change to this directory
                try:
                    ftp.cwd(part)
                except ftplib.error_perm:
                    # If it doesn't exist, create it
                    logger.info(f"Creating directory: {part}")
                    ftp.mkd(part)
                    ftp.cwd(part)

            # Go back to root directory
            ftp.cwd("/")

        except Exception as e:
            logger.error(f"Failed to create directory {path}: {str(e)}")
            raise

    def upload_image(self, source, destination):
        """Upload an image to the remote storage."""
        ftp = self.connect()
        try:
            # Ensure the directory exists
            destination_dir = os.path.dirname(destination)
            # if destination_dir:
            #     self.ensure_directory(destination_dir)

            # Extract filename from path
            filename = os.path.basename(destination)

            # Reset file pointer to beginning
            source.seek(0)

            # Upload the file
            ftp.storbinary(f'STOR {destination}', source)
            logger.info(f"Uploaded image to {destination}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload image to {destination}: {str(e)}")
            return False

    def close(self):
        """Close the FTP connection."""
        if self.ftp:
            try:
                self.ftp.quit()
            except:
                self.ftp.close()
            self.ftp = None
            logger.info("FTP connection closed")


class EnhancedImageProcessor:
    """Process images from database, upload to storage, and perform deduplication."""

    def __init__(self, config_path="config.json", test_mode=False):
        self.config = Config(config_path)
        self.db = DatabaseHandler(self.config.get_db_config())

        # Set test mode either from parameter or config
        self.test_mode = test_mode or self.config.get_processing_config().get("test_mode", False)

        # Use appropriate storage handler based on test mode
        if self.test_mode:
            logger.info("Using local storage for test mode")
            self.storage = LocalStorageHandler(self.config.get_storage_config())
        else:
            logger.info("Using FTP storage for production mode")
            self.storage = StorageHandler(self.config.get_storage_config())

        self.processing_config = self.config.get_processing_config()
        self.analyzer = ImageAnalyzer(self.config.get_analyzer_config())

        # Initialize failed downloads tracker
        logs_dir = self.processing_config.get("logs_dir", "./logs")
        self.failed_downloads_tracker = FailedDownloadsTracker(logs_dir)

        # Concurrency controls
        self.max_concurrent_downloads = self.processing_config.get("max_concurrent_downloads", 10)
        self.max_concurrent_uploads = self.processing_config.get("max_concurrent_uploads", 5)
        self.max_concurrent_processes = self.processing_config.get("max_concurrent_processes", 10)
        self.set_max_parallelism()

        # Performance tracking
        self.start_time = None
        self.end_time = None
        self.processed_count = 0
        self.duplicate_count = 0
        self.failed_downloads = 0

        # Keep track of processed listings to avoid reprocessing
        self.processed_listings = set()

        # Global URL cache to prevent processing duplicate URLs across all listings
        self.processed_urls = set()

    def extract_image_urls(self, listing):
        """Extract image URLs from a listing row."""
        internal_id = listing['internal_id']

        # Convert internal_id to string for logging
        internal_id_str = str(internal_id)

        # Check each possible column for image URLs
        for column in ['hyper_reality', 'sreality_image_urls', 'bezrealitky_image_urls', 'idnes']:
            data = listing[column]
            if data:
                # Handle different data formats
                if isinstance(data, str):
                    try:
                        # Try to parse as JSON if it's a string
                        urls = json.loads(data)
                    except json.JSONDecodeError:
                        # If not JSON, treat as a single URL
                        urls = [data]
                elif isinstance(data, list):
                    urls = data
                    # Handle nested lists
                    if urls and isinstance(urls[0], list):
                        urls = urls[0]
                else:
                    urls = []

                # Remove duplicate URLs and already processed ones
                unique_urls = []
                seen = set()
                for url in urls:
                    # Skip empty, None, or already processed URLs
                    if not url or url in self.processed_urls:
                        continue

                    # Skip duplicates within this listing
                    if url not in seen:
                        seen.add(url)
                        unique_urls.append(url)

                if unique_urls:
                    logger.info(f"[ID:{internal_id_str}] Found {len(unique_urls)} unique URLs in {column}")
                    skipped_count = len(urls) - len(unique_urls)
                    if skipped_count > 0:
                        logger.warning(f"[ID:{internal_id_str}] Skipped {skipped_count} duplicate/processed URLs")

                    # Log each URL with the internal ID
                    for i, url in enumerate(unique_urls):
                        logger.debug(f"[ID:{internal_id_str}] URL {i + 1}/{len(unique_urls)}: {url}")

                    return unique_urls, column

        logger.warning(f"[ID:{internal_id_str}] No new unique image URLs found")
        return [], None

    async def download_image_async(self, url, internal_id=None, source_column=None):
        """Download an image from a URL asynchronously. No retries."""
        # Format the id_info string for logging - ensure internal_id is a string
        id_info = f"[ID:{str(internal_id)}]" if internal_id is not None else ""

        # Check if URL has already been processed
        if url in self.processed_urls:
            logger.info(f"{id_info} Skipping already processed URL: {url}")
            return None

        # Add URL to processed set
        self.processed_urls.add(url)

        # Single attempt with reasonable timeout
        timeout = aiohttp.ClientTimeout(total=self.processing_config.get("timeout", 30))

        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        image_data = await response.read()
                        return BytesIO(image_data)
                    else:
                        error_msg = f"Failed to download {id_info} {url}: HTTP {response.status}"
                        logger.warning(error_msg)

                        # Record failure
                        if internal_id is not None:
                            self.failed_downloads_tracker.record_failure(
                                internal_id=str(internal_id),
                                url=url,
                                source_column=source_column or "unknown",
                                error_message=error_msg
                            )
                            self.failed_downloads += 1

                        return None
        except Exception as e:
            error_msg = f"Failed to download {id_info} {url}: {str(e)}"
            logger.warning(error_msg)

            # Record failure
            if internal_id is not None:
                self.failed_downloads_tracker.record_failure(
                    internal_id=str(internal_id),
                    url=url,
                    source_column=source_column or "unknown",
                    error_message=error_msg
                )
                self.failed_downloads += 1

            return None

    def download_image(self, url, internal_id=None, source_column=None):
        """Synchronous wrapper for download_image_async."""
        return asyncio.run(self.download_image_async(url, internal_id, source_column))

    async def process_image_async(self, internal_id, url, idx, source_column):
        """Process a single image asynchronously."""
        image_data = None
        try:
            # Download the image
            image_data = await self.download_image_async(
                url=url,
                internal_id=internal_id,
                source_column=source_column
            )
            if not image_data:
                return None

            # Create the destination path
            root_path = self.config.get_storage_config().get("root_path", "/ftp_storage/images/")
            filename = f"{internal_id}_{source_column}_{idx}.jpg"
            destination = os.path.join(root_path, filename)

            # Process the image: create hash and optimize for storage
            try:
                # Make a copy of the image data for processing
                image_copy = BytesIO(image_data.getvalue())

                # Process the image - now returns binary hash and bucket too
                hash_str, processed_image, hash_binary, hash_bucket = self.analyzer.process_image(image_copy)

                # Upload the image to storage first
                if not self.storage.upload_image(image_data, destination):
                    logger.error(f"Failed to upload image {idx} for listing {internal_id}")
                    return None

                # Save processed image to database - WITHOUT duplicate check
                image_id = self.db.save_processed_image(
                    internal_id=internal_id,
                    image_data=processed_image,
                    image_hash=hash_str,
                    hash_binary=hash_binary,
                    hash_bucket=hash_bucket,
                    source_url=url,
                    source_column=source_column
                )

                self.processed_count += 1
                return destination

            except Exception as e:
                logger.error(f"Failed to process image {idx} for deduplication: {str(e)}")
                return None

        except Exception as e:
            logger.error(f"Error processing image {idx} for listing {internal_id}: {str(e)}")
            return None
        finally:
            # Clean up the image data to free memory
            if image_data:
                try:
                    image_data.close()
                    del image_data
                except:
                    pass

    async def process_listing_async(self, listing):
        """Process a single listing asynchronously with concurrent image processing."""
        internal_id = listing['internal_id']
        # Convert internal_id to string for logging
        internal_id_str = str(internal_id)

        logger.info(f"[ID:{internal_id_str}] Processing listing")

        # Extract image URLs
        urls, source_column = self.extract_image_urls(listing)
        if not urls:
            return 0

        # Create semaphore for limiting concurrent downloads
        semaphore = asyncio.Semaphore(self.max_concurrent_downloads)

        async def process_with_semaphore(url, idx):
            """Process a single image with semaphore limiting."""
            async with semaphore:
                return await self.process_image_async(internal_id, url, idx, source_column)

        # Process all images concurrently
        tasks = [process_with_semaphore(url, idx) for idx, url in enumerate(urls)]
        local_paths = await asyncio.gather(*tasks)

        # Filter out None results
        local_paths = [path for path in local_paths if path]

        # Update the database if we uploaded any images
        if local_paths:
            update_success = self.db.update_listing(internal_id, local_paths)
            if update_success:
                logger.info(f"[ID:{internal_id_str}] Successfully processed {len(local_paths)} of {len(urls)} images")
            else:
                logger.error(
                    f"[ID:{internal_id_str}] Database update failed after processing {len(local_paths)} images")
                return 0  # Indicate failure
        else:
            logger.warning(f"[ID:{internal_id_str}] Failed to process any images")

        # Trigger garbage collection to free memory
        gc.collect()

        return len(local_paths)

    def process_listing(self, listing):
        """Synchronous wrapper for process_listing_async."""
        return asyncio.run(self.process_listing_async(listing))

    async def process_batch_async(self, batch_size=100):
        """Process a batch of listings asynchronously."""
        try:
            listings = self.db.get_all_unprocessed_listings(batch_size)

            if not listings:
                logger.info("No unprocessed listings found")
                return 0

            logger.info(f"Processing batch of {len(listings)} listings")

            # Log the internal IDs in the batch - convert to strings first
            id_list = [str(listing['internal_id']) for listing in listings]
            logger.info(f"Batch includes IDs: {', '.join(id_list)}")

            # Filter out listings we've already processed in this session
            filtered_listings = []
            for listing in listings:
                internal_id = listing['internal_id']
                if internal_id not in self.processed_listings:
                    filtered_listings.append(listing)
                    # Mark as processed to avoid duplicates
                    self.processed_listings.add(internal_id)
                else:
                    logger.warning(f"[ID:{internal_id}] Skipping already processed listing")

            if len(filtered_listings) < len(listings):
                logger.warning(f"Filtered out {len(listings) - len(filtered_listings)} duplicate listings")

            if not filtered_listings:
                logger.warning("No new listings to process after filtering")
                return 0

            # Create semaphore for limiting concurrent listing processing
            semaphore = asyncio.Semaphore(self.max_concurrent_processes)

            async def process_listing_with_semaphore(listing):
                """Process a single listing with semaphore limiting."""
                async with semaphore:
                    return await self.process_listing_async(listing)

            # Process all listings concurrently
            tasks = [process_listing_with_semaphore(listing) for listing in filtered_listings]
            results = await asyncio.gather(*tasks)

            # Count total processed
            total_processed = sum(1 for r in results if r)

            # Force garbage collection to free memory
            gc.collect()

            return len(filtered_listings)

        except Exception as e:
            logger.error(f"Batch processing failed: {str(e)}")
            return 0

    def process_batch(self, batch_size=None):
        """Synchronous wrapper for process_batch_async."""
        if batch_size is None:
            batch_size = self.processing_config.get("batch_size", 100)
        return asyncio.run(self.process_batch_async(batch_size))

    def process_batch_from_listings(self, listings, batch_num=None, total_batches=None):
        """
        Process a batch of listings directly.

        Args:
            listings: List of listing dictionaries
            batch_num: Current batch number (optional)
            total_batches: Total number of batches (optional)

        Returns:
            int: Number of listings processed
        """
        try:
            if not listings:
                logger.info("No listings to process in this batch")
                return 0

            # Reset batch-specific counters
            batch_start_time = time.time()
            batch_processed = 0
            batch_failed = 0
            initial_failed_downloads = self.failed_downloads_tracker.get_failure_count()

            # Log the batch information
            batch_info = ""
            if batch_num is not None and total_batches is not None:
                batch_info = f" {batch_num}/{total_batches}"
            logger.info(f"Processing batch{batch_info} with {len(listings)} listings")

            # Log the internal IDs in the batch
            id_list = [str(listing['internal_id']) for listing in listings]
            logger.info(f"Batch includes IDs: {', '.join(id_list)}")

            # Filter out listings we've already processed in this session
            filtered_listings = []
            for listing in listings:
                internal_id = listing['internal_id']
                if internal_id not in self.processed_listings:
                    # Convert back to a RowMapping if needed for compatibility
                    if isinstance(listing, dict) and not hasattr(listing, 'keys'):
                        # This is a regular dict, might need conversion depending on code expectations
                        pass
                    filtered_listings.append(listing)
                    # Mark as processed to avoid duplicates
                    self.processed_listings.add(internal_id)
                else:
                    logger.warning(f"[ID:{str(internal_id)}] Skipping already processed listing")

            if len(filtered_listings) < len(listings):
                logger.warning(f"Filtered out {len(listings) - len(filtered_listings)} duplicate listings")

            if not filtered_listings:
                logger.warning("No new listings to process after filtering")
                return 0

            # Process each listing sequentially with proper error handling
            for listing in filtered_listings:
                try:
                    result = asyncio.run(self.process_listing_async(listing))
                    if result > 0:
                        batch_processed += 1
                    else:
                        batch_failed += 1
                except Exception as e:
                    batch_failed += 1
                    logger.error(f"Failed to process listing [ID:{listing['internal_id']}]: {str(e)}")

            # Calculate batch-specific metrics
            batch_duration = time.time() - batch_start_time
            batch_failed_downloads = self.failed_downloads_tracker.get_failure_count() - initial_failed_downloads

            # Print batch summary
            logger.info(f"===== Batch{batch_info} Summary =====")
            logger.info(f"Listings processed successfully: {batch_processed}/{len(listings)}")
            logger.info(f"Listings failed: {batch_failed}")
            logger.info(f"Batch duration: {batch_duration:.2f} seconds")
            logger.info(f"Processing rate: {batch_processed / max(1, batch_duration):.2f} listings/second")
            logger.info(f"Failed downloads in this batch: {batch_failed_downloads}")
            logger.info("=======================================")

            # Force garbage collection to free memory
            gc.collect()

            return batch_processed

        except Exception as e:
            logger.error(f"Batch processing failed: {str(e)}")
            return 0

    def process_all_duplicates(self):
        """Process all duplicates as a separate phase."""
        logger.info("Starting batch duplicate detection")
        start_time = time.time()

        # Get total image count
        total_images = self.db.get_total_image_count()
        logger.info(f"Found {total_images} images to check for duplicates")

        # Process in batches to avoid memory issues
        batch_size = 1000
        total_duplicates = 0

        for offset in range(0, total_images, batch_size):
            batch_start = time.time()

            # Get batch of images
            images = self.db.get_image_batch_for_deduplication(batch_size, offset)

            logger.info(
                f"Processing duplicate batch {offset // batch_size + 1}/{(total_images + batch_size - 1) // batch_size}, size: {len(images)}")

            # Find duplicates for each image
            batch_duplicates = 0
            for img in images:
                duplicates = self.analyzer.find_duplicates(
                    db_handler=self.db,
                    new_image_id=img['id'],
                    new_hash=img['image_hash']
                )

                # Record duplicates
                for dup_id, similarity in duplicates:
                    self.db.record_duplicate(
                        original_id=img['id'],
                        duplicate_id=dup_id,
                        similarity_score=similarity
                    )
                    batch_duplicates += 1
                    total_duplicates += 1

            batch_time = time.time() - batch_start

            logger.info(f"Batch completed: found {batch_duplicates} duplicates in {batch_time:.2f} seconds")

            # Memory cleanup after each batch
            gc.collect()

        elapsed = time.time() - start_time
        logger.info(f"Duplicate detection completed: found {total_duplicates} duplicates in {elapsed:.2f} seconds")
        self.duplicate_count = total_duplicates
        return total_duplicates

    def run(self, max_batches=None):
        """Run the image processing job."""
        try:
            self.start_time = time.time()
            batch_count = 0
            processed_count = 0

            logger.info("Starting image processing job")

            batch_size = self.processing_config.get("batch_size", 100)

            while max_batches is None or batch_count < max_batches:
                processed = self.process_batch(batch_size)
                processed_count += processed
                batch_count += 1

                logger.info(f"Processed batch {batch_count}, total processed: {processed_count}")

                # If we processed fewer than the batch size, we're done
                if processed < batch_size:
                    break

            self.end_time = time.time()

            # Log performance metrics
            self.log_performance_metrics()

            logger.info(
                f"Image processing job completed. Processed {processed_count} listings in {batch_count} batches.")

        except Exception as e:
            logger.error(f"Image processing job failed: {str(e)}")
        finally:
            self.close()

    def run_process_and_deduplicate(self, max_batches=None):
        """Run the complete job: process images and then find duplicates."""
        try:
            # First run the processing part
            self.run(max_batches)

            # Then run the deduplication part
            logger.info("Processing phase complete, starting deduplication phase")
            self.process_all_duplicates()

            # Update end time and log final metrics
            self.end_time = time.time()
            self.log_performance_metrics()

            logger.info("Complete processing and deduplication job finished")

        except Exception as e:
            logger.error(f"Processing and deduplication job failed: {str(e)}")
        finally:
            self.close()

    def run_benchmark(self, sample_count=10):
        """Run a benchmark to measure hash comparison performance."""
        logger.info(f"Running hash comparison benchmark with {sample_count} samples")
        avg_time = self.analyzer.benchmark_find_duplicates(self.db, sample_count)
        logger.info(f"Benchmark complete. Average time: {avg_time:.3f}s per image")
        return avg_time

    def log_performance_metrics(self):
        """Log performance metrics."""
        if not self.start_time or not self.end_time:
            return

        total_duration = self.end_time - self.start_time
        analyzer_metrics = self.analyzer.get_performance_metrics()
        db_duplicate_stats = self.db.get_duplicate_statistics()
        failed_downloads_count = self.failed_downloads_tracker.get_failure_count()

        logger.info("=== Performance Metrics ===")
        logger.info(f"Total runtime: {total_duration:.2f} seconds")
        logger.info(f"Listings processed: {len(self.processed_listings)}")
        logger.info(f"Images processed: {self.processed_count}")
        logger.info(f"Unique URLs processed: {len(self.processed_urls)}")
        logger.info(f"Processing rate: {self.processed_count / max(1, total_duration):.2f} images/second")
        logger.info(f"Total image comparisons: {analyzer_metrics['total_comparisons']}")
        logger.info(f"Average comparison time: {analyzer_metrics['avg_comparison_time']:.2f} ms")
        logger.info(f"Total duplicates found: {self.duplicate_count}")
        logger.info(f"Total duplicates in database: {db_duplicate_stats['total_duplicates']}")
        logger.info(f"Average similarity score: {db_duplicate_stats['average_similarity']:.4f}")
        logger.info(f"Failed downloads: {failed_downloads_count}")
        logger.info(f"Failed downloads log: {self.failed_downloads_tracker.csv_file}")
        logger.info("===========================")

    def set_max_parallelism(self):
        """
        Set the maximum parallelism for image processing.
        This determines the number of concurrent downloads, uploads, and processes.
        Uses information from system configuration to set optimal values.
        """
        import os
        import psutil

        # Get system resources
        cpu_count = os.cpu_count() or 4
        total_memory_gb = psutil.virtual_memory().total / (1024 * 1024 * 1024)

        # Calculate optimal parallelism
        # Use 75% of available CPU cores to avoid overloading the system
        optimal_cpu_parallelism = max(4, int(cpu_count * 0.75))

        # Memory considerations - each process might use around 100MB
        memory_based_parallelism = max(4, int(total_memory_gb * 0.8 * 10))  # 10 processes per GB at 80% utilization

        # Network constraints - this is more of a guess, adjust based on your network
        network_based_parallelism = 30  # Assuming a decent internet connection

        # Set the parallelism values - take the minimum of the calculated values
        self.max_concurrent_processes = min(optimal_cpu_parallelism, memory_based_parallelism,
                                            network_based_parallelism)
        self.max_concurrent_downloads = self.max_concurrent_processes
        self.max_concurrent_uploads = max(4, self.max_concurrent_processes // 2)  # Upload is often slower

        logger.info(f"Set max parallelism: processes={self.max_concurrent_processes}, "
                    f"downloads={self.max_concurrent_downloads}, uploads={self.max_concurrent_uploads}")

        return self.max_concurrent_processes

    def close(self):
        """Close all connections."""
        self.db.close()
        self.storage.close()


def main():
    """Main entry point."""
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