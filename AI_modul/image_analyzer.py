import io
import os
import logging
import time
import concurrent.futures
from PIL import Image
import numpy as np
import imagehash
import threading

# Set up logging
logger = logging.getLogger("ImageAnalyzer")


class ImageAnalyzer:
    """Analyze images, generate hashes, and compare for similarity with optimized performance."""

    def __init__(self, config):
        self.config = config
        self.hash_size = config.get("hash_size", 16)
        self.hash_type = config.get("hash_type", "phash")
        self.similarity_threshold = config.get("similarity_threshold", 0.85)
        self.max_batch_size = config.get("max_batch_size", 1000)
        self.bucket_bits = config.get("bucket_bits", 8)  # Number of bits to use for bucketing

        # Performance metrics
        self.total_processed = 0
        self.total_comparison_time = 0
        self.total_comparisons = 0

    def process_image(self, image_data):
        """
        Process an image and return its hash and binary data.

        Args:
            image_data (BytesIO): The image data as bytes

        Returns:
            tuple: (image_hash, processed_image_data, hash_binary, hash_bucket)
        """
        try:
            start_time = time.time()

            # Open image using PIL
            img = Image.open(image_data)

            # Convert to RGB if needed (handles PNG with alpha channel, etc.)
            if img.mode != 'RGB':
                img = img.convert('RGB')

            # Resize for storage efficiency if configured
            if 'max_size' in self.config:
                max_size = self.config.get("max_size")
                img.thumbnail((max_size, max_size), Image.LANCZOS)

            # Generate image hash according to configured hash type
            if self.hash_type == "phash":
                img_hash = imagehash.phash(img, hash_size=self.hash_size)
            elif self.hash_type == "dhash":
                img_hash = imagehash.dhash(img, hash_size=self.hash_size)
            elif self.hash_type == "ahash":
                img_hash = imagehash.average_hash(img, hash_size=self.hash_size)
            elif self.hash_type == "whash":
                img_hash = imagehash.whash(img, hash_size=self.hash_size)
            else:
                # Default to perceptual hash
                img_hash = imagehash.phash(img, hash_size=self.hash_size)

            # Convert to string for storage
            hash_str = str(img_hash)

            # Generate binary hash and bucket
            hash_binary = self.hash_to_bytes(hash_str)
            hash_bucket = self.calculate_hash_bucket(hash_str)

            # Save processed image to bytes
            img_bytes = io.BytesIO()
            img.save(img_bytes, format='JPEG', quality=85)
            img_bytes = img_bytes.getvalue()

            processing_time = time.time() - start_time
            self.total_processed += 1

            logger.debug(f"Processed image: {hash_str}, size: {len(img_bytes)} bytes, time: {processing_time:.3f}s")

            return hash_str, img_bytes, hash_binary, hash_bucket

        except Exception as e:
            logger.error(f"Error processing image: {str(e)}")
            raise

    def hash_to_binary_array(self, hash_str):
        """Convert hash string to numpy binary array for faster comparison."""
        # Convert hex string to binary
        try:
            binary = bin(int(hash_str, 16))[2:].zfill(self.hash_size * self.hash_size)
            return np.array([int(bit) for bit in binary], dtype=np.bool_)
        except ValueError as e:
            logger.error(f"Error converting hash to binary: {str(e)}, hash: {hash_str}")
            # Return a default array if conversion fails
            return np.zeros(self.hash_size * self.hash_size, dtype=np.bool_)

    def hash_to_bytes(self, hash_str):
        """Convert hash string to bytes for database storage."""
        # Convert the hash string to a binary numpy array
        binary_array = self.hash_to_binary_array(hash_str)
        # Pack the boolean array into bytes (8 bits per byte)
        return np.packbits(binary_array).tobytes()

    def bytes_to_binary_array(self, hash_bytes):
        """Convert stored bytes back to binary array."""
        # Unpack bytes to boolean array, making sure it's the right length
        unpacked = np.unpackbits(np.frombuffer(hash_bytes, dtype=np.uint8))
        # Truncate or pad to the expected length
        expected_length = self.hash_size * self.hash_size
        if len(unpacked) > expected_length:
            return unpacked[:expected_length].astype(np.bool_)
        elif len(unpacked) < expected_length:
            return np.pad(unpacked, (0, expected_length - len(unpacked)), 'constant').astype(np.bool_)
        return unpacked.astype(np.bool_)

    def calculate_hash_bucket(self, hash_str):
        """Calculate the hash bucket for a given hash string."""
        # Use first n bits to determine bucket
        binary_array = self.hash_to_binary_array(hash_str)

        # Convert first bucket_bits to an integer bucket ID
        bucket_id = 0
        for i in range(min(self.bucket_bits, len(binary_array))):
            if binary_array[i]:
                bucket_id |= (1 << i)

        return bucket_id

    def compare_binary_arrays(self, array1, array2):
        """Compare two binary arrays and return similarity score."""
        # Ensure arrays are the same length
        min_len = min(len(array1), len(array2))
        array1 = array1[:min_len]
        array2 = array2[:min_len]

        # Calculate hamming distance using XOR (finds differing bits)
        distance = np.sum(array1 != array2)

        # Convert to similarity score (0 to 1)
        max_distance = len(array1)
        similarity = 1.0 - (distance / max_distance)

        # Convert to standard Python float
        return float(similarity)

    def compare_hashes(self, hash1, hash2):
        """
        Compare two image hashes and return similarity score.
        Backward compatible method that converts to binary first.

        Args:
            hash1 (str): First image hash string
            hash2 (str): Second image hash string

        Returns:
            float: Similarity score (0.0 to 1.0, higher is more similar)
        """
        try:
            # Convert to binary arrays
            array1 = self.hash_to_binary_array(hash1)
            array2 = self.hash_to_binary_array(hash2)

            # Use optimized binary comparison
            return self.compare_binary_arrays(array1, array2)

        except Exception as e:
            logger.error(f"Error comparing hashes: {str(e)}")
            return 0.0

    def find_duplicates(self, db_handler, new_image_id, new_hash):
        """
        Find duplicates for a newly processed image using LSH for efficiency.

        Args:
            db_handler: Database connection
            new_image_id: ID of the newly processed image
            new_hash: Hash string of the new image

        Returns:
            list: List of tuples (image_id, similarity_score) of duplicates
        """
        duplicates = []

        # Calculate bucket for this hash
        bucket_id = self.calculate_hash_bucket(new_hash)

        # Get nearby buckets (hamming distance 1)
        nearby_buckets = [bucket_id]
        for i in range(self.bucket_bits):  # Flip each bit to get adjacent buckets
            nearby_buckets.append(bucket_id ^ (1 << i))

        # Convert new hash to binary array for comparison
        new_hash_array = self.hash_to_binary_array(new_hash)

        logger.info(f"Starting LSH duplicate search for image ID {new_image_id}, bucket {bucket_id}")

        start_time = time.time()

        # Get candidates from database
        candidates = db_handler.get_images_in_buckets(nearby_buckets)
        if not candidates:
            logger.info(f"No candidates found in buckets for image ID {new_image_id}")
            return []

        logger.info(f"Found {len(candidates)} candidate matches across {len(nearby_buckets)} buckets")

        # Use ProcessPoolExecutor for better performance on CPU-bound tasks
        with concurrent.futures.ProcessPoolExecutor(max_workers=min(16, os.cpu_count())) as executor:
            # Prepare tasks for parallel processing
            future_to_candidate = {}
            for candidate in candidates:
                # Skip comparing with itself
                if candidate['id'] == new_image_id:
                    continue

                # Convert stored binary hash back to array
                candidate_hash_array = self.bytes_to_binary_array(candidate['hash_binary'])

                # Submit comparison task
                future = executor.submit(self.compare_binary_arrays, new_hash_array, candidate_hash_array)
                future_to_candidate[future] = candidate

            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_candidate):
                candidate = future_to_candidate[future]
                try:
                    similarity = future.result()
                    self.total_comparisons += 1

                    # Record if above threshold
                    if similarity >= self.similarity_threshold:
                        duplicates.append((candidate['id'], similarity))
                        logger.info(
                            f"Found duplicate: {new_image_id} and {candidate['id']} with score {similarity:.4f}")
                except Exception as e:
                    logger.error(f"Error comparing with image ID {candidate['id']}: {str(e)}")

        total_time = time.time() - start_time
        self.total_comparison_time += total_time

        if duplicates:
            logger.info(f"Found {len(duplicates)} duplicates for image ID {new_image_id} in {total_time:.3f}s")
        else:
            logger.info(f"No duplicates found for image ID {new_image_id} in {total_time:.3f}s")

        return duplicates

    def benchmark_find_duplicates(self, db_handler, test_samples=10):
        """Benchmark the duplicate finding process."""
        logger.info(f"Starting benchmark with {test_samples} sample images")

        # Get some random images for testing
        conn = db_handler.connect()
        samples = []
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                    SELECT id, image_hash 
                    FROM processed_images
                    ORDER BY RANDOM()
                    LIMIT %s
                """, (test_samples,))
                samples = cur.fetchall()
        except Exception as e:
            logger.error(f"Error fetching samples for benchmark: {str(e)}")
            return None

        if not samples:
            logger.warning("No samples found for benchmarking")
            return None

        # Track metrics
        total_time = 0
        total_candidates = 0
        duplicates_found = 0

        for sample in samples:
            start_time = time.time()
            duplicates = self.find_duplicates(db_handler, sample['id'], sample['image_hash'])
            elapsed = time.time() - start_time

            duplicates_found += len(duplicates)
            total_time += elapsed

            logger.info(f"Sample {sample['id']}: found {len(duplicates)} duplicates in {elapsed:.3f}s")

        avg_time = total_time / len(samples)
        logger.info(f"Benchmark results: Avg time per image: {avg_time:.3f}s, Total duplicates: {duplicates_found}")
        return avg_time

    def get_performance_metrics(self):
        """Get performance metrics for reporting."""
        metrics = {
            "total_processed": self.total_processed,
            "total_comparisons": self.total_comparisons,
            "avg_comparison_time": self.total_comparison_time / max(1, self.total_comparisons) * 1000  # in ms
        }
        return metrics