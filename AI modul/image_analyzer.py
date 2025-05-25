import io
import os
import time
import logging
import concurrent.futures
import threading
import numpy as np
from PIL import Image
import imagehash

# === LOGGING SETUP ===
logger = logging.getLogger("ImageAnalyzer")

# === IMAGE ANALYZER ===
class ImageAnalyzer:
    """Image hashing and duplicate detection (fast, efficient, configurable)."""

    def __init__(self, config):
        self.config = config
        self.hash_size = config.get("hash_size", 16)
        self.hash_type = config.get("hash_type", "phash")
        self.similarity_threshold = config.get("similarity_threshold", 0.85)
        self.max_batch_size = config.get("max_batch_size", 1000)
        self.bucket_bits = config.get("bucket_bits", 8)

        # --- Stats ---
        self.total_processed = 0
        self.total_comparison_time = 0
        self.total_comparisons = 0

    # === IMAGE HASHING ===
    def process_image(self, image_data):
        try:
            t0 = time.time()
            img = Image.open(image_data)
            if img.mode != 'RGB':
                img = img.convert('RGB')
            if 'max_size' in self.config:
                max_size = self.config['max_size']
                img.thumbnail((max_size, max_size), Image.LANCZOS)

            # --- Hash type selection ---
            htype = self.hash_type
            if htype == "phash":
                img_hash = imagehash.phash(img, hash_size=self.hash_size)
            elif htype == "dhash":
                img_hash = imagehash.dhash(img, hash_size=self.hash_size)
            elif htype == "ahash":
                img_hash = imagehash.average_hash(img, hash_size=self.hash_size)
            elif htype == "whash":
                img_hash = imagehash.whash(img, hash_size=self.hash_size)
            else:
                img_hash = imagehash.phash(img, hash_size=self.hash_size)

            hash_str = str(img_hash)
            hash_binary = self.hash_to_bytes(hash_str)
            hash_bucket = self.calculate_hash_bucket(hash_str)

            img_bytes = io.BytesIO()
            img.save(img_bytes, format='JPEG', quality=85)
            img_bytes = img_bytes.getvalue()

            self.total_processed += 1
            logger.debug(f"Image processed: {hash_str}, {len(img_bytes)} bytes, {time.time()-t0:.3f}s")
            return hash_str, img_bytes, hash_binary, hash_bucket

        except Exception as e:
            logger.error(f"process_image: {e}")
            raise

    # === HASH CONVERSIONS ===
    def hash_to_binary_array(self, hash_str):
        try:
            binary = bin(int(hash_str, 16))[2:].zfill(self.hash_size * self.hash_size)
            return np.array([int(bit) for bit in binary], dtype=np.bool_)
        except Exception as e:
            logger.error(f"hash_to_binary_array: {e}")
            return np.zeros(self.hash_size * self.hash_size, dtype=np.bool_)

    def hash_to_bytes(self, hash_str):
        arr = self.hash_to_binary_array(hash_str)
        return np.packbits(arr).tobytes()

    def bytes_to_binary_array(self, hash_bytes):
        arr = np.unpackbits(np.frombuffer(hash_bytes, dtype=np.uint8))
        L = self.hash_size * self.hash_size
        if len(arr) > L:
            return arr[:L].astype(np.bool_)
        elif len(arr) < L:
            return np.pad(arr, (0, L - len(arr)), 'constant').astype(np.bool_)
        return arr.astype(np.bool_)

    # === HASH BUCKETING ===
    def calculate_hash_bucket(self, hash_str):
        arr = self.hash_to_binary_array(hash_str)
        bucket_id = 0
        for i in range(min(self.bucket_bits, len(arr))):
            if arr[i]:
                bucket_id |= (1 << i)
        return bucket_id

    # === HASH COMPARISON ===
    def compare_binary_arrays(self, arr1, arr2):
        L = min(len(arr1), len(arr2))
        dist = np.sum(arr1[:L] != arr2[:L])
        sim = 1.0 - (dist / L)
        return float(sim)

    def compare_hashes(self, hash1, hash2):
        try:
            arr1 = self.hash_to_binary_array(hash1)
            arr2 = self.hash_to_binary_array(hash2)
            return self.compare_binary_arrays(arr1, arr2)
        except Exception as e:
            logger.error(f"compare_hashes: {e}")
            return 0.0

    # === DUPLICATE SEARCH ===
    def find_duplicates(self, db_handler, new_image_id, new_hash):
        bucket_id = self.calculate_hash_bucket(new_hash)
        nearby = [bucket_id] + [bucket_id ^ (1 << i) for i in range(self.bucket_bits)]
        new_arr = self.hash_to_binary_array(new_hash)
        logger.info(f"LSH search for ID {new_image_id}, bucket {bucket_id}")
        t0 = time.time()
        candidates = db_handler.get_images_in_buckets(nearby)
        if not candidates:
            logger.info(f"No candidates found for image ID {new_image_id}")
            return []
        logger.info(f"Found {len(candidates)} candidates across {len(nearby)} buckets")
        duplicates = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=min(16, os.cpu_count())) as pool:
            futures = {}
            for cand in candidates:
                if cand['id'] == new_image_id:
                    continue
                cand_arr = self.bytes_to_binary_array(cand['hash_binary'])
                fut = pool.submit(self.compare_binary_arrays, new_arr, cand_arr)
                futures[fut] = cand
            for fut in concurrent.futures.as_completed(futures):
                cand = futures[fut]
                try:
                    sim = fut.result()
                    self.total_comparisons += 1
                    if sim >= self.similarity_threshold:
                        duplicates.append((cand['id'], sim))
                        logger.info(f"Duplicate: {new_image_id} and {cand['id']} (score {sim:.4f})")
                except Exception as e:
                    logger.error(f"compare error for {cand['id']}: {e}")
        t1 = time.time() - t0
        self.total_comparison_time += t1
        logger.info(f"Duplicates for ID {new_image_id}: {len(duplicates)} in {t1:.3f}s")
        return duplicates

    # === BENCHMARKING ===
    def benchmark_find_duplicates(self, db_handler, test_samples=10):
        logger.info(f"Benchmarking on {test_samples} samples")
        conn = db_handler.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id, image_hash FROM processed_images ORDER BY RANDOM() LIMIT %s", (test_samples,))
                samples = cur.fetchall()
        except Exception as e:
            logger.error(f"benchmark_find_duplicates: {e}")
            return None
        if not samples:
            logger.warning("No samples for benchmarking")
            return None
        t_all = 0
        total_found = 0
        for s in samples:
            t0 = time.time()
            dups = self.find_duplicates(db_handler, s[0], s[1])
            t1 = time.time() - t0
            total_found += len(dups)
            t_all += t1
            logger.info(f"Sample {s[0]}: {len(dups)} dups, {t1:.3f}s")
        avg = t_all / len(samples)
        logger.info(f"Benchmark: {avg:.3f}s/image, total found: {total_found}")
        return avg

    # === METRICS ===
    def get_performance_metrics(self):
        return {
            "total_processed": self.total_processed,
            "total_comparisons": self.total_comparisons,
            "avg_comparison_time": (self.total_comparison_time / max(1, self.total_comparisons)) * 1000
        }