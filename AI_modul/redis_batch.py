import os
import redis
import json
import logging
import time
import concurrent.futures
from io import BytesIO
import numpy as np

logger = logging.getLogger("RedisBatchProcessor")


# Helper function for parallel hash comparison defined at module level
def compare_batch_helper(start_idx, end_idx, all_hashes, hash_data_cache, similarity_threshold):
    """
    Helper function for parallel hash comparison.
    Must be defined at module level to allow pickling.

    Args:
        start_idx: Start index in the all_hashes list
        end_idx: End index in the all_hashes list
        all_hashes: List of all hash values
        hash_data_cache: Dictionary mapping hash values to their metadata
        similarity_threshold: Minimum similarity to be considered a duplicate

    Returns:
        tuple: (duplicates_dict, count)
    """
    local_duplicates = {}
    local_count = 0

    # Helper function for hash comparison
    def compare_hashes(hash1, hash2):
        """Compare two hash strings and return similarity score (0-1)."""
        try:
            # Convert hex strings to binary
            bin1 = bin(int(hash1, 16))[2:].zfill(64)
            bin2 = bin(int(hash2, 16))[2:].zfill(64)

            # Calculate hamming distance
            distance = sum(bit1 != bit2 for bit1, bit2 in zip(bin1, bin2))

            # Convert to similarity score
            similarity = 1.0 - (distance / len(bin1))
            return similarity
        except:
            return 0.0  # Return 0 similarity if comparison fails

    for i in range(start_idx, min(end_idx, len(all_hashes))):
        hash1 = all_hashes[i]

        # Get info for hash1
        hash1_data = hash_data_cache.get(hash1)
        if not hash1_data:
            continue

        internal_id1 = hash1_data.get('internal_id')

        # Initialize duplicates for this ID if needed
        if internal_id1 not in local_duplicates:
            local_duplicates[internal_id1] = set()

        # Compare with all other hashes
        for j in range(i + 1, len(all_hashes)):
            hash2 = all_hashes[j]

            # Get info for hash2
            hash2_data = hash_data_cache.get(hash2)
            if not hash2_data:
                continue

            internal_id2 = hash2_data.get('internal_id')

            # Skip if same internal_id
            if internal_id1 == internal_id2:
                continue

            # Compare hashes
            similarity = compare_hashes(hash1, hash2)

            # Record duplicates if above threshold
            if similarity >= similarity_threshold:
                local_duplicates[internal_id1].add(internal_id2)

                # Ensure second ID has an entry
                if internal_id2 not in local_duplicates:
                    local_duplicates[internal_id2] = set()
                local_duplicates[internal_id2].add(internal_id1)

                local_count += 1

    return local_duplicates, local_count


class RedisBatchProcessor:
    """Handle batching and Redis caching for simplified image processing workflow."""

    def __init__(self, redis_config):
        """Initialize Redis connection and configuration."""
        self.config = redis_config
        self.r = redis.Redis(
            host=redis_config.get("host", "localhost"),
            port=redis_config.get("port", 6379),
            db=redis_config.get("db", 0),
            password=redis_config.get("password", None),
            decode_responses=True  # Use string responses for simplicity
        )
        self.prefix = redis_config.get("prefix", "imgproc:")
        logger.info(
            f"Initialized Redis connection to {redis_config.get('host', 'localhost')}:{redis_config.get('port', 6379)}")

    def key(self, name):
        """Create a prefixed key."""
        return f"{self.prefix}{name}"

    def flush_cache(self):
        """Clear any existing cache data."""
        pattern = self.key("*")
        keys = self.r.keys(pattern)
        if keys:
            self.r.delete(*keys)
            logger.info(f"Flushed {len(keys)} keys from Redis cache")
        return True

    def load_all_entries(self, db_handler):
        """
        Load all entries from image_links table to Redis.
        This step loads the METADATA only, not the images themselves.
        """
        logger.info("Loading all entries from database to Redis...")

        # Get all entries that need processing from database
        entries = db_handler.get_all_listings_for_processing()

        if not entries:
            logger.info("No entries found for processing")
            return 0

        return self.load_entries(entries)

    def load_entries(self, entries):
        """
        Load specific entries to Redis.

        Args:
            entries: List of entry dictionaries

        Returns:
            int: Number of entries loaded
        """
        if not entries:
            logger.info("No entries provided for loading")
            return 0

        logger.info(f"Loading {len(entries)} entries to Redis")

        # Use pipeline for bulk loading
        pipe = self.r.pipeline()
        count = 0

        # Add each entry to Redis
        for entry in entries:
            internal_id = str(entry['internal_id'])

            # Store entry metadata
            pipe.hset(self.key(f"entry:{internal_id}"), mapping={
                'internal_id': internal_id,
                'processed': 'false'
            })

            # Store URLs by source
            for source in ['hyper_reality', 'sreality_image_urls', 'bezrealitky_image_urls', 'idnes']:
                if entry[source]:
                    # Extract URLs from the source
                    urls = self._extract_urls(entry[source])

                    if urls:
                        # Store URLs as a list
                        url_key = self.key(f"urls:{internal_id}:{source}")
                        pipe.delete(url_key)  # Clear any existing data
                        pipe.rpush(url_key, *urls)

                        # Store URL count
                        pipe.hset(self.key(f"entry:{internal_id}"), f"{source}_count", len(urls))

            # Add to entries set for batch processing
            pipe.sadd(self.key("entries"), internal_id)

            count += 1
            # Execute in batches to avoid large pipelines
            if count % 1000 == 0:
                pipe.execute()
                pipe = self.r.pipeline()
                logger.info(f"Loaded {count} entries to Redis")

        # Execute any remaining commands
        if count % 1000 != 0:
            pipe.execute()

        logger.info(f"Successfully loaded {count} entries to Redis")
        return count

    def _extract_urls(self, data):
        """Extract URLs from different data formats."""
        if isinstance(data, str):
            try:
                # Try to parse as JSON
                urls = json.loads(data)
                if isinstance(urls, list):
                    return urls
                return [urls]
            except:
                # If not JSON, treat as single URL
                return [data]
        elif isinstance(data, list):
            # Handle nested lists
            if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list):
                return data[0]
            return data
        return []

    def get_batch(self, batch_size=100):
        """
        Get a batch of entries for processing.

        Returns:
            list: List of internal_ids in the batch
        """
        # Get batch of entries from set
        batch = self.r.spop(self.key("entries"), batch_size)

        if not batch:
            return []

        # Convert to list of strings
        return list(batch)

    def get_entry_urls(self, internal_id):
        """
        Get all URLs for an entry.

        Returns:
            dict: Dictionary of source -> list of URLs
        """
        result = {}

        # Check each possible source
        for source in ['hyper_reality', 'sreality_image_urls', 'bezrealitky_image_urls', 'idnes']:
            url_key = self.key(f"urls:{internal_id}:{source}")
            if self.r.exists(url_key):
                urls = self.r.lrange(url_key, 0, -1)
                if urls:
                    result[source] = urls

        return result

    def store_hash(self, internal_id, source, index, url, hash_value):
        """
        Store a hash value for a specific URL.

        Args:
            internal_id: Internal ID of the entry
            source: Source column name
            index: Index of the URL in the source
            url: The URL itself
            hash_value: The calculated hash value
        """
        # Store hash for this URL
        hash_key = self.key(f"hash:{internal_id}:{source}:{index}")
        self.r.set(hash_key, hash_value)

        # Store URL by hash for deduplication
        url_data = {
            'internal_id': internal_id,
            'source': source,
            'index': str(index),
            'url': url
        }

        hash_url_key = self.key(f"hash_url:{hash_value}")
        self.r.hset(hash_url_key, mapping=url_data)

        # Add to hashes set for deduplication
        self.r.sadd(self.key("all_hashes"), hash_value)

        # Add to hashes for this internal_id
        self.r.sadd(self.key(f"hashes:{internal_id}"), hash_value)

        # Store in the correct position in the source array
        pos_key = self.key(f"hash_pos:{internal_id}:{source}:{hash_value}")
        self.r.set(pos_key, index)

        return True

    def check_hash_exists(self, internal_id, source, index):
        """Check if a hash already exists for this URL."""
        hash_key = self.key(f"hash:{internal_id}:{source}:{index}")
        return self.r.exists(hash_key)

    def get_hash(self, internal_id, source, index):
        """Get the hash value for a URL if it exists."""
        hash_key = self.key(f"hash:{internal_id}:{source}:{index}")
        return self.r.get(hash_key)

    def mark_entry_processed(self, internal_id):
        """Mark an entry as fully processed."""
        entry_key = self.key(f"entry:{internal_id}")
        self.r.hset(entry_key, "processed", "true")
        self.r.sadd(self.key("processed_entries"), internal_id)

    def get_total_entries(self):
        """Get total number of entries loaded."""
        return self.r.scard(self.key("entries")) + self.r.scard(self.key("processed_entries"))

    def get_processed_entries(self):
        """Get number of processed entries."""
        return self.r.scard(self.key("processed_entries"))

    def get_remaining_entries(self):
        """Get number of remaining entries."""
        return self.r.scard(self.key("entries"))

    def get_all_hashes(self):
        """Get all hash values in the system."""
        return self.r.smembers(self.key("all_hashes"))

    def get_entry_hashes(self, internal_id):
        """Get all hashes for a specific entry."""
        result = {}

        # Get all hashes for this entry
        hashes = self.r.smembers(self.key(f"hashes:{internal_id}"))

        # Group by source
        for source in ['hyper_reality', 'sreality_image_urls', 'bezrealitky_image_urls', 'idnes']:
            source_hashes = []

            for hash_value in hashes:
                # Check if hash exists for this source
                pos_key = self.key(f"hash_pos:{internal_id}:{source}:{hash_value}")
                pos = self.r.get(pos_key)

                if pos:
                    # Add to source hashes with position
                    source_hashes.append((int(pos), hash_value))

            # Sort by position
            if source_hashes:
                source_hashes.sort(key=lambda x: x[0])
                result[source] = source_hashes

        return result

    def hash_to_binary_array(self, hash_str):
        """Convert hash string to binary array."""
        try:
            # Convert hex string to binary
            binary = bin(int(hash_str, 16))[2:].zfill(64)  # Assuming 16x16 hash size
            return np.array([int(bit) for bit in binary], dtype=np.bool_)
        except ValueError:
            # Return default array if conversion fails
            logger.warning(f"Invalid hash value: {hash_str}")
            return np.zeros(64, dtype=np.bool_)

    def run_deduplication(self, similarity_threshold=0.85):
        """
        Run deduplication across all hashes.

        Returns:
            dict: Dictionary of internal_id -> list of duplicate internal_ids
        """
        # Try to use the ANN-based approach first
        try:
            return self.run_deduplication_with_ann(similarity_threshold)
        except ImportError as e:
            logger.warning(f"ANN library not available: {str(e)}. Falling back to standard method.")
            import os
            max_workers = os.cpu_count() or 4
            return self.run_deduplication_with_parallelism(similarity_threshold, max_workers)

    def run_deduplication_with_ann(self, similarity_threshold=0.85):
        """
        Run deduplication using Approximate Nearest Neighbor search with Annoy.
        This method is much faster than exhaustive comparison.

        Args:
            similarity_threshold: Minimum similarity to consider as duplicate

        Returns:
            dict: Dictionary of internal_id -> list of duplicate internal_ids
        """
        from annoy import AnnoyIndex

        logger.info("Starting deduplication with Approximate Nearest Neighbor search...")
        start_time = time.time()

        # Get all hashes
        all_hashes = list(self.get_all_hashes())
        if not all_hashes:
            logger.info("No hashes found for deduplication")
            return {}

        logger.info(f"Found {len(all_hashes)} hashes for ANN-based deduplication")

        # Map from index to hash value and internal_id
        index_to_hash = {}
        hash_to_index = {}
        hash_to_internal_id = {}

        # Convert hashes to binary arrays for ANN
        hash_vectors = []
        for i, hash_val in enumerate(all_hashes):
            # Get metadata for this hash
            hash_data = self.r.hgetall(self.key(f"hash_url:{hash_val}"))
            if not hash_data or 'internal_id' not in hash_data:
                continue

            # Convert hash to binary array
            bin_array = self.hash_to_binary_array(hash_val)

            # Store mapping information
            index_to_hash[i] = hash_val
            hash_to_index[hash_val] = i
            hash_to_internal_id[hash_val] = hash_data['internal_id']

            # Add to vectors list
            hash_vectors.append(bin_array)

        if not hash_vectors:
            logger.warning("No valid hash vectors found")
            return {}

        # Create Annoy index
        vector_dim = len(hash_vectors[0])
        logger.info(f"Creating Annoy index with {len(hash_vectors)} vectors of dimension {vector_dim}")

        # Use Hamming distance for binary hashes
        index = AnnoyIndex(vector_dim, 'hamming')

        # Add items to index
        for i, vector in enumerate(hash_vectors):
            if i in index_to_hash:  # Only add valid vectors
                index.add_item(i, vector)

        # Build the index - more trees = more accurate but slower to build
        n_trees = min(50, len(hash_vectors) // 10)  # Scale trees with data size
        n_trees = max(10, n_trees)  # But use at least 10 trees

        logger.info(f"Building Annoy index with {n_trees} trees...")
        index.build(n_trees)

        # Set of already compared pairs to avoid duplicates
        compared_pairs = set()

        # Dictionary to store duplicates by internal_id
        duplicates = {}
        dup_count = 0

        # Number of neighbors to search
        k_neighbors = min(30, len(hash_vectors) // 2)  # Scale with data size
        k_neighbors = max(10, k_neighbors)  # But use at least 10 neighbors

        logger.info(f"Performing ANN search with k={k_neighbors} neighbors...")

        # For each hash, find nearest neighbors
        for i in range(len(hash_vectors)):
            if i not in index_to_hash:
                continue

            hash_val = index_to_hash[i]
            internal_id = hash_to_internal_id[hash_val]

            # Find k nearest neighbors
            neighbor_indices, distances = index.get_nns_by_item(i, k_neighbors, include_distances=True)

            # Process neighbors
            for neighbor_idx, distance in zip(neighbor_indices, distances):
                if neighbor_idx == i:
                    continue  # Skip self

                neighbor_hash = index_to_hash.get(neighbor_idx)
                if not neighbor_hash:
                    continue

                neighbor_id = hash_to_internal_id.get(neighbor_hash)
                if not neighbor_id:
                    continue

                # Skip if same internal_id (same entry)
                if internal_id == neighbor_id:
                    continue

                # Create a unique pair identifier
                if internal_id < neighbor_id:
                    pair = (internal_id, neighbor_id)
                else:
                    pair = (neighbor_id, internal_id)

                # Skip if already compared
                if pair in compared_pairs:
                    continue

                compared_pairs.add(pair)

                # Calculate similarity from distance
                # Annoy returns Hamming distance (lower is more similar)
                similarity = 1.0 - (distance / vector_dim)

                # Check if above threshold
                if similarity >= similarity_threshold:
                    # Add to duplicates dictionary (both ways)
                    if internal_id not in duplicates:
                        duplicates[internal_id] = set()
                    duplicates[internal_id].add(neighbor_id)

                    if neighbor_id not in duplicates:
                        duplicates[neighbor_id] = set()
                    duplicates[neighbor_id].add(internal_id)

                    dup_count += 1

        # Convert sets to lists for return
        result = {}
        for id_val, dups in duplicates.items():
            if dups:  # Only include entries with duplicates
                result[id_val] = list(dups)

        elapsed_time = time.time() - start_time
        logger.info(f"ANN deduplication completed in {elapsed_time:.2f}s. "
                    f"Found {dup_count} duplicate relationships across {len(result)} entries.")

        return result

    def run_deduplication_with_parallelism(self, similarity_threshold=0.85, max_workers=None):
        """
        Run deduplication across all hashes with parallelism.

        Args:
            similarity_threshold: Minimum similarity to consider images as duplicates
            max_workers: Maximum number of parallel workers for comparison (default: CPU count)

        Returns:
            dict: Dictionary of internal_id -> list of duplicate internal_ids
        """
        import concurrent.futures
        import os
        import sys

        # Import the helper function from the current module
        # This ensures it's available for the executor
        logger.info("Starting deduplication process with parallelism...")

        # Get all hashes
        all_hashes = list(self.get_all_hashes())
        logger.info(f"Found {len(all_hashes)} total hashes for comparison")

        # Track duplicates by internal_id
        duplicates = {}

        # Maximum number of workers
        if max_workers is None:
            max_workers = os.cpu_count() or 4

        # Calculate total comparisons
        total_comparisons = (len(all_hashes) * (len(all_hashes) - 1)) // 2
        logger.info(f"Will perform approximately {total_comparisons} comparisons")

        # Pre-load all hash data to avoid Redis latency in the inner loop
        hash_data_cache = {}
        for hash_val in all_hashes:
            hash_data = self.r.hgetall(self.key(f"hash_url:{hash_val}"))
            if hash_data:
                hash_data_cache[hash_val] = hash_data

        # Process each hash in batches for better parallelism
        dup_count = 0
        start_time = time.time()
        completed = 0

        # Calculate batch size based on total comparisons and workers
        batch_size = max(100, len(all_hashes) // (max_workers * 2))

        # Create batch ranges
        batches = []
        for i in range(0, len(all_hashes), batch_size):
            batches.append((i, i + batch_size))

        logger.info(f"Split deduplication into {len(batches)} batches with size {batch_size}")

        # We'll use ThreadPoolExecutor instead of ProcessPoolExecutor to avoid pickling issues
        # It's still faster than sequential processing for I/O bound tasks
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all batches
            future_to_batch = {
                executor.submit(
                    compare_batch_helper,  # Use the global function
                    start,
                    end,
                    all_hashes,
                    hash_data_cache,
                    similarity_threshold
                ): (start, end)
                for start, end in batches
            }

            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_batch):
                batch_range = future_to_batch[future]
                try:
                    batch_duplicates, batch_count = future.result()

                    # Merge batch results into main dictionary
                    for id, dups in batch_duplicates.items():
                        if id not in duplicates:
                            duplicates[id] = set()
                        duplicates[id].update(dups)

                    dup_count += batch_count
                    completed += 1

                    # Log progress
                    elapsed = time.time() - start_time
                    progress = completed / len(batches) * 100

                    logger.info(f"Completed batch {completed}/{len(batches)} ({progress:.1f}%) - "
                                f"Found {dup_count} duplicates so far, elapsed time: {elapsed:.2f}s")

                except Exception as e:
                    logger.error(f"Error processing batch {batch_range}: {str(e)}")

        # Convert to dictionary with list values
        result = {}
        for id, dups in duplicates.items():
            if dups:  # Only include entries with duplicates
                result[id] = list(dups)

        total_time = time.time() - start_time
        logger.info(f"Deduplication complete in {total_time:.2f}s. Found {len(result)} entries with duplicates")
        return result

    def compare_hashes(self, hash1, hash2):
        """
        Compare two hashes and return similarity.

        Args:
            hash1: First hash string (hex format)
            hash2: Second hash string (hex format)

        Returns:
            float: Similarity score (0-1)
        """
        # Convert to binary arrays
        try:
            # Convert hex strings to binary
            bin1 = bin(int(hash1, 16))[2:].zfill(64)
            bin2 = bin(int(hash2, 16))[2:].zfill(64)

            # Calculate hamming distance
            distance = sum(bit1 != bit2 for bit1, bit2 in zip(bin1, bin2))

            # Convert to similarity score
            similarity = 1.0 - (distance / len(bin1))
            return similarity
        except:
            return 0.0  # Return 0 similarity if comparison fails

    def close(self):
        """Close Redis connection."""
        # Redis connections are managed automatically
        pass