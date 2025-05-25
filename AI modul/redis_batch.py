import os
import redis
import json
import logging
import time
import numpy as np
import concurrent.futures

# === LOGGING ===
logger = logging.getLogger("RedisBatchProcessor")

# === HELPERS ===
def compare_batch_helper(start_idx, end_idx, all_hashes, hash_data_cache, similarity_threshold):
    local_duplicates = {}
    local_count = 0

    def compare_hashes(hash1, hash2):
        try:
            bin1 = bin(int(hash1, 16))[2:].zfill(64)
            bin2 = bin(int(hash2, 16))[2:].zfill(64)
            distance = sum(bit1 != bit2 for bit1, bit2 in zip(bin1, bin2))
            return 1.0 - (distance / len(bin1))
        except:
            return 0.0

    for i in range(start_idx, min(end_idx, len(all_hashes))):
        hash1 = all_hashes[i]
        hash1_data = hash_data_cache.get(hash1)
        if not hash1_data:
            continue
        id1 = hash1_data.get('internal_id')
        if id1 not in local_duplicates:
            local_duplicates[id1] = set()
        for j in range(i + 1, len(all_hashes)):
            hash2 = all_hashes[j]
            hash2_data = hash_data_cache.get(hash2)
            if not hash2_data:
                continue
            id2 = hash2_data.get('internal_id')
            if id1 == id2:
                continue
            similarity = compare_hashes(hash1, hash2)
            if similarity >= similarity_threshold:
                local_duplicates[id1].add(id2)
                if id2 not in local_duplicates:
                    local_duplicates[id2] = set()
                local_duplicates[id2].add(id1)
                local_count += 1
    return local_duplicates, local_count

# === REDIS BATCH PROCESSOR ===
class RedisBatchProcessor:
    def __init__(self, redis_config):
        self.config = redis_config
        self.r = redis.Redis(
            host=redis_config.get("host", "localhost"),
            port=redis_config.get("port", 6379),
            db=redis_config.get("db", 0),
            password=redis_config.get("password", None),
            decode_responses=True
        )
        self.prefix = redis_config.get("prefix", "imgproc:")
        logger.info(f"Redis connected: {redis_config.get('host', 'localhost')}:{redis_config.get('port', 6379)}")

    def key(self, name):
        return f"{self.prefix}{name}"

    # --- CACHE ---
    def flush_cache(self):
        keys = self.r.keys(self.key("*"))
        if keys:
            self.r.delete(*keys)
            logger.info(f"Flushed {len(keys)} keys from cache")
        return True

    # --- LOAD ENTRIES ---
    def load_all_entries(self, db_handler):
        entries = db_handler.get_all_listings_for_processing()
        if not entries:
            logger.info("No entries to load")
            return 0
        return self.load_entries(entries)

    def load_entries(self, entries):
        if not entries:
            return 0
        logger.info(f"Loading {len(entries)} entries")
        pipe = self.r.pipeline()
        count = 0
        for entry in entries:
            internal_id = str(entry['internal_id'])
            pipe.hset(self.key(f"entry:{internal_id}"), mapping={'internal_id': internal_id, 'processed': 'false'})
            for source in ['hyper_reality', 'sreality_image_urls', 'bezrealitky_image_urls', 'idnes']:
                if entry[source]:
                    urls = self._extract_urls(entry[source])
                    if urls:
                        url_key = self.key(f"urls:{internal_id}:{source}")
                        pipe.delete(url_key)
                        pipe.rpush(url_key, *urls)
                        pipe.hset(self.key(f"entry:{internal_id}"), f"{source}_count", len(urls))
            pipe.sadd(self.key("entries"), internal_id)
            count += 1
            if count % 1000 == 0:
                pipe.execute()
                pipe = self.r.pipeline()
        if count % 1000 != 0:
            pipe.execute()
        logger.info(f"Loaded {count} entries")
        return count

    def _extract_urls(self, data):
        if isinstance(data, str):
            try:
                urls = json.loads(data)
                if isinstance(urls, list):
                    return urls
                return [urls]
            except:
                return [data]
        elif isinstance(data, list):
            if len(data) > 0 and isinstance(data[0], list):
                return data[0]
            return data
        return []

    # --- BATCH ---
    def get_batch(self, batch_size=100):
        batch = self.r.spop(self.key("entries"), batch_size)
        if not batch:
            return []
        return list(batch)

    def get_entry_urls(self, internal_id):
        result = {}
        for source in ['hyper_reality', 'sreality_image_urls', 'bezrealitky_image_urls', 'idnes']:
            url_key = self.key(f"urls:{internal_id}:{source}")
            if self.r.exists(url_key):
                urls = self.r.lrange(url_key, 0, -1)
                if urls:
                    result[source] = urls
        return result

    # --- HASHES ---
    def store_hash(self, internal_id, source, idx, url, hash_value):
        hash_key = self.key(f"hash:{internal_id}:{source}:{idx}")
        self.r.set(hash_key, hash_value)
        url_data = {'internal_id': internal_id, 'source': source, 'index': str(idx), 'url': url}
        self.r.hset(self.key(f"hash_url:{hash_value}"), mapping=url_data)
        self.r.sadd(self.key("all_hashes"), hash_value)
        self.r.sadd(self.key(f"hashes:{internal_id}"), hash_value)
        self.r.set(self.key(f"hash_pos:{internal_id}:{source}:{hash_value}"), idx)
        return True

    def check_hash_exists(self, internal_id, source, idx):
        return self.r.exists(self.key(f"hash:{internal_id}:{source}:{idx}"))

    def mark_entry_processed(self, internal_id):
        self.r.hset(self.key(f"entry:{internal_id}"), "processed", "true")
        self.r.sadd(self.key("processed_entries"), internal_id)

    # --- STATS ---
    def get_total_entries(self):
        return self.r.scard(self.key("entries")) + self.r.scard(self.key("processed_entries"))
    def get_processed_entries(self):
        return self.r.scard(self.key("processed_entries"))
    def get_remaining_entries(self):
        return self.r.scard(self.key("entries"))
    def get_all_hashes(self):
        return self.r.smembers(self.key("all_hashes"))

    def get_entry_hashes(self, internal_id):
        result = {}
        hashes = self.r.smembers(self.key(f"hashes:{internal_id}"))
        for source in ['hyper_reality', 'sreality_image_urls', 'bezrealitky_image_urls', 'idnes']:
            src_hashes = []
            for hash_value in hashes:
                pos = self.r.get(self.key(f"hash_pos:{internal_id}:{source}:{hash_value}"))
                if pos:
                    src_hashes.append((int(pos), hash_value))
            if src_hashes:
                src_hashes.sort(key=lambda x: x[0])
                result[source] = src_hashes
        return result

    # --- DEDUPLICATION ---
    def hash_to_binary_array(self, hash_str):
        try:
            binary = bin(int(hash_str, 16))[2:].zfill(64)
            return np.array([int(bit) for bit in binary], dtype=np.bool_)
        except ValueError:
            logger.warning(f"Invalid hash: {hash_str}")
            return np.zeros(64, dtype=np.bool_)

    def run_deduplication(self, similarity_threshold=0.85):
        try:
            return self.run_deduplication_with_ann(similarity_threshold)
        except ImportError as e:
            logger.warning(f"ANN not available: {e}, using standard")
            return self.run_deduplication_with_parallelism(similarity_threshold)

    def run_deduplication_with_ann(self, similarity_threshold=0.85):
        from annoy import AnnoyIndex
        logger.info("Starting ANN deduplication")
        all_hashes = list(self.get_all_hashes())
        if not all_hashes:
            return {}
        index_to_hash = {}
        hash_to_internal_id = {}
        hash_vectors = []
        for i, h in enumerate(all_hashes):
            data = self.r.hgetall(self.key(f"hash_url:{h}"))
            if not data or 'internal_id' not in data:
                continue
            bin_arr = self.hash_to_binary_array(h)
            index_to_hash[i] = h
            hash_to_internal_id[h] = data['internal_id']
            hash_vectors.append(bin_arr)
        if not hash_vectors:
            logger.warning("No valid vectors")
            return {}
        dim = len(hash_vectors[0])
        idx = AnnoyIndex(dim, 'hamming')
        for i, vec in enumerate(hash_vectors):
            if i in index_to_hash:
                idx.add_item(i, vec)
        n_trees = max(10, min(50, len(hash_vectors) // 10))
        idx.build(n_trees)
        compared_pairs = set()
        duplicates = {}
        dup_count = 0
        k = max(10, min(30, len(hash_vectors) // 2))
        for i in range(len(hash_vectors)):
            if i not in index_to_hash:
                continue
            h1 = index_to_hash[i]
            id1 = hash_to_internal_id[h1]
            neighbors, distances = idx.get_nns_by_item(i, k, include_distances=True)
            for ni, dist in zip(neighbors, distances):
                if ni == i:
                    continue
                h2 = index_to_hash.get(ni)
                if not h2:
                    continue
                id2 = hash_to_internal_id.get(h2)
                if not id2 or id1 == id2:
                    continue
                pair = tuple(sorted((id1, id2)))
                if pair in compared_pairs:
                    continue
                compared_pairs.add(pair)
                similarity = 1.0 - (dist / dim)
                if similarity >= similarity_threshold:
                    duplicates.setdefault(id1, set()).add(id2)
                    duplicates.setdefault(id2, set()).add(id1)
                    dup_count += 1
        result = {id: list(dups) for id, dups in duplicates.items() if dups}
        logger.info(f"ANN deduplication done. {dup_count} duplicate pairs")
        return result

    def run_deduplication_with_parallelism(self, similarity_threshold=0.85, max_workers=None):
        all_hashes = list(self.get_all_hashes())
        logger.info(f"Parallel deduplication on {len(all_hashes)} hashes")
        duplicates = {}
        if max_workers is None:
            max_workers = os.cpu_count() or 4
        hash_data_cache = {h: self.r.hgetall(self.key(f"hash_url:{h}")) for h in all_hashes}
        batch_size = max(100, len(all_hashes) // (max_workers * 2))
        batches = [(i, i + batch_size) for i in range(0, len(all_hashes), batch_size)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(compare_batch_helper, start, end, all_hashes, hash_data_cache, similarity_threshold): (start, end) for start, end in batches}
            for future in concurrent.futures.as_completed(futures):
                batch_duplicates, _ = future.result()
                for id, dups in batch_duplicates.items():
                    if id not in duplicates:
                        duplicates[id] = set()
                    duplicates[id].update(dups)
        result = {id: list(dups) for id, dups in duplicates.items() if dups}
        logger.info(f"Parallel deduplication complete. {len(result)} entries with duplicates")
        return result

    def compare_hashes(self, hash1, hash2):
        try:
            bin1 = bin(int(hash1, 16))[2:].zfill(64)
            bin2 = bin(int(hash2, 16))[2:].zfill(64)
            distance = sum(bit1 != bit2 for bit1, bit2 in zip(bin1, bin2))
            return 1.0 - (distance / len(bin1))
        except:
            return 0.0

    def close(self):
        pass