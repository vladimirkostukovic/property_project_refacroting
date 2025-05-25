import os
import json
import logging
from datetime import datetime
import psycopg2.extras

# ========== LOGGER SETUP ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger("BatchManager")

# ========== BATCH MANAGER CLASS ==========
class BatchManager:
    """Manage batching of database entries for processing."""

    def __init__(self, db_handler, batch_size=50):
        """
        Args:
            db_handler: Database handler instance
            batch_size: Size of each batch
        """
        self.db_handler = db_handler
        self.batch_size = batch_size
        self.batches_dir = os.path.join(os.getcwd(), "batches")
        os.makedirs(self.batches_dir, exist_ok=True)

    # ========== BATCH PREPARATION ==========
    def prepare_batches(self, total_entries):
        """
        Prepare batches from the database.

        Args:
            total_entries: Total number of entries to fetch

        Returns:
            tuple: (batch_file_path, total_batches, total_listings)
        """
        logger.info(f"Preparing batches for {total_entries} entries (batch size {self.batch_size})")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        batch_file = os.path.join(self.batches_dir, f"batch_run_{timestamp}.json")
        all_entries = self._fetch_all_unique_entries(total_entries)

        if not all_entries:
            logger.warning("No entries found in database for processing")
            return None, 0, 0

        # Split into batches
        batches = [all_entries[i:i+self.batch_size] for i in range(0, len(all_entries), self.batch_size)]

        with open(batch_file, 'w') as f:
            json.dump(batches, f, indent=2)

        logger.info(f"Created {len(batches)} batches ({len(all_entries)} total entries)")
        logger.info(f"Batches saved to {batch_file}")

        return batch_file, len(batches), len(all_entries)

    # ========== FETCH ENTRIES FROM DB ==========
    def _fetch_all_unique_entries(self, limit):
        """
        Fetch unique entries from the database.

        Args:
            limit: Maximum number of entries to fetch

        Returns:
            list: List of unique entries (dicts)
        """
        logger.info(f"Fetching {limit} unique entries from database")
        unique_entries = []
        processed_ids = set()

        conn = self.db_handler.connect()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                while len(unique_entries) < limit:
                    needed = limit - len(unique_entries)
                    exclude = tuple(str(i) for i in processed_ids)

                    if exclude:
                        clause = "AND CAST(internal_id AS TEXT) NOT IN %s" if len(exclude) > 1 else "AND CAST(internal_id AS TEXT) != %s"
                        query = f"""
                            SELECT DISTINCT ON (internal_id) internal_id, hyper_reality, sreality_image_urls, bezrealitky_image_urls, idnes
                            FROM image_links
                            WHERE (local_path IS NULL OR array_length(local_path, 1) IS NULL)
                            {clause}
                            ORDER BY internal_id
                            LIMIT %s
                        """
                        params = (exclude, needed * 2) if len(exclude) > 1 else (exclude[0], needed * 2)
                    else:
                        query = """
                            SELECT DISTINCT ON (internal_id) internal_id, hyper_reality, sreality_image_urls, bezrealitky_image_urls, idnes
                            FROM image_links
                            WHERE (local_path IS NULL OR array_length(local_path, 1) IS NULL)
                            ORDER BY internal_id
                            LIMIT %s
                        """
                        params = (needed * 2,)

                    cur.execute(query, params)
                    batch = cur.fetchall()
                    if not batch:
                        logger.info("No more entries available in database")
                        break

                    for entry in batch:
                        entry_id = entry['internal_id']
                        if entry_id not in processed_ids:
                            unique_entries.append(dict(entry))
                            processed_ids.add(entry_id)
                            if len(unique_entries) >= limit:
                                break

                    if len(batch) == 0:
                        break
        except Exception as e:
            logger.error(f"Error fetching entries: {str(e)}")
            raise

        logger.info(f"Fetched {len(unique_entries)} unique entries from database")
        return unique_entries

    # ========== LOAD BATCHES FROM FILE ==========
    def load_batch_run(self, batch_file):
        """
        Load batches from file.

        Args:
            batch_file: Path to the batch file

        Returns:
            list: List of batches
        """
        if not os.path.exists(batch_file):
            logger.error(f"Batch file not found: {batch_file}")
            return None

        logger.info(f"Loading batch file: {batch_file}")
        with open(batch_file, 'r') as f:
            batches = json.load(f)
        logger.info(f"Loaded {len(batches)} batches ({sum(len(b) for b in batches)} total entries)")
        return batches

    # ========== GET RECENT BATCH FILE ==========
    def get_recent_batch_file(self):
        """
        Get the most recent batch file.

        Returns:
            str: Path to the most recent batch file
        """
        files = [f for f in os.listdir(self.batches_dir) if f.startswith("batch_run_") and f.endswith(".json")]
        if not files:
            return None
        files.sort(reverse=True)
        return os.path.join(self.batches_dir, files[0])