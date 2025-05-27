import os
import json
import logging
import pickle
from datetime import datetime
import psycopg2
import psycopg2.extras

# Set up logging
logger = logging.getLogger("BatchManager")


class BatchManager:
    """Manage batching of database entries for processing."""

    def __init__(self, db_handler, batch_size=50):
        """
        Initialize the batch manager.

        Args:
            db_handler: Database handler instance
            batch_size: Size of each batch
        """
        self.db_handler = db_handler
        self.batch_size = batch_size
        self.batches_dir = os.path.join(os.getcwd(), "batches")

        # Ensure batches directory exists
        os.makedirs(self.batches_dir, exist_ok=True)

    def prepare_batches(self, total_entries):
        """
        Prepare batches of entries from the database.

        Args:
            total_entries: Total number of entries to fetch

        Returns:
            tuple: (batch_file_path, total_batches, total_listings)
        """
        logger.info(f"Preparing batches for {total_entries} entries with batch size {self.batch_size}")

        # Create a timestamp for this batch run
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        batch_file = os.path.join(self.batches_dir, f"batch_run_{timestamp}.json")

        # Fetch all unique entries
        all_entries = self._fetch_all_unique_entries(total_entries)

        if not all_entries:
            logger.warning("No entries found in database for processing")
            return None, 0, 0

        # Create batches
        batches = []
        for i in range(0, len(all_entries), self.batch_size):
            batch = all_entries[i:i + self.batch_size]
            batches.append(batch)

        # Save batches to JSON file
        with open(batch_file, 'w') as f:
            json.dump(batches, f, indent=2)

        logger.info(f"Created {len(batches)} batches with {len(all_entries)} total listings")
        logger.info(f"Batches saved to {batch_file}")

        return batch_file, len(batches), len(all_entries)

    def _fetch_all_unique_entries(self, limit):
        """
        Fetch all unique entries from the database.

        Args:
            limit: Maximum number of entries to fetch

        Returns:
            list: List of unique entries
        """
        logger.info(f"Fetching {limit} unique entries from database")

        # Get all unique entries
        unique_entries = []
        processed_ids = set()

        conn = self.db_handler.connect()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                while len(unique_entries) < limit:
                    # Calculate how many more we need
                    remaining = limit - len(unique_entries)

                    # Construct exclusion clause if we have processed IDs
                    if processed_ids:
                        # Convert all IDs to strings for comparison
                        str_exclude_ids = [str(id) for id in processed_ids]

                        # Handle single ID case differently
                        if len(str_exclude_ids) == 1:
                            cur.execute("""
                                SELECT DISTINCT ON (internal_id) internal_id, hyper_reality, sreality_image_urls, bezrealitky_image_urls, idnes
                                FROM image_links
                                WHERE (local_path IS NULL OR array_length(local_path, 1) IS NULL)
                                AND CAST(internal_id AS TEXT) != %s
                                ORDER BY internal_id
                                LIMIT %s
                            """, (str_exclude_ids[0], remaining * 2))
                        else:
                            # Use NOT IN for multiple IDs
                            cur.execute("""
                                SELECT DISTINCT ON (internal_id) internal_id, hyper_reality, sreality_image_urls, bezrealitky_image_urls, idnes
                                FROM image_links
                                WHERE (local_path IS NULL OR array_length(local_path, 1) IS NULL)
                                AND CAST(internal_id AS TEXT) NOT IN %s
                                ORDER BY internal_id
                                LIMIT %s
                            """, (tuple(str_exclude_ids), remaining * 2))
                    else:
                        # No exclusions yet
                        cur.execute("""
                            SELECT DISTINCT ON (internal_id) internal_id, hyper_reality, sreality_image_urls, bezrealitky_image_urls, idnes
                            FROM image_links
                            WHERE (local_path IS NULL OR array_length(local_path, 1) IS NULL)
                            ORDER BY internal_id
                            LIMIT %s
                        """, (remaining * 2,))

                    batch = cur.fetchall()

                    if not batch:
                        logger.info("No more entries available in database")
                        break

                    # Add unique entries
                    for entry in batch:
                        entry_id = entry['internal_id']
                        if entry_id not in processed_ids:
                            unique_entries.append(dict(entry))  # Convert to regular dict for serialization
                            processed_ids.add(entry_id)

                            # Break if we've reached the limit
                            if len(unique_entries) >= limit:
                                break

                    # If this batch didn't yield any new entries, we're done
                    if len(batch) == 0:
                        break
        except Exception as e:
            logger.error(f"Error fetching entries: {str(e)}")
            raise

        logger.info(f"Fetched {len(unique_entries)} unique entries from database")
        return unique_entries

    def load_batch_run(self, batch_file):
        """
        Load a previously saved batch run.

        Args:
            batch_file: Path to the batch file

        Returns:
            list: List of batches
        """
        if not os.path.exists(batch_file):
            logger.error(f"Batch file not found: {batch_file}")
            return None

        logger.info(f"Loading batch file: {batch_file}")
        if batch_file.endswith('.json'):
            with open(batch_file, 'r') as f:
                batches = json.load(f)
        else:  # Assume it's a pickle file
            with open(batch_file, 'rb') as f:
                batches = pickle.load(f)

        logger.info(f"Loaded {len(batches)} batches with a total of {sum(len(batch) for batch in batches)} listings")
        return batches

    def get_recent_batch_file(self):
        """
        Get the most recent batch file.

        Returns:
            str: Path to the most recent batch file
        """
        json_files = [f for f in os.listdir(self.batches_dir) if f.startswith("batch_run_") and f.endswith(".json")]
        pickle_files = [f for f in os.listdir(self.batches_dir) if f.startswith("batch_run_") and f.endswith(".pkl")]

        batch_files = json_files + pickle_files

        if not batch_files:
            return None

        # Sort by timestamp (which is part of the filename)
        batch_files.sort(reverse=True)
        return os.path.join(self.batches_dir, batch_files[0])