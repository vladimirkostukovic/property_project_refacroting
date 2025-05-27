import logging
import json
import psycopg2
import psycopg2.extras
from datetime import datetime

# Set up logging
logger = logging.getLogger("DBHandler")


class DatabaseHandler:
    """Database handler for simplified image processing workflow."""

    def __init__(self, db_config):
        self.config = db_config
        self.conn = None
        self.ensure_schema()

    def connect(self):
        """Establish a database connection."""
        if self.conn is None or self.conn.closed:
            logger.info("Connecting to database...")
            try:
                self.conn = psycopg2.connect(
                    host=self.config.get("host"),
                    port=self.config.get("port"),
                    user=self.config.get("user"),
                    password=self.config.get("password"),
                    dbname=self.config.get("dbname")
                )
                logger.info("Database connection established")
            except Exception as e:
                logger.error(f"Database connection failed: {str(e)}")
                raise
        return self.conn

    def ensure_schema(self):
        """Ensure the necessary columns exist in the image_links table with correct types."""
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                # Check if columns exist
                cur.execute("""
                    SELECT column_name, data_type FROM information_schema.columns 
                    WHERE table_name = 'image_links' AND 
                    column_name IN ('image_hashes', 'duplicate_ids')
                """)
                columns = {row[0]: row[1] for row in cur.fetchall()}

                # Add or modify image_hashes column
                if 'image_hashes' not in columns:
                    logger.info("Adding image_hashes column to image_links table as JSONB")
                    cur.execute("ALTER TABLE image_links ADD COLUMN image_hashes JSONB DEFAULT NULL")
                elif columns.get('image_hashes') != 'jsonb':
                    # If column exists but is wrong type, drop and recreate
                    logger.info(f"Converting image_hashes column from {columns.get('image_hashes')} to JSONB")
                    cur.execute("ALTER TABLE image_links ALTER COLUMN image_hashes TYPE JSONB USING NULL")

                # Add or modify duplicate_ids column
                if 'duplicate_ids' not in columns:
                    logger.info("Adding duplicate_ids column to image_links table as JSONB")
                    cur.execute("ALTER TABLE image_links ADD COLUMN duplicate_ids JSONB DEFAULT NULL")
                elif columns.get('duplicate_ids') != 'jsonb':
                    # If column exists but is wrong type, drop and recreate
                    logger.info(f"Converting duplicate_ids column from {columns.get('duplicate_ids')} to JSONB")
                    cur.execute("ALTER TABLE image_links ALTER COLUMN duplicate_ids TYPE JSONB USING NULL")

                conn.commit()
                logger.info("Schema verification complete")
                return True
        except Exception as e:
            conn.rollback()
            logger.error(f"Schema verification failed: {str(e)}")
            raise

    def get_all_listings_for_processing(self):
        """
        Get all listings that need processing (have no hashes yet).

        Returns:
            list: List of dictionaries with listing data
        """
        conn = self.connect()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                # Get entries that don't have hashes yet
                cur.execute("""
                    SELECT internal_id, hyper_reality, sreality_image_urls, bezrealitky_image_urls, idnes
                    FROM image_links
                    WHERE image_hashes IS NULL
                    ORDER BY internal_id
                """)

                results = cur.fetchall()
                logger.info(f"Found {len(results)} listings without hashes")
                return results
        except Exception as e:
            logger.error(f"Failed to get listings for processing: {str(e)}")
            return []

    def update_listing_hashes(self, internal_id, hashes_by_source):
        """
        Update a listing with hashes.

        Args:
            internal_id: Internal ID of the listing
            hashes_by_source: Dictionary of source -> list of (position, hash) tuples

        Returns:
            bool: True if update successful
        """
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                # Convert hashes to serializable format
                hash_array = {}
                for source, hashes in hashes_by_source.items():
                    source_array = {}
                    for pos, hash_value in hashes:
                        source_array[str(pos)] = hash_value
                    hash_array[source] = source_array

                # Convert to JSON string
                hash_json = json.dumps(hash_array)

                # Update database
                cur.execute("""
                    UPDATE image_links
                    SET image_hashes = %s
                    WHERE internal_id = %s
                """, (hash_json, internal_id))

                conn.commit()

                if cur.rowcount > 0:
                    logger.debug(f"Updated hashes for listing {internal_id}")
                    return True
                else:
                    logger.warning(f"No rows updated for listing {internal_id}")
                    return False
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to update hashes for listing {internal_id}: {str(e)}")
            return False

    def update_listing_duplicates(self, internal_id, duplicate_ids):
        """
        Update a listing with duplicate IDs.

        Args:
            internal_id: Internal ID of the listing
            duplicate_ids: List of duplicate internal IDs

        Returns:
            bool: True if update successful
        """
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                # Convert to JSON string
                dups_json = json.dumps(duplicate_ids) if duplicate_ids else None

                # Update database
                cur.execute("""
                    UPDATE image_links
                    SET duplicate_ids = %s
                    WHERE internal_id = %s
                """, (dups_json, internal_id))

                conn.commit()

                if cur.rowcount > 0:
                    logger.debug(f"Updated duplicates for listing {internal_id}")
                    return True
                else:
                    logger.warning(f"No rows updated for listing {internal_id}")
                    return False
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to update duplicates for listing {internal_id}: {str(e)}")
            return False

    def batch_update_listings(self, hash_updates, duplicate_updates):
        """
        Update multiple listings with hashes and duplicates in bulk.

        Args:
            hash_updates: Dictionary of internal_id -> hash data
            duplicate_updates: Dictionary of internal_id -> duplicate IDs

        Returns:
            int: Number of listings updated
        """
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                # Track successful updates
                success_count = 0

                # Use JSONB instead of array
                for internal_id, hashes in hash_updates.items():
                    try:
                        # Convert the hash data to a proper JSON object
                        # It should be in the format {"source": {"position": "hash"}}
                        hash_json = json.dumps(hashes)

                        # Update database using JSONB
                        cur.execute("""
                            UPDATE image_links
                            SET image_hashes = %s::jsonb
                            WHERE internal_id = %s
                        """, (hash_json, internal_id))

                        if cur.rowcount > 0:
                            success_count += 1
                    except Exception as e:
                        logger.error(f"Failed to update hashes for listing {internal_id}: {str(e)}")

                # Then update duplicates using JSONB
                for internal_id, duplicates in duplicate_updates.items():
                    try:
                        # Convert to JSON string
                        dups_json = json.dumps(duplicates) if duplicates else None

                        # Update database using JSONB
                        if dups_json:
                            cur.execute("""
                                UPDATE image_links
                                SET duplicate_ids = %s::jsonb
                                WHERE internal_id = %s
                            """, (dups_json, internal_id))
                    except Exception as e:
                        logger.error(f"Failed to update duplicates for listing {internal_id}: {str(e)}")

                conn.commit()
                logger.info(f"Batch updated {success_count} listings in database")
                return success_count
        except Exception as e:
            conn.rollback()
            logger.error(f"Batch update failed: {str(e)}")
            return 0

    def close(self):
        """Close the database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Database connection closed")