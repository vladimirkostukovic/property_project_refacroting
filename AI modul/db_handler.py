import logging
import json
import psycopg2
import psycopg2.extras

# === LOGGING SETUP ===
logger = logging.getLogger("DBHandler")

# === DATABASE HANDLER ===
class DatabaseHandler:
    def __init__(self, db_config):
        self.config = db_config
        self.conn = None

    # === CONNECTION ===
    def connect(self):
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

    # === SCHEMA MIGRATION ===
    def ensure_schema(self):
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = 'image_links'
                        AND column_name IN ('image_hashes', 'duplicate_ids')
                """)
                columns = {row[0]: row[1] for row in cur.fetchall()}

                # image_hashes column
                if 'image_hashes' not in columns:
                    logger.info("Adding image_hashes (JSONB) to image_links")
                    cur.execute("ALTER TABLE image_links ADD COLUMN image_hashes JSONB DEFAULT NULL")
                elif columns['image_hashes'] != 'jsonb':
                    logger.info(f"Converting image_hashes from {columns['image_hashes']} to JSONB")
                    cur.execute("ALTER TABLE image_links ALTER COLUMN image_hashes TYPE JSONB USING image_hashes::jsonb")

                # duplicate_ids column
                if 'duplicate_ids' not in columns:
                    logger.info("Adding duplicate_ids (JSONB) to image_links")
                    cur.execute("ALTER TABLE image_links ADD COLUMN duplicate_ids JSONB DEFAULT NULL")
                elif columns['duplicate_ids'] != 'jsonb':
                    logger.info(f"Converting duplicate_ids from {columns['duplicate_ids']} to JSONB")
                    cur.execute("ALTER TABLE image_links ALTER COLUMN duplicate_ids TYPE JSONB USING duplicate_ids::jsonb")

                conn.commit()
                logger.info("Schema check complete")
        except Exception as e:
            conn.rollback()
            logger.error(f"Schema check failed: {str(e)}")
            raise

    # === GET LISTINGS ===
    def get_all_listings_for_processing(self):
        conn = self.connect()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
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
            logger.error(f"Failed to get listings: {str(e)}")
            return []

    # === UPDATE HASHES ===
    def update_listing_hashes(self, internal_id, hashes_by_source):
        conn = self.connect()
        try:
            hash_json = json.dumps({
                src: {str(pos): hashv for pos, hashv in vals}
                for src, vals in hashes_by_source.items()
            })
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE image_links SET image_hashes = %s
                    WHERE internal_id = %s
                """, (hash_json, internal_id))
                conn.commit()
                updated = cur.rowcount
            if updated:
                logger.debug(f"Updated hashes for listing {internal_id}")
                return True
            else:
                logger.warning(f"No rows updated for listing {internal_id}")
                return False
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to update hashes for {internal_id}: {str(e)}")
            return False

    # === UPDATE DUPLICATES ===
    def update_listing_duplicates(self, internal_id, duplicate_ids):
        conn = self.connect()
        try:
            dups_json = json.dumps(duplicate_ids) if duplicate_ids else None
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE image_links SET duplicate_ids = %s
                    WHERE internal_id = %s
                """, (dups_json, internal_id))
                conn.commit()
                updated = cur.rowcount
            if updated:
                logger.debug(f"Updated duplicates for listing {internal_id}")
                return True
            else:
                logger.warning(f"No rows updated for listing {internal_id}")
                return False
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to update duplicates for {internal_id}: {str(e)}")
            return False

    # === BATCH UPDATE ===
    def batch_update_listings(self, hash_updates, duplicate_updates):
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                updated = 0
                for internal_id, hashes in hash_updates.items():
                    try:
                        hash_json = json.dumps(hashes)
                        cur.execute("""
                            UPDATE image_links SET image_hashes = %s::jsonb
                            WHERE internal_id = %s
                        """, (hash_json, internal_id))
                        if cur.rowcount > 0:
                            updated += 1
                    except Exception as e:
                        logger.error(f"Failed to update hashes for {internal_id}: {str(e)}")
                for internal_id, dups in duplicate_updates.items():
                    try:
                        dups_json = json.dumps(dups) if dups else None
                        if dups_json:
                            cur.execute("""
                                UPDATE image_links SET duplicate_ids = %s::jsonb
                                WHERE internal_id = %s
                            """, (dups_json, internal_id))
                    except Exception as e:
                        logger.error(f"Failed to update duplicates for {internal_id}: {str(e)}")
                conn.commit()
                logger.info(f"Batch updated {updated} listings")
                return updated
        except Exception as e:
            conn.rollback()
            logger.error(f"Batch update failed: {str(e)}")
            return 0

    # === CLOSE CONNECTION ===
    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Database connection closed")