import os
import sys
import json
import logging
import argparse
import psycopg2
from psycopg2 import sql

# ========= LOGGING SETUP =========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("db_clear.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TableCleaner")

# ========= CONFIG CLASS =========
class Config:
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

# ========= TABLE CLEANER CLASS =========
class TableCleaner:

    def __init__(self, config_path="config.json"):
        self.config = Config(config_path)
        self.db_config = self.config.get_db_config()
        self.conn = None

    def connect(self):
        if not self.conn or self.conn.closed:
            try:
                self.conn = psycopg2.connect(
                    host=self.db_config.get("host"),
                    port=self.db_config.get("port"),
                    user=self.db_config.get("user"),
                    password=self.db_config.get("password"),
                    dbname=self.db_config.get("dbname")
                )
                logger.info("Connected to database")
            except Exception as e:
                logger.error(f"Database connection failed: {str(e)}")
                raise
        return self.conn

    def check_table_exists(self, table_name):
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables WHERE table_name = %s
                    )
                """, (table_name,))
                return cur.fetchone()[0]
        except Exception as e:
            logger.error(f"Failed to check table '{table_name}': {str(e)}")
            return False

    def check_column_exists(self, table_name, column_name):
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = %s AND column_name = %s
                    )
                """, (table_name, column_name))
                return cur.fetchone()[0]
        except Exception as e:
            logger.error(f"Failed to check column '{column_name}' in '{table_name}': {str(e)}")
            return False

    def clear_local_paths(self):
        conn = self.connect()
        if not self.check_table_exists('image_links'):
            logger.warning("Table 'image_links' does not exist. Skipping.")
            return False
        if not self.check_column_exists('image_links', 'local_path'):
            logger.warning("Column 'local_path' does not exist in 'image_links'. Skipping.")
            return False
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM image_links WHERE local_path IS NOT NULL")
                count_before = cur.fetchone()[0]
                cur.execute("UPDATE image_links SET local_path = NULL")
                conn.commit()
                logger.info(f"Cleared 'local_path' column in 'image_links' ({count_before} rows updated)")
                return True
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to clear local_path: {str(e)}")
            return False

    def clear_processed_images(self):
        conn = self.connect()
        if not self.check_table_exists('processed_images'):
            logger.warning("Table 'processed_images' does not exist. Skipping.")
            return False
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM processed_images")
                count_before = cur.fetchone()[0]
                cur.execute("DELETE FROM processed_images")
                conn.commit()
                logger.info(f"Cleared 'processed_images' table ({count_before} rows deleted)")
                return True
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to clear processed_images: {str(e)}")
            return False

    def clear_image_duplicates(self):
        conn = self.connect()
        if not self.check_table_exists('image_duplicates'):
            logger.warning("Table 'image_duplicates' does not exist. Skipping.")
            return False
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM image_duplicates")
                count_before = cur.fetchone()[0]
                cur.execute("DELETE FROM image_duplicates")
                conn.commit()
                logger.info(f"Cleared 'image_duplicates' table ({count_before} rows deleted)")
                return True
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to clear image_duplicates: {str(e)}")
            return False

    def clear_all_tables(self):
        self.clear_image_duplicates()
        self.clear_processed_images()
        self.clear_local_paths()
        logger.info("All tables cleared (image_links preserved, only local_path column reset)")
        return True

    def close(self):
        """Closes DB connection if open."""
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Database connection closed")

# ========= MAIN ROUTINE =========
def main():
    parser = argparse.ArgumentParser(description="Clear database tables for the image processor.")
    parser.add_argument("--config", default="config.json", help="Path to config file")
    parser.add_argument("--all", action="store_true", help="Clear all (processed_images, image_duplicates, local_path)")
    parser.add_argument("--local-paths", action="store_true", help="Reset local_path in image_links")
    parser.add_argument("--processed", action="store_true", help="Clear processed_images table")
    parser.add_argument("--duplicates", action="store_true", help="Clear image_duplicates table")
    parser.add_argument("--force", action="store_true", help="Skip confirmation prompt")
    args = parser.parse_args()

    if not any([args.all, args.local_paths, args.processed, args.duplicates]):
        parser.print_help()
        sys.exit(1)

    cleaner = TableCleaner(args.config)

    # Confirm with user unless --force is used
    if not args.force:
        table_list = []
        if args.all or args.local_paths:
            table_list.append("local_path in image_links")
        if args.all or args.processed:
            table_list.append("processed_images")
        if args.all or args.duplicates:
            table_list.append("image_duplicates")
        tables_str = ", ".join(table_list)
        confirm = input(f"This will clear: {tables_str}\nContinue? (y/n): ")
        if confirm.lower() != 'y':
            logger.info("Operation cancelled by user")
            return

    # Perform selected actions
    try:
        if args.all:
            cleaner.clear_all_tables()
        else:
            if args.local_paths:
                cleaner.clear_local_paths()
            if args.processed:
                cleaner.clear_processed_images()
            if args.duplicates:
                cleaner.clear_image_duplicates()
    finally:
        cleaner.close()

if __name__ == "__main__":
    main()