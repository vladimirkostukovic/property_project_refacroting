#!/usr/bin/env python3
"""
Script to visualize duplicate image entries.
Shows all entries that have duplicate relationships.
"""

import os
import sys
import json
import logging
import argparse
import redis
import psycopg2
import psycopg2.extras
from tabulate import tabulate
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("DuplicateViewer")


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

    def get_redis_config(self):
        return self.config.get("redis", {})


class DuplicateViewer:
    """View duplicate image entries with details."""

    def __init__(self, config_path="config.json"):
        self.config = Config(config_path)
        self.db_config = self.config.get_db_config()
        self.redis_config = self.config.get_redis_config()
        self.conn = None
        self.r = None
        self.redis_prefix = self.redis_config.get("prefix", "imgproc:")

    def redis_key(self, name):
        """Create a prefixed key for Redis."""
        return f"{self.redis_prefix}{name}"

    def connect_db(self):
        """Establish a database connection."""
        if self.conn is None or self.conn.closed:
            logger.info("Connecting to database...")
            try:
                self.conn = psycopg2.connect(
                    host=self.db_config.get("host"),
                    port=self.db_config.get("port"),
                    user=self.db_config.get("user"),
                    password=self.db_config.get("password"),
                    dbname=self.db_config.get("dbname")
                )
                logger.info("Database connection established")
            except Exception as e:
                logger.error(f"Database connection failed: {str(e)}")
                raise
        return self.conn

    def connect_redis(self):
        """Establish a Redis connection."""
        if self.r is None:
            logger.info("Connecting to Redis...")
            try:
                self.r = redis.Redis(
                    host=self.redis_config.get("host", "localhost"),
                    port=self.redis_config.get("port", 6379),
                    db=self.redis_config.get("db", 0),
                    password=self.redis_config.get("password", None),
                    decode_responses=True
                )
                logger.info("Redis connection established")
            except Exception as e:
                logger.error(f"Redis connection failed: {str(e)}")
                raise
        return self.r

    def get_duplicates_from_redis(self):
        """Get all duplicate relationships from Redis."""
        r = self.connect_redis()

        # Find all processed entries
        processed_entries = r.smembers(self.redis_key("processed_entries"))
        logger.info(f"Found {len(processed_entries)} processed entries in Redis")

        # Get duplicates for each entry
        duplicates = {}
        dup_count = 0

        for entry_id in processed_entries:
            # Get hashes for this entry
            entry_hashes = r.smembers(self.redis_key(f"hashes:{entry_id}"))

            # Find all duplicates for this entry
            entry_dups = set()

            # Check all hash values for this entry
            for hash_val in entry_hashes:
                # Get URL data for this hash
                hash_url_data = r.hgetall(self.redis_key(f"hash_url:{hash_val}"))

                if not hash_url_data:
                    continue

                # Find other entries with similar hashes
                similar_entries = set()

                # This would be from the deduplication process
                # In our case, if deduplication was run with ANN,
                # we can check if this entry has duplicates

                # In a real-world scenario, we'd have stored these relationships
                # Here we'll examine entries with similar hash values

                # Since we might not have these stored, we'll use an approximation
                # by comparing with other hashes (this is a simplified approach)

                # In a production environment, the duplicates would be stored
                # in a structured way from the deduplication process

            # For now, let's try to get duplicates from the database
            db_dups = self.get_duplicates_from_db(entry_id)
            if db_dups:
                duplicates[entry_id] = db_dups
                dup_count += len(db_dups)

        logger.info(f"Found {dup_count} duplicate relationships across {len(duplicates)} entries")
        return duplicates

    def get_duplicates_from_db(self, internal_id=None):
        """Get duplicates from the database."""
        conn = self.connect_db()

        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                if internal_id:
                    # Get duplicates for a specific entry
                    cur.execute("""
                        SELECT duplicate_ids 
                        FROM image_links 
                        WHERE internal_id = %s AND duplicate_ids IS NOT NULL
                    """, (internal_id,))

                    result = cur.fetchone()
                    if result and result['duplicate_ids']:
                        return result['duplicate_ids']
                    return []
                else:
                    # Get all entries with duplicates
                    cur.execute("""
                        SELECT internal_id, duplicate_ids 
                        FROM image_links 
                        WHERE duplicate_ids IS NOT NULL AND 
                              duplicate_ids != 'null'::jsonb AND
                              duplicate_ids != '[]'::jsonb
                    """)

                    results = cur.fetchall()
                    duplicates = {}

                    for row in results:
                        if row['duplicate_ids']:
                            duplicates[str(row['internal_id'])] = row['duplicate_ids']

                    logger.info(f"Found {len(duplicates)} entries with duplicates in database")
                    return duplicates
        except Exception as e:
            logger.error(f"Error getting duplicates from database: {str(e)}")
            return {}

    def get_entry_details(self, internal_id):
        """Get details for a specific entry."""
        conn = self.connect_db()

        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                    SELECT internal_id, hyper_reality, sreality_image_urls, 
                           bezrealitky_image_urls, idnes, local_path, image_hashes
                    FROM image_links 
                    WHERE internal_id = %s
                """, (internal_id,))

                result = cur.fetchone()
                if result:
                    return dict(result)
                return None
        except Exception as e:
            logger.error(f"Error getting entry details: {str(e)}")
            return None

    def extract_urls_from_entry(self, entry):
        """Extract all URLs from an entry."""
        urls = []

        # Check each possible source column
        for column in ['hyper_reality', 'sreality_image_urls', 'bezrealitky_image_urls', 'idnes']:
            if entry[column]:
                try:
                    # Try to parse as JSON
                    if isinstance(entry[column], str):
                        column_urls = json.loads(entry[column])
                    else:
                        column_urls = entry[column]

                    # Handle lists or single values
                    if isinstance(column_urls, list):
                        urls.extend([(column, url) for url in column_urls])
                    else:
                        urls.append((column, column_urls))
                except:
                    # Not JSON, treat as a single URL
                    urls.append((column, entry[column]))

        return urls

    def get_local_paths(self, entry):
        """Get local paths from an entry."""
        paths = []

        if entry['local_path']:
            try:
                # Try to parse as JSON if it's a string
                if isinstance(entry['local_path'], str):
                    local_paths = json.loads(entry['local_path'])
                else:
                    local_paths = entry['local_path']

                # Handle lists or single values
                if isinstance(local_paths, list):
                    paths.extend(local_paths)
                else:
                    paths.append(local_paths)
            except:
                # Not JSON, treat as a single path
                paths.append(entry['local_path'])

        return paths

    def view_duplicates(self, output_file=None, limit=None):
        """View all entries with duplicates."""
        # Get duplicates from database
        all_duplicates = self.get_duplicates_from_db()

        if not all_duplicates:
            logger.info("No duplicates found")
            return

        # Apply limit if specified
        if limit and limit < len(all_duplicates):
            logger.info(f"Limiting output to {limit} entries")
            # Convert to list, slice, then back to dict
            entries = list(all_duplicates.items())[:limit]
            all_duplicates = dict(entries)

        # Prepare output
        output = []
        output.append(f"Duplicate Images Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        output.append(f"Found {len(all_duplicates)} entries with duplicates\n")

        # Process each entry with duplicates
        for idx, (entry_id, dup_ids) in enumerate(all_duplicates.items(), 1):
            # Get details for this entry
            entry = self.get_entry_details(entry_id)

            if not entry:
                logger.warning(f"Entry {entry_id} not found in database")
                continue

            # Get URLs for this entry
            urls = self.extract_urls_from_entry(entry)
            local_paths = self.get_local_paths(entry)

            # Add entry header
            output.append(f"\n{'=' * 80}")
            output.append(f"Entry {idx}: Internal ID {entry_id} - Has {len(dup_ids)} duplicates")
            output.append(f"{'-' * 80}")

            # Add URLs
            output.append("URLs:")
            for idx, (source, url) in enumerate(urls, 1):
                output.append(f"  {idx}. [{source}] {url}")

            # Add local paths
            if local_paths:
                output.append("\nLocal Paths:")
                for idx, path in enumerate(local_paths, 1):
                    output.append(f"  {idx}. {path}")

            # Process duplicates
            output.append(f"\nDuplicate Entries:")

            for dup_idx, dup_id in enumerate(dup_ids, 1):
                dup_entry = self.get_entry_details(dup_id)

                if not dup_entry:
                    output.append(f"  {dup_idx}. Duplicate ID {dup_id} - Not found in database")
                    continue

                # Get URLs for duplicate
                dup_urls = self.extract_urls_from_entry(dup_entry)
                dup_local_paths = self.get_local_paths(dup_entry)

                output.append(f"  {dup_idx}. Duplicate ID {dup_id}")

                # Add duplicate URLs
                if dup_urls:
                    output.append("     URLs:")
                    for url_idx, (source, url) in enumerate(dup_urls, 1):
                        output.append(f"       {url_idx}. [{source}] {url}")

                # Add duplicate local paths
                if dup_local_paths:
                    output.append("     Local Paths:")
                    for path_idx, path in enumerate(dup_local_paths, 1):
                        output.append(f"       {path_idx}. {path}")

        # Print to console
        for line in output:
            print(line)

        # Save to file if requested
        if output_file:
            with open(output_file, 'w') as f:
                for line in output:
                    f.write(line + '\n')
            logger.info(f"Output saved to {output_file}")

    def close(self):
        """Close database and Redis connections."""
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Database connection closed")

        if self.r:
            # Redis connections are pooled and managed automatically
            logger.info("Redis connection released")


def main():
    parser = argparse.ArgumentParser(description="View duplicate image entries with details")
    parser.add_argument("--config", default="config.json", help="Path to configuration file")
    parser.add_argument("--output", help="Output file path")
    parser.add_argument("--limit", type=int, help="Limit the number of entries to show")

    args = parser.parse_args()

    viewer = DuplicateViewer(args.config)

    try:
        viewer.view_duplicates(args.output, args.limit)
    finally:
        viewer.close()


if __name__ == "__main__":
    main()