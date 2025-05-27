import os
import csv
import json
import logging
import threading
from datetime import datetime

# Set up logging
logger = logging.getLogger("FailedDownloadsTracker")


class FailedDownloadsTracker:
    """Track failed image downloads with details for later analysis or retry."""

    def __init__(self, log_dir="./logs"):
        self.log_dir = log_dir
        self.lock = threading.Lock()  # Thread-safe file writing

        # Ensure log directory exists
        os.makedirs(log_dir, exist_ok=True)

        # Create a timestamped filename for this session
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.csv_file = os.path.join(log_dir, f"failed_downloads_{timestamp}.csv")
        self.json_file = os.path.join(log_dir, f"failed_downloads_{timestamp}.json")

        # Initialize CSV file with headers
        with open(self.csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'internal_id', 'url', 'source_column', 'error_message'])

        # Initialize JSON file with empty list
        with open(self.json_file, 'w') as f:
            json.dump([], f)

        logger.info(f"Failed downloads will be tracked in {self.csv_file} and {self.json_file}")

    def record_failure(self, internal_id, url, source_column, error_message):
        """
        Record a failed download with details.

        Args:
            internal_id (str): Internal ID of the listing
            url (str): URL that failed to download
            source_column (str): Source column from the database
            error_message (str): Error message explaining the failure
        """
        timestamp = datetime.now().isoformat()

        # Ensure internal_id is a string
        internal_id_str = str(internal_id)

        # Create record
        record = {
            'timestamp': timestamp,
            'internal_id': internal_id_str,
            'url': url,
            'source_column': source_column,
            'error_message': error_message
        }

        # Thread-safe file writing
        with self.lock:
            # Append to CSV
            try:
                with open(self.csv_file, 'a', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        timestamp,
                        internal_id_str,
                        url,
                        source_column,
                        error_message
                    ])
            except Exception as e:
                logger.error(f"Failed to write to CSV file: {str(e)}")

            # Append to JSON
            try:
                # Read current data
                with open(self.json_file, 'r') as f:
                    try:
                        data = json.load(f)
                    except json.JSONDecodeError:
                        # If file is empty or malformed, start with empty list
                        data = []

                # Append new record
                data.append(record)

                # Write back to file
                with open(self.json_file, 'w') as f:
                    json.dump(data, f, indent=2)
            except Exception as e:
                logger.error(f"Failed to write to JSON file: {str(e)}")

        logger.info(f"Recorded failed download: [ID:{internal_id_str}] {url}")

    def get_failure_count(self):
        """Get the total number of recorded failures."""
        try:
            with open(self.json_file, 'r') as f:
                try:
                    data = json.load(f)
                    return len(data)
                except json.JSONDecodeError:
                    return 0
        except FileNotFoundError:
            return 0

    def get_failures_by_internal_id(self, internal_id):
        """Get all failures for a specific internal ID."""
        try:
            with open(self.json_file, 'r') as f:
                try:
                    data = json.load(f)
                    return [record for record in data if record['internal_id'] == internal_id]
                except json.JSONDecodeError:
                    return []
        except FileNotFoundError:
            return []