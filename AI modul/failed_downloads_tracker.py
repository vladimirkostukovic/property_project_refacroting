import os
import csv
import json
import logging
import threading
from datetime import datetime

# === LOGGING SETUP ===
logger = logging.getLogger("FailedDownloadsTracker")

# === FAILED DOWNLOADS TRACKER ===
class FailedDownloadsTracker:
    """Tracks failed image downloads for later analysis or retry."""

    def __init__(self, log_dir="./logs"):
        self.log_dir = log_dir
        self.lock = threading.Lock()
        os.makedirs(log_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.csv_file = os.path.join(log_dir, f"failed_downloads_{ts}.csv")
        self.json_file = os.path.join(log_dir, f"failed_downloads_{ts}.json")

        # --- INIT CSV ---
        with open(self.csv_file, 'w', newline='') as f:
            csv.writer(f).writerow(['timestamp', 'internal_id', 'url', 'source_column', 'error_message'])
        # --- INIT JSON ---
        with open(self.json_file, 'w') as f:
            json.dump([], f)

        logger.info(f"Tracking failures in {self.csv_file} and {self.json_file}")

    # === RECORD FAILURE ===
    def record_failure(self, internal_id, url, source_column, error_message):
        ts = datetime.now().isoformat()
        record = {
            'timestamp': ts,
            'internal_id': str(internal_id),
            'url': url,
            'source_column': source_column,
            'error_message': error_message
        }
        with self.lock:
            # --- CSV ---
            try:
                with open(self.csv_file, 'a', newline='') as f:
                    csv.writer(f).writerow([ts, record['internal_id'], url, source_column, error_message])
            except Exception as e:
                logger.error(f"CSV write failed: {e}")
            # --- JSON ---
            try:
                data = []
                if os.path.getsize(self.json_file) > 0:
                    with open(self.json_file, 'r') as f:
                        try:
                            data = json.load(f)
                        except Exception:
                            data = []
                data.append(record)
                with open(self.json_file, 'w') as f:
                    json.dump(data, f, indent=2)
            except Exception as e:
                logger.error(f"JSON write failed: {e}")
        logger.info(f"Recorded failed download: [ID:{record['internal_id']}] {url}")

    # === GET COUNT ===
    def get_failure_count(self):
        try:
            with open(self.json_file, 'r') as f:
                data = json.load(f)
                return len(data)
        except Exception:
            return 0

    # === GET BY ID ===
    def get_failures_by_internal_id(self, internal_id):
        try:
            with open(self.json_file, 'r') as f:
                data = json.load(f)
                return [r for r in data if r['internal_id'] == str(internal_id)]
        except Exception:
            return []