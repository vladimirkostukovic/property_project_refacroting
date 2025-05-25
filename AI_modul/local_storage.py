import os
import logging
import shutil

# === LOGGING SETUP ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
log = logging.getLogger("LocalStorage")

# === LOCAL STORAGE HANDLER ===
class LocalStorageHandler:
    """Local storage for test/debug mode (saves files locally, not to FTP)."""

    def __init__(self, storage_config):
        self.config = storage_config
        self.local_root = storage_config.get("local_root", "./local_images")
        os.makedirs(self.local_root, exist_ok=True)
        log.info(f"Local storage initialized: {self.local_root}")

    def connect(self):
        """Ensure local storage directory exists."""
        if not os.path.exists(self.local_root):
            os.makedirs(self.local_root, exist_ok=True)
            log.info(f"Created local storage dir: {self.local_root}")
        return True

    def ensure_directory(self, path):
        """Create subdirectory inside local_root if missing."""
        full_path = os.path.join(self.local_root, path.lstrip('/'))
        dir_path = os.path.dirname(full_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
            log.info(f"Created local dir: {dir_path}")

    def upload_image(self, source, destination):
        """Save image file locally (destination is relative to local_root)."""
        try:
            normalized_dest = destination.lstrip('/')
            full_path = os.path.join(self.local_root, normalized_dest)
            dir_path = os.path.dirname(full_path)
            if not os.path.exists(dir_path):
                os.makedirs(dir_path, exist_ok=True)
            source.seek(0)
            with open(full_path, 'wb') as f:
                shutil.copyfileobj(source, f)
            log.info(f"Saved image: {full_path}")
            return True
        except Exception as e:
            log.error(f"Failed to save image {destination}: {str(e)}")
            return False

    def close(self):
        """No connections to close for local."""
        log.debug("Local storage handler closed")