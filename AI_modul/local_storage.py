import os
import logging
import shutil

# Set up logging
logger = logging.getLogger("LocalStorage")


class LocalStorageHandler:
    """Handle local storage operations for test mode."""

    def __init__(self, storage_config):
        self.config = storage_config
        self.local_root = storage_config.get("local_root", "./local_images")

        # Ensure the root directory exists
        os.makedirs(self.local_root, exist_ok=True)
        logger.info(f"Local storage initialized at: {self.local_root}")

    def connect(self):
        """Local equivalent of connect - ensure directory exists."""
        # Check if directory exists, create if not
        if not os.path.exists(self.local_root):
            os.makedirs(self.local_root, exist_ok=True)
            logger.info(f"Created local storage directory: {self.local_root}")
        return True

    def ensure_directory(self, path):
        """Ensure the directory exists on the local filesystem."""
        # Create full path by joining with root
        full_path = os.path.join(self.local_root, path.lstrip('/'))
        dir_path = os.path.dirname(full_path)

        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
            logger.info(f"Created local directory: {dir_path}")

    def upload_image(self, source, destination):
        """Save an image to the local filesystem."""
        try:
            # Normalize the destination path - strip leading slash
            normalized_dest = destination.lstrip('/')

            # Create full path by joining with root
            full_path = os.path.join(self.local_root, normalized_dest)

            # Ensure directory exists
            dir_path = os.path.dirname(full_path)
            if not os.path.exists(dir_path):
                os.makedirs(dir_path, exist_ok=True)

            # Reset file pointer to beginning
            source.seek(0)

            # Open the destination file and copy the source
            with open(full_path, 'wb') as f:
                shutil.copyfileobj(source, f)

            logger.info(f"Saved image to local path: {full_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to save image to {destination}: {str(e)}")
            return False

    def close(self):
        """No connections to close for local storage."""
        logger.debug("Local storage handler closed")