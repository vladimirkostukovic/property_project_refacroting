import subprocess
import time
import logging
import os
import signal
import sys
import smtplib
from email.mime.text import MIMEText

# === Import config from project root ===
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import config

# === Configuration ===
MAX_RETRIES = 3
SLEEP_BETWEEN = 10  # seconds between scripts
SLEEP_ON_FAIL = 1400  # 10 minutes on hard failure

scripts = [
    "sreality.py",
    "bezrealitky.py",
    "hyper.py",
    "idnes.py"
]

# === Logging setup ===
logging.basicConfig(
    filename="etl_from_scrappers_execution.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# === Graceful shutdown ===
def graceful_exit(signum, frame):
    log.warning("Interrupted. Exiting ETL module...")
    sys.exit(1)

signal.signal(signal.SIGINT, graceful_exit)
signal.signal(signal.SIGTERM, graceful_exit)

# === Email alert ===
def send_alert(script_name, error_msg):
    subject = f"[CRITICAL] {script_name} failed after {MAX_RETRIES} retries"
    body = f"""
    Script: {script_name}
    Error: {error_msg}
    Time: {time.strftime('%Y-%m-%d %H:%M:%S')}
    """

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = config.SMTP_USER
    msg['To'] = config.ALERT_EMAIL

    try:
        with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as server:
            server.starttls()
            server.login(config.SMTP_USER, config.SMTP_PASSWORD)
            server.send_message(msg)
        log.critical(f"[ALERT SENT] Email sent for {script_name}")
    except Exception as e:
        log.error(f"[ALERT ERROR] Failed to send email alert: {e}")

# === Main execution ===
success = []
failed = []

log.info("Starting ETL pipeline (scrapers)...")

for script in scripts:
    log.info(f"Running script: {script}")
    retries = 0
    last_error = ""
    while retries < MAX_RETRIES:
        try:
            subprocess.run(["python3", script], check=True)
            log.info(f"Finished: {script}")
            success.append(script)
            break
        except subprocess.CalledProcessError as e:
            retries += 1
            last_error = str(e)
            log.warning(f"{script} failed (attempt {retries}/{MAX_RETRIES}): {last_error}")
            time.sleep(5)
    else:
        log.error(f"{script} failed after {MAX_RETRIES} retries.")
        send_alert(script, last_error)
        failed.append(script)
        log.warning(f"Sleeping for {SLEEP_ON_FAIL} seconds to avoid overload...")
        time.sleep(SLEEP_ON_FAIL)

    time.sleep(SLEEP_BETWEEN)

# === Final report ===
log.info(f"Scraping completed. Successful: {success}, Failed: {failed}")
print(f"Scraping completed.\nSuccessful: {success}\nFailed: {failed}")