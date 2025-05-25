import logging
from sqlalchemy import create_engine, text
from config import DB_URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

engine = create_engine(DB_URL)

with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS standartize_geo (LIKE standartize_silver INCLUDING ALL)
    """))
    for col in ["region", "norm_city", "norm_city_part", "norm_street"]:
        conn.execute(text(f"ALTER TABLE standartize_geo ADD COLUMN IF NOT EXISTS {col} TEXT;"))
    log.info("Table 'standartize_geo' checked and updated.")