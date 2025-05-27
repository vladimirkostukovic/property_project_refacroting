import os
import sys
from dotenv import load_dotenv
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_URL

import pandas as pd
from sqlalchemy import create_engine
import unidecode
import logging
from rapidfuzz import process, fuzz
import psycopg2.extras

# === Setup ===
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)
engine = create_engine(DB_URL)

# === Load data ===
log.info("Loading only rows where norm_city_part is NULL...")
geo = pd.read_sql(
    """
    SELECT internal_id, district, city, city_part, street, region, name, norm_city_part
    FROM silver.standartize_geo
    WHERE norm_city_part IS NULL
    """,
    con=engine
)
log.info(f"Loaded {len(geo)} rows for normalization")

ruian = pd.read_sql_table(
    'ruian_ulice',
    schema='silver',
    con=engine,
    columns=['nazev_obce', 'nazev_casti_obce', 'nazev_ulice', 'kraj']
)
log.info(f"Loaded ruian_ulice: {len(ruian)} rows")

# === Normalize helper ===
def norm_string(x):
    if pd.isna(x):
        return ""
    return unidecode.unidecode(str(x)).strip().lower()

ruian['nazev_obce_norm'] = ruian['nazev_obce'].apply(norm_string)
ruian['nazev_casti_obce_norm'] = ruian['nazev_casti_obce'].apply(norm_string)
geo['city_norm'] = geo['city'].apply(norm_string)
geo['city_part_norm'] = geo['city_part'].apply(norm_string)

# === Step 1: Exact match ===
log.info("Starting exact city_part/city matching...")

matches = []
matched_count = 0

city_to_city_parts = ruian.groupby('nazev_obce_norm')['nazev_casti_obce_norm'].apply(set).to_dict()
city_to_city_parts_true = ruian.groupby('nazev_obce_norm')['nazev_casti_obce'].apply(list).to_dict()

for idx, row in geo.iterrows():
    city_norm = row['city_norm']
    city_part_norm = row['city_part_norm']
    norm_city_part = None

    if city_norm and city_part_norm:
        allowed_parts = city_to_city_parts.get(city_norm, set())
        if city_part_norm in allowed_parts:
            true_parts = city_to_city_parts_true[city_norm]
            for true_part in true_parts:
                if norm_string(true_part) == city_part_norm:
                    norm_city_part = true_part
                    matched_count += 1
                    break

    matches.append({
        "internal_id": row["internal_id"],
        "norm_city_part": norm_city_part
    })

exact_matches_df = pd.DataFrame(matches).dropna(subset=["internal_id"])
exact_matches_df.set_index("internal_id", inplace=True)
geo.set_index("internal_id", inplace=True)
geo.update(exact_matches_df[["norm_city_part"]])
geo.reset_index(inplace=True)

# === Stats ===
total = len(geo)
unmatched_count = total - matched_count
log.info(f"Total rows processed: {total}")
log.info(f"Total matched by exact city_part/city: {matched_count}")
log.info(f"Total unmatched: {unmatched_count}")

# === Batch update ===
def batch_update_norm_city_part(df, engine, batch_size=1000):
    conn = engine.raw_connection()
    cursor = conn.cursor()
    update_sql = """
        UPDATE silver.standartize_geo AS s
        SET norm_city_part = data.norm_city_part
        FROM (VALUES %s) AS data(internal_id, norm_city_part)
        WHERE s.internal_id = data.internal_id
    """

    data_tuples = [
        (int(row.internal_id), row.norm_city_part)
        for row in df.itertuples()
        if pd.notna(row.internal_id) and row.norm_city_part is not None
    ]

    if not data_tuples:
        log.info("No rows to update.")
        return

    for i in range(0, len(data_tuples), batch_size):
        batch = data_tuples[i:i+batch_size]
        psycopg2.extras.execute_values(cursor, update_sql, batch, template=None, page_size=batch_size)
        conn.commit()

    cursor.close()
    conn.close()

to_update = geo.loc[geo['norm_city_part'].notna(), ['internal_id', 'norm_city_part']]
log.info(f"Updating {len(to_update)} rows with norm_city_part in database...")
batch_update_norm_city_part(to_update, engine)
log.info("DB update complete.")