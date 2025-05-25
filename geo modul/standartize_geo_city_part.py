import pandas as pd
from sqlalchemy import create_engine
import unidecode
import logging
from rapidfuzz import process, fuzz
import psycopg2.extras
from config import DB_URL

# === Logging setup ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

# === Database connection ===
engine = create_engine(DB_URL)

# === Load only rows needing normalization ===
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

# === Step 1: Exact matching of city_part with city ===
log.info("Starting exact city_part/city matching...")

norm_city_part_list = []
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
            try:
                true_index = list(allowed_parts).index(city_part_norm)
                norm_city_part = city_to_city_parts_true[city_norm][true_index]
            except Exception:
                norm_city_part = city_part_norm
            matched_count += 1

    norm_city_part_list.append(norm_city_part)

geo['norm_city_part'] = norm_city_part_list

total = len(geo)
unmatched_count = total - matched_count

log.info(f"Total rows processed: {total}")
log.info(f"Total matched by exact city_part/city: {matched_count}")
log.info(f"Total unmatched: {unmatched_count}")

# === Step 2: Fuzzy matching for unmatched ===
FUZZY_THRESHOLD = 90
fuzzy_matched = []
still_unmatched = []

for idx, row in geo[geo['norm_city_part'].isna()].iterrows():
    city_norm = row['city_norm']
    city_part_norm = row['city_part_norm']
    if not city_norm or not city_part_norm:
        still_unmatched.append((idx, row['city'], row['city_part']))
        continue

    options = ruian.loc[ruian['nazev_obce_norm'] == city_norm, 'nazev_casti_obce_norm'].dropna().unique().tolist()
    if not options:
        still_unmatched.append((idx, row['city'], row['city_part']))
        continue

    match = process.extractOne(city_part_norm, options, scorer=fuzz.ratio)
    if match and match[1] >= FUZZY_THRESHOLD:
        try:
            matched_original = ruian[
                (ruian['nazev_obce_norm'] == city_norm) &
                (ruian['nazev_casti_obce_norm'] == match[0])
            ]['nazev_casti_obce'].iloc[0]
        except Exception:
            matched_original = match[0]
        geo.at[idx, 'norm_city_part'] = matched_original
        fuzzy_matched.append((idx, row['city'], row['city_part'], matched_original, match[1]))
    else:
        still_unmatched.append((idx, row['city'], row['city_part']))

log.info(f"Fuzzy matched city_part (threshold {FUZZY_THRESHOLD}): {len(fuzzy_matched)}")
log.info(f"Still unmatched after fuzzy pass: {len(still_unmatched)}")

# === Final statistics ===
final_matched = geo['norm_city_part'].notna().sum()
final_unmatched = geo['norm_city_part'].isna().sum()
log.info(f"FINAL matched city_part: {final_matched}")
log.info(f"FINAL unmatched city_part: {final_unmatched}")
log.info("Sample matched rows (up to 10):")
print(geo.loc[geo['norm_city_part'].notna(), ['internal_id', 'city', 'city_part', 'norm_city_part']].head(10).to_string())
log.info("Sample unmatched rows (up to 10):")
print(geo.loc[geo['norm_city_part'].isna(), ['internal_id', 'city', 'city_part', 'norm_city_part']].head(10).to_string())

# === Batch update norm_city_part in DB ===
def batch_update_norm_city_part(df, engine, batch_size=1000):
    conn = engine.raw_connection()
    cursor = conn.cursor()
    update_sql = """
        UPDATE silver.standartize_geo AS s
        SET norm_city_part = data.norm_city_part
        FROM (VALUES %s) AS data(internal_id, norm_city_part)
        WHERE s.internal_id = data.internal_id
    """
    data_tuples = [(int(row.internal_id), row.norm_city_part) for row in df.itertuples() if row.norm_city_part is not None]
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