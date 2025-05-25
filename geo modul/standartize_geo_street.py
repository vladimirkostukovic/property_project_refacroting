import pandas as pd
from sqlalchemy import create_engine
import unidecode
import logging
from rapidfuzz import process, fuzz
from tqdm import tqdm
import psycopg2.extras
from config import DB_URL

# === Setup logging ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

# === DB Connection ===
engine = create_engine(DB_URL)

# === Load tables into RAM ===
log.info("Loading reference and target tables...")

ruian = pd.read_sql_table(
    'ruian_ulice',
    schema='silver',
    con=engine,
    columns=['nazev_obce', 'nazev_casti_obce', 'nazev_ulice', 'kraj']
)
log.info(f"Loaded ruian_ulice: {len(ruian)} rows")

# Load only rows where norm_ulice is NULL (for efficient batch)
geo = pd.read_sql(
    """
    SELECT internal_id, district, city, city_part, street, region, name, norm_city, norm_ulice
    FROM silver.standartize_geo
    WHERE norm_ulice IS NULL
    """,
    con=engine
)
log.info(f"Loaded {len(geo)} rows from standartize_geo for normalization")

# === Normalize helper ===
def norm_string(x):
    if pd.isna(x):
        return ""
    return unidecode.unidecode(str(x)).strip().lower()

ruian['nazev_obce_norm'] = ruian['nazev_obce'].apply(norm_string)
ruian['nazev_ulice_norm'] = ruian['nazev_ulice'].apply(norm_string)

geo['district_norm'] = geo['district'].apply(norm_string)
geo['city_norm'] = geo['city'].apply(norm_string)
geo['city_part_norm'] = geo['city_part'].apply(norm_string)
geo['street_norm'] = geo['street'].apply(norm_string)
geo['norm_city_norm'] = geo['norm_city'].apply(norm_string)

# === Step 1: Strict matching ===
log.info("Step 1: Strict matching (city + street, city_part, district)...")
tqdm.pandas(desc="Strict")

ruian_pairs = set(zip(ruian['nazev_obce_norm'], ruian['nazev_ulice_norm']))
norm_ulice_list = []
strict_matched = 0

for idx, row in tqdm(geo.iterrows(), total=len(geo), desc="Strict"):
    norm_city = row['norm_city_norm']
    if not norm_city:
        norm_ulice_list.append(None)
        continue

    found = False
    # Try to match by street, city_part, city, district in order
    for field in ['street_norm', 'city_part_norm', 'city_norm', 'district_norm']:
        street_val = row[field]
        if street_val and (norm_city, street_val) in ruian_pairs:
            orig_ulice = ruian.loc[
                (ruian['nazev_obce_norm'] == norm_city) &
                (ruian['nazev_ulice_norm'] == street_val),
                'nazev_ulice'
            ]
            norm_ulice_list.append(orig_ulice.iloc[0] if not orig_ulice.empty else None)
            strict_matched += 1
            found = True
            break
    if not found:
        norm_ulice_list.append(None)

geo['norm_ulice'] = norm_ulice_list

log.info(f"Strict matched {strict_matched} rows")
not_matched = geo['norm_ulice'].isna().sum()
log.info(f"Proceeding to fuzzy match, unmatched count: {not_matched}")

# === Step 2: Fuzzy matching for not matched ===
ruian_ulice_dict = ruian.groupby('nazev_obce_norm')['nazev_ulice_norm'].apply(list).to_dict()
ruian_ulice_orig = ruian.groupby('nazev_obce_norm')['nazev_ulice'].apply(list).to_dict()

fuzzy_matched = 0
for idx, row in tqdm(geo[geo['norm_ulice'].isna()].iterrows(), total=not_matched, desc="Fuzzy"):
    norm_city = row['norm_city_norm']
    if not norm_city:
        continue
    candidates = ruian_ulice_dict.get(norm_city, [])
    orig_names = ruian_ulice_orig.get(norm_city, [])
    if not candidates:
        continue
    # Find the best fuzzy score among fields
    best_score = 0
    best_idx = None
    for field in ['street_norm', 'city_part_norm', 'city_norm', 'district_norm']:
        val = row[field]
        if val:
            match = process.extractOne(val, candidates, scorer=fuzz.ratio)
            if match and match[1] > best_score:
                best_score = match[1]
                best_idx = candidates.index(match[0]) if match[0] in candidates else None
    if best_idx is not None and best_score > 89:
        geo.at[idx, 'norm_ulice'] = orig_names[best_idx]
        fuzzy_matched += 1

# === Summary statistics ===
total_matched = geo['norm_ulice'].notna().sum()
log.info("======= ULICE MATCH STATISTICS =======")
log.info(f"Total rows: {len(geo)}")
log.info(f"Matched: {total_matched}")
log.info(f"Not matched: {len(geo) - total_matched}")

log.info("Sample matched rows (up to 10):")
print(geo.loc[geo['norm_ulice'].notna(), ['internal_id', 'norm_city', 'norm_ulice']].head(10).to_string())

log.info("Sample unmatched rows (up to 10):")
print(geo.loc[geo['norm_ulice'].isna(), ['internal_id', 'norm_city', 'street', 'city_part', 'city', 'district']].head(10).to_string())

log.info("Top norm_ulice values:")
print(geo['norm_ulice'].value_counts(dropna=False).head(20))

# === Batch update norm_ulice in DB ===
def batch_update_norm_ulice(df, engine, batch_size=1000):
    conn = engine.raw_connection()
    cursor = conn.cursor()
    update_sql = """
        UPDATE silver.standartize_geo AS s
        SET norm_ulice = data.norm_ulice
        FROM (VALUES %s) AS data(internal_id, norm_ulice)
        WHERE s.internal_id = data.internal_id
    """
    data_tuples = [(int(row.internal_id), row.norm_ulice) for row in df.itertuples() if row.norm_ulice is not None]
    for i in range(0, len(data_tuples), batch_size):
        batch = data_tuples[i:i+batch_size]
        psycopg2.extras.execute_values(cursor, update_sql, batch, template=None, page_size=batch_size)
        conn.commit()
    cursor.close()
    conn.close()

to_update = geo.loc[geo['norm_ulice'].notna(), ['internal_id', 'norm_ulice']]
log.info(f"Updating {len(to_update)} rows with norm_ulice in database...")
batch_update_norm_ulice(to_update, engine)
log.info("DB update complete.")