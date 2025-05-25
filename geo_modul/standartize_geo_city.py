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
log.info("Loading only rows with region not null and norm_city IS NULL...")
geo = pd.read_sql(
    """
    SELECT internal_id, district, city, city_part, street, region, name
    FROM silver.standartize_geo
    WHERE region IS NOT NULL AND norm_city IS NULL
    """,
    con=engine
)
log.info(f"Loaded {len(geo)} rows for normalization")

# ruian table stays the same
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

geo['district_norm'] = geo['district'].apply(norm_string)
geo['city_norm'] = geo['city'].apply(norm_string)
geo['city_part_norm'] = geo['city_part'].apply(norm_string)

# === Step 1: Match by district ===
geo['norm_city'] = None
total = len(geo)
log.info(f"Step 1: Matching city by 'district' (total: {total})")

district_to_city = dict(ruian[['nazev_obce_norm', 'nazev_obce']].drop_duplicates().values)
matched_1 = 0

for idx, row in geo.iterrows():
    d = row['district_norm']
    if d and d in district_to_city:
        geo.at[idx, 'norm_city'] = district_to_city[d]
        matched_1 += 1

log.info(f"Step 1 complete: matched {matched_1} rows by district")
remaining = geo['norm_city'].isna().sum()
log.info(f"Unmatched after Step 1: {remaining}")

# === Step 2: Match by city ===
mask = geo['norm_city'].isna()
log.info(f"Step 2: Matching city by 'city' (left: {mask.sum()})")
matched_2 = 0

for idx in geo[mask].index:
    c = geo.at[idx, 'city_norm']
    if c and c in district_to_city:
        geo.at[idx, 'norm_city'] = district_to_city[c]
        matched_2 += 1

log.info(f"Step 2 complete: matched {matched_2} rows by city")
remaining = geo['norm_city'].isna().sum()
log.info(f"Unmatched after Step 2: {remaining}")

# === Step 3: Match by city_part ===
mask = geo['norm_city'].isna()
log.info(f"Step 3: Matching city by 'city_part' (left: {mask.sum()})")
city_part_to_city = dict(ruian[['nazev_casti_obce_norm', 'nazev_obce']].drop_duplicates().values)
matched_3 = 0

for idx in geo[mask].index:
    cp = geo.at[idx, 'city_part_norm']
    if cp and cp in city_part_to_city:
        geo.at[idx, 'norm_city'] = city_part_to_city[cp]
        matched_3 += 1

log.info(f"Step 3 complete: matched {matched_3} rows by city_part")
remaining = geo['norm_city'].isna().sum()
log.info(f"Unmatched after Step 3: {remaining}")

# === Step 4: Fuzzy match by district ===
mask = geo['norm_city'].isna()
log.info(f"Step 4: Fuzzy match city by 'district' (left: {mask.sum()})")

matched_4 = 0
district_keys = list(district_to_city.keys())

for idx in geo[mask].index:
    d = geo.at[idx, 'district_norm']
    if d:
        match = process.extractOne(d, district_keys, scorer=fuzz.ratio)
        if match and match[1] > 90:
            geo.at[idx, 'norm_city'] = district_to_city[match[0]]
            matched_4 += 1

log.info(f"Step 4 complete: fuzzy matched {matched_4} rows by district")
remaining = geo['norm_city'].isna().sum()
log.info(f"Unmatched after Step 4: {remaining}")

# === Step 5: Fuzzy match by city ===
mask = geo['norm_city'].isna()
log.info(f"Step 5: Fuzzy match city by 'city' (left: {mask.sum()})")

matched_5 = 0
for idx in geo[mask].index:
    c = geo.at[idx, 'city_norm']
    if c:
        match = process.extractOne(c, district_keys, scorer=fuzz.ratio)
        if match and match[1] > 90:
            geo.at[idx, 'norm_city'] = district_to_city[match[0]]
            matched_5 += 1

log.info(f"Step 5 complete: fuzzy matched {matched_5} rows by city")
remaining = geo['norm_city'].isna().sum()
log.info(f"Unmatched after Step 5: {remaining}")

# === Step 6: Fuzzy match by city_part ===
mask = geo['norm_city'].isna()
log.info(f"Step 6: Fuzzy match city by 'city_part' (left: {mask.sum()})")

matched_6 = 0
city_part_keys = list(city_part_to_city.keys())

for idx in geo[mask].index:
    cp = geo.at[idx, 'city_part_norm']
    if cp:
        match = process.extractOne(cp, city_part_keys, scorer=fuzz.ratio)
        if match and match[1] > 90:
            geo.at[idx, 'norm_city'] = city_part_to_city[match[0]]
            matched_6 += 1

log.info(f"Step 6 complete: fuzzy matched {matched_6} rows by city_part")
remaining = geo['norm_city'].isna().sum()
log.info(f"Unmatched after Step 6: {remaining}")

# === Step 7: Extract city from 'name' using fuzzy matching ===
mask = geo['norm_city'].isna()
log.info(f"Step 7: Extract city from 'name' (left: {mask.sum()})")

ruian_cities = list(set(ruian['nazev_obce_norm'].dropna()))
matched_7 = 0

for idx in geo[mask].index:
    name = geo.at[idx, 'name']
    if not isinstance(name, str):
        continue
    name_norm = unidecode.unidecode(name).lower()
    match = process.extractOne(name_norm, ruian_cities, scorer=fuzz.partial_ratio)
    if match and match[1] > 85:
        city_matched = district_to_city.get(match[0])
        if city_matched:
            geo.at[idx, 'norm_city'] = city_matched
            matched_7 += 1

log.info(f"Step 7 complete: matched {matched_7} rows from name")
remaining = geo['norm_city'].isna().sum()
log.info(f"Unmatched after Step 7: {remaining}")

# === Final statistics ===
matched_total = matched_1 + matched_2 + matched_3 + matched_4 + matched_5 + matched_6 + matched_7
log.info("========== NORMALIZATION STATISTICS ==========")
log.info(f"Total rows processed: {total}")
log.info(f"Matched by district: {matched_1}")
log.info(f"Matched by city: {matched_2}")
log.info(f"Matched by city_part: {matched_3}")
log.info(f"Fuzzy matched by district: {matched_4}")
log.info(f"Fuzzy matched by city: {matched_5}")
log.info(f"Fuzzy matched by city_part: {matched_6}")
log.info(f"Matched by name: {matched_7}")
log.info(f"TOTAL matched: {matched_total}")
log.info(f"TOTAL unmatched: {remaining}")
log.info("===============================================")

# === Batch update in DB ===

def batch_update_norm_city(df, engine, batch_size=1000):
    import psycopg2.extras
    conn = engine.raw_connection()
    cursor = conn.cursor()
    update_sql = """
        UPDATE silver.standartize_geo
        SET norm_city = data.norm_city
        FROM (VALUES %s) AS data(internal_id, norm_city)
        WHERE silver.standartize_geo.internal_id = data.internal_id
    """
    data_tuples = [(int(row.internal_id), row.norm_city) for row in df.itertuples() if row.norm_city is not None]
    for i in range(0, len(data_tuples), batch_size):
        batch = data_tuples[i:i+batch_size]
        psycopg2.extras.execute_values(cursor, update_sql, batch, template=None, page_size=batch_size)
        conn.commit()
    cursor.close()
    conn.close()

# Run update
to_update = geo.loc[geo['norm_city'].notna(), ['internal_id', 'norm_city']]
log.info(f"Updating {len(to_update)} rows in database...")
batch_update_norm_city(to_update, engine)
log.info("DB update complete.")