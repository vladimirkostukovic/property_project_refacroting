import re
import unidecode
import pandas as pd
from rapidfuzz import fuzz, process
from sqlalchemy import create_engine, text
import logging
from config import DB_URL

# === Setup logging ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

# === Constants ===
ADM_STOPWORDS = {'okres', 'kraj', 'ul', 'ulice', 'město', 'města', 'obec', 'část', 'cast'}
FUZZY_THRESHOLD = 90
TEMP_REGION_TABLE = "tmp_region_update"

# === HARDCODED city2region ===
HARDCODED_CITY2REGION = {
    'kladno': 'Středočeský kraj',
    'hodonin': 'Jihomoravský kraj',
    'pisek': 'Jihočeský kraj',
    'písek': 'Jihočeský kraj',
    'benesov': 'Středočeský kraj',
    'benešov': 'Středočeský kraj',
    'tachov': 'Plzeňský kraj',
    'frydek': 'Moravskoslezský kraj',
    'frýdek': 'Moravskoslezský kraj',
}

def match_hardcoded_city(city):
    c = unidecode.unidecode(str(city)).lower().strip()
    return HARDCODED_CITY2REGION.get(c)

# === HARDCODED district2region ===
DISTRICT2REGION = {
    # Add your district-region pairs as needed
}

def match_district_to_region(district):
    c = unidecode.unidecode(str(district)).lower().strip()
    return DISTRICT2REGION.get(c)

def clean_text(s: str) -> str:
    if pd.isna(s) or not s:
        return ""
    txt = unidecode.unidecode(str(s)).lower().strip()
    if txt.startswith('praha'):
        if 'praha-vychod' in txt or 'praha-zapad' in txt:
            return 'stredocesky kraj'
        if re.search(r'praha\s*\d+', txt):
            return 'hl.m. praha'
        return 'hl.m. praha'
    suffix_stop = {'město','jih','sever','západ','východ','střed','centrum','města'}
    if '-' in txt:
        pref, suf = txt.split('-',1)
        if suf.split()[0] in suffix_stop:
            txt = pref
    txt = re.sub(r"[^0-9a-z\s]", " ", txt)
    tokens = [t for t in txt.split() if t not in ADM_STOPWORDS]
    return " ".join(tokens).strip()

def apply_temp_update(conn, df: pd.DataFrame, table_name: str, target_col: str):
    conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
    df.to_sql(table_name, con=conn, index=False)
    conn.execute(text(f"""
        UPDATE silver.standartize_geo s
           SET {target_col} = u.{target_col}
          FROM {table_name} u
         WHERE s.internal_id = u.internal_id
    """))
    conn.execute(text(f"DROP TABLE {table_name}"))

if __name__ == '__main__':
    engine = create_engine(DB_URL)
    log.info("Loading tables into memory...")

    ruian = pd.read_sql_table('ruian_ulice', schema='silver', con=engine,
                              columns=['nazev_obce','nazev_casti_obce','nazev_ulice','kraj'])
    amb_exc = pd.read_sql_table('ambiguous_cities_with_kraje', schema='silver', con=engine)
    geo = pd.read_sql_table('standartize_geo', schema='silver', con=engine)

    GARBAGE_COLUMNS = list(geo.columns)
    log.info(f"Detected columns for geo/garbage: {GARBAGE_COLUMNS}")

    ruian['kraj_clean'] = ruian['kraj'].apply(clean_text)
    kraj_map = ruian[['kraj_clean','kraj']].drop_duplicates().set_index('kraj_clean')['kraj'].to_dict()
    ruian['city_clean'] = ruian['nazev_obce'].apply(clean_text)
    city_counts = ruian.groupby('city_clean')['kraj'].nunique()
    ambiguous = set(city_counts[city_counts > 1].index)
    ambiguous |= set(amb_exc['nazev_obce'].apply(clean_text))
    city_map = (ruian[~ruian['city_clean'].isin(ambiguous)]
                .drop_duplicates('city_clean')
                .set_index('city_clean')['kraj']
                .to_dict())

    log.info("Start assigning regions (step 1: by district, step 2: by city if not found)")

    def assign_region_by_column(row, column, rownum=None):
        orig = row[column]
        c = clean_text(orig)
        rid = row['internal_id']
        if not c:
            log.info(f"[{rid}] {column}: EMPTY after cleaning ('{orig}')")
            return None

        district_region = match_district_to_region(orig) if column == 'district' else None
        if district_region:
            log.info(f"[{rid}] {column}: DISTRICT2REGION '{orig}' => '{district_region}'")
            return district_region

        hardcoded = match_hardcoded_city(orig)
        if hardcoded:
            log.info(f"[{rid}] {column}: HARDCODED city2region '{orig}' => '{hardcoded}'")
            return hardcoded

        log.info(f"[{rid}] {column}: trying '{orig}' -> '{c}'")
        m = process.extractOne(c, list(kraj_map), scorer=fuzz.ratio)
        if m and m[1] >= FUZZY_THRESHOLD:
            log.info(f"[{rid}] {column}: FUZZY kraj match '{c}' -> '{m[0]}' (score={m[1]}) => {kraj_map[m[0]]}")
            return kraj_map[m[0]]
        else:
            log.info(f"[{rid}] {column}: No fuzzy kraj match (best: '{m[0]}' score {m[1]})" if m else f"[{rid}] {column}: No fuzzy kraj match at all")
        if c in city_map:
            log.info(f"[{rid}] {column}: Exact city_map match '{c}' => {city_map[c]}")
            return city_map[c]
        sub = ruian[ruian['nazev_obce'].str.lower() == str(orig).lower()]
        if not sub.empty:
            ks = sub['kraj'].unique()
            if len(ks) == 1:
                log.info(f"[{rid}] {column}: Unique RUIAN match for nazev_obce='{orig}' => {ks[0]}")
                return ks[0]
            part = row['city_part'] if 'city_part' in row else None
            if part and not pd.isna(part):
                sub2 = sub[sub['nazev_casti_obce'].str.lower() == str(part).lower()]
                ks2 = sub2['kraj'].unique()
                if len(ks2) == 1:
                    log.info(f"[{rid}] {column}: Unique RUIAN match for nazev_obce='{orig}', nazev_casti_obce='{part}' => {ks2[0]}")
                    return ks2[0]
            street = row['street'] if 'street' in row else None
            if street and not pd.isna(street):
                sub3 = sub[sub['nazev_ulice'].str.lower() == str(street).lower()]
                ks3 = sub3['kraj'].unique()
                if len(ks3) == 1:
                    log.info(f"[{rid}] {column}: Unique RUIAN match for nazev_obce='{orig}', nazev_ulice='{street}' => {ks3[0]}")
                    return ks3[0]
            log.info(f"[{rid}] {column}: Ambiguous RUIAN match for nazev_obce='{orig}' (no unique kraj)")
        if orig and isinstance(orig, str):
            low = orig.lower()
            if low.startswith('praha'):
                if 'praha-vychod' in low or 'praha-zapad' in low:
                    log.info(f"[{rid}] {column}: PRAHA-VYCHOD/ZAPAD => Středočeský kraj")
                    return 'Středočeský kraj'
                log.info(f"[{rid}] {column}: PRAHA => Hl.m. Praha")
                return 'Hl.m. Praha'
        log.info(f"[{rid}] {column}: Region NOT FOUND")
        return None

    geo['new_region'] = None
    mask_null_region = geo['region'].isna()
    if mask_null_region.any():
        geo.loc[mask_null_region, 'new_region'] = geo[mask_null_region].apply(
            lambda row: assign_region_by_column(row, 'district'), axis=1
        )

    mask_no_region_found = geo['region'].isna() & geo['new_region'].isna()
    if mask_no_region_found.any():
        log.info(f"Assigning regions from city for {mask_no_region_found.sum()} remaining rows...")
        geo.loc[mask_no_region_found, 'new_region'] = geo[mask_no_region_found].apply(
            lambda row: assign_region_by_column(row, 'city'), axis=1
        )

    mask_name_extract = geo['region'].isna() & geo['district'].isna()
    if mask_name_extract.any():
        log.info(f"Extracting region from name for {mask_name_extract.sum()} rows...")
        ruian_cities = list(set([unidecode.unidecode(str(x)).lower().strip() for x in ruian['nazev_obce'].dropna()]))

        def extract_city_from_name(name):
            if not isinstance(name, str) or not name:
                return None
            txt = unidecode.unidecode(name).lower()
            match = process.extractOne(txt, ruian_cities, scorer=fuzz.partial_ratio)
            if match and match[1] >= 85:
                return match[0]
            return None

        geo.loc[mask_name_extract, 'city_from_name'] = geo.loc[mask_name_extract, 'name'].apply(extract_city_from_name)

        def get_region_by_city(city):
            if not city:
                return None
            match_row = ruian[ruian['nazev_obce'].apply(
                lambda x: unidecode.unidecode(str(x)).lower().strip() == city)]
            if not match_row.empty:
                return match_row.iloc[0]['kraj']
            hardcoded = HARDCODED_CITY2REGION.get(city)
            if hardcoded:
                return hardcoded
            return None

        geo.loc[mask_name_extract, 'region_from_name'] = geo.loc[mask_name_extract, 'city_from_name'].apply(get_region_by_city)

        to_update3 = geo.loc[geo['region'].isna() & geo['region_from_name'].notna(),
                             ['internal_id', 'region_from_name']]
        updates3 = to_update3.rename(columns={'region_from_name': 'region'})
        log.info(f"Rows to update from name field: {len(updates3)}")
        if not updates3.empty:
            with engine.begin() as conn:
                apply_temp_update(conn, updates3, TEMP_REGION_TABLE, "region")
            log.info("Regions updated from name field.")

    to_update = geo.loc[geo['region'].isna() & geo['new_region'].notna(),
                        ['internal_id','new_region']]
    updates = to_update.rename(columns={'new_region':'region'})
    log.info(f"Rows to update: {len(updates)}")

    if not updates.empty:
        with engine.begin() as conn:
            apply_temp_update(conn, updates, TEMP_REGION_TABLE, "region")
        log.info("Regions updated.")
    else:
        log.info("No updates needed.")

    create_garbage_table_sql = f"""
    CREATE TABLE IF NOT EXISTS silver.listings_garbage_geo AS
    SELECT {', '.join(GARBAGE_COLUMNS)}
    FROM silver.standartize_geo
    WHERE 1=0;
    """
    with engine.begin() as conn:
        conn.execute(text(create_garbage_table_sql))
        log.info("Ensured listings_garbage_geo table exists.")

    garbage_mask = (
        geo['region'].isna() &
        geo['district'].isna() &
        geo['city'].isna() &
        (geo['region_from_name'].isna() if 'region_from_name' in geo.columns else True)
    )
    garbage = geo.loc[garbage_mask, GARBAGE_COLUMNS].copy()
    log.info(f"Found {len(garbage)} unresolved listings for garbage table.")

    if not garbage.empty:
        garbage.to_sql('listings_garbage_geo', con=engine, schema='silver', if_exists='append', index=False)
        log.info(f"Inserted {len(garbage)} rows into listings_garbage_geo.")

        delete_sql = """
        DELETE FROM silver.standartize_geo s
        USING silver.listings_garbage_geo g
        WHERE s.internal_id = g.internal_id
        """
        with engine.begin() as conn:
            result = conn.execute(text(delete_sql))
            log.info(f"Deleted {result.rowcount} garbage rows from standartize_geo.")
    else:
        log.info("No unresolved geo listings to transfer or delete.")

    log.info("Done.")