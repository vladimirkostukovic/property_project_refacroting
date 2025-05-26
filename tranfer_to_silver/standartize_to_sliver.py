import pandas as pd
import logging
from sqlalchemy import create_engine, text
from datetime import date
from config import DB_URL

# === Logging ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)
engine = create_engine(DB_URL)

# === Category mapping ===
category_map = {
    'byty': (1, 'Byty'), 'apartments': (1, 'Byty'), 'byty k prodeji': (1, 'Byty'),
    'byty k pronájmu': (1, 'Byty'), 'byty k pronajmu': (1, 'Byty'),
    'domy': (2, 'Domy'), 'houses': (2, 'Domy'), 'rodinné domy': (2, 'Domy'),
    'chaty a chalupy': (2, 'Domy'), 'novostavby': (2, 'Domy'),
    'pozemky': (3, 'Pozemky'), 'land': (3, 'Pozemky'),
    'pozemky, zahrady a historické stavby': (3, 'Pozemky'),
    'komerční': (4, 'Komerční'), 'komercni': (4, 'Komerční'), 'commercial': (4, 'Komerční'),
    'komerční objekty': (4, 'Komerční'),
    'ostatní': (5, 'Ostatní'), 'ostatni': (5, 'Ostatní'),
    'ostatní nemovitosti': (5, 'Ostatní'), 'garages': (5, 'Ostatní'),
    'garáže a drobné objekты': (5, 'Ostatní'), 'garáže': (5, 'Ostatní'),
    'nemovitosti v zahraničí': (5, 'Ostatní'), 'prodej': (5, 'Ostatní'),
}

def map_category(name):
    if not isinstance(name, str):
        return (5, 'Ostatní')
    key = name.strip().lower()
    for k in category_map:
        if k in key:
            return category_map[k]
    return (5, 'Ostatní')

# === Load source table ===
log.info("Loading public.standartize...")
df = pd.read_sql("SELECT * FROM public.standartize", con=engine)

# === Transform ===
df[['category_value', 'category_name']] = df['category_name'].apply(lambda x: pd.Series(map_category(x)))
for col in ['added_date', 'archived_date']:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col]).dt.date
df['house_type'] = df['category_value']

required_columns = [
    'internal_id', 'site_id', 'added_date', 'avalaible', 'archived_date', 'source_id',
    'category_value', 'category_name', 'name', 'deal_type', 'price', 'rooms', 'area_build',
    'area_land', 'house_type', 'district', 'city', 'city_part', 'street', 'house_number',
    'longitude', 'latitude', 'district_id', 'municipality_id', 'region_id', 'street_id'
]
df = df[required_columns]

# === Load silver.standartize_silver ===
log.info("Loading silver.standartize_silver...")
df_silver = pd.read_sql("SELECT internal_id, avalaible, archived_date FROM silver.standartize_silver", con=engine)

# === Load silver.standartize_geo ===
log.info("Loading silver.standartize_geo...")
df_geo = pd.read_sql("SELECT internal_id, avalaible, archived_date FROM silver.standartize_geo", con=engine)

# === Detect new & changed for silver ===
new_mask_silver = ~df['internal_id'].isin(df_silver['internal_id'])
df_new_silver = df[new_mask_silver].copy()

merged_silver = df.merge(df_silver, on='internal_id', how='inner', suffixes=('', '_old'))
changed_mask_silver = (
    (merged_silver['avalaible'] != merged_silver['avalaible_old']) |
    (merged_silver['archived_date'] != merged_silver['archived_date_old'])
)
df_changed_silver = merged_silver[changed_mask_silver].copy()

# === Detect new & changed for geo ===
new_mask_geo = ~df['internal_id'].isin(df_geo['internal_id'])
df_new_geo = df[new_mask_geo].copy()

merged_geo = df.merge(df_geo, on='internal_id', how='inner', suffixes=('', '_old'))
changed_mask_geo = (
    (merged_geo['avalaible'] != merged_geo['avalaible_old']) |
    (merged_geo['archived_date'] != merged_geo['archived_date_old'])
)
df_changed_geo = merged_geo[changed_mask_geo].copy()

# === Insert new to silver ===
if not df_new_silver.empty:
    df_new_silver.to_sql('standartize_silver', con=engine, schema='silver', if_exists='append', index=False)
    log.info(f"Inserted {len(df_new_silver)} rows into silver.standartize_silver.")

# === Update existing in silver ===
if not df_changed_silver.empty:
    temp = 'tmp_silver_updates'
    df_changed_silver[['internal_id', 'avalaible', 'archived_date']].to_sql(temp, con=engine, schema='silver', if_exists='replace', index=False)
    update_sql = f"""
        UPDATE silver.standartize_silver t
        SET
            avalaible = tmp.avalaible,
            archived_date = tmp.archived_date
        FROM silver.{temp} tmp
        WHERE t.internal_id = tmp.internal_id
    """
    with engine.begin() as conn:
        conn.execute(text(update_sql))
        conn.execute(text(f"DROP TABLE silver.{temp}"))
    log.info(f"Updated {len(df_changed_silver)} rows in silver.standartize_silver.")

# === Insert new to geo ===
if not df_new_geo.empty:
    df_new_geo.to_sql('standartize_geo', con=engine, schema='silver', if_exists='append', index=False)
    log.info(f"Inserted {len(df_new_geo)} rows into silver.standartize_geo.")

# === Update existing in geo ===
if not df_changed_geo.empty:
    temp_geo = 'tmp_geo_updates'
    df_changed_geo[['internal_id', 'avalaible', 'archived_date']].to_sql(temp_geo, con=engine, schema='silver', if_exists='replace', index=False)
    update_sql_geo = f"""
        UPDATE silver.standartize_geo g
        SET
            avalaible = tmp.avalaible,
            archived_date = tmp.archived_date
        FROM silver.{temp_geo} tmp
        WHERE g.internal_id = tmp.internal_id
    """
    with engine.begin() as conn:
        conn.execute(text(update_sql_geo))
        conn.execute(text(f"DROP TABLE silver.{temp_geo}"))
    log.info(f"Updated {len(df_changed_geo)} rows in silver.standartize_geo.")

# === Final log ===
print("=" * 60)
print(f"Sync summary — {date.today()}")
print()
print("Table: silver.standartize_silver")
print(f"  Rows added   : {len(df_new_silver)}")
print(f"  Rows updated : {len(df_changed_silver)}")
print()
print("Table: silver.standartize_geo")
print(f"  Rows added   : {len(df_new_geo)}")
print(f"  Rows updated : {len(df_changed_geo)}")
print("=" * 60)