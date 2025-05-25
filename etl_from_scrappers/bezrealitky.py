import os
from dotenv import load_dotenv
import pandas as pd
import re
import logging
from datetime import date, timedelta
from sqlalchemy import create_engine, text
from config import DB_URL

# ========== ENV LOADING ==========
load_dotenv()
# ========== LOGGING SETUP ==========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

def stat_row(label, value):
    print(f"{label:<50}: {value}")

print("="*70)
print("BZEREALITY ETL SCRIPT: Listings Loading".center(70))
print("="*70)

# ========== DB CONNECTION ==========
log.info("1. Connecting to database...")
engine = create_engine(DB_URL)
# ========== GET LATEST SOURCE TABLE ==========
log.info("2. Finding the latest bzereality_lastdate table...")
with engine.connect() as conn:
    table_name = conn.execute(text("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name LIKE 'bzereality\\_%' ESCAPE '\\'
          AND table_name ~ '^bzereality_[0-9]{8}$'
        ORDER BY table_name DESC LIMIT 1
    """)).scalar()
    if not table_name:
        raise ValueError("No bzereality_lastdate table found")
stat_row("Latest input table", table_name)

# ========== LOAD & PROCESS DATA ==========
log.info("3. Loading all data into RAM...")
today = date.today()
source_id = 2

df = pd.read_sql(f"SELECT * FROM public.{table_name}", con=engine)
df['site_id'] = df['id'].astype(str)
df['source_id'] = source_id
df['added_date'] = pd.to_datetime(today)
df['archived_date'] = pd.NaT
df['avalaible'] = True
df['deal_type'] = df['offer_type'].str.split().str[0].str.lower()
df['name'] = df['image_alt_text']
df['estate_type_clean'] = df['estate_type'].str.lower()

log.info("   → Listings loaded: %d", len(df))

# --- Categories ---
map_types = {
    'pozemek': (3, 'Pozemky'),
    'byt': (1, 'Byty'),
    'dum': (2, 'Domy'),
    'kancelar': (4, 'Komerční'),
    'nebytovy_prostor': (4, 'Komerční'),
    'garaz': (5, 'Ostatní')
}
df[['category_value', 'category_name']] = df['estate_type_clean'].apply(lambda x: pd.Series(map_types.get(x, (None, 'Neznámá'))))

# --- Parse rooms, area, etc. (your logic preserved) ---
def parse_disposition(disp, alt):
    if pd.notna(disp):
        return disp.replace('DISP_', '').replace('_', '+').lower()
    m = re.search(r'([1-6][+](?:kk|[1-6]))', str(alt).lower())
    return m.group(1) if m else None

df['rooms'] = df.apply(lambda x: parse_disposition(x['disposition'], x['image_alt_text']), axis=1)
df['area_build'] = df['image_alt_text'].str.extract(r'([0-9]+(?:[.,][0-9]+)?)\s*(?:m2|m²|m\^2)')[0].str.replace(',', '.').astype(float)

def parse_area_land(row):
    text = str(row['image_alt_text']).lower()
    if row['estate_type_clean'] == 'dum':
        match = re.search(r'pozemek\s+([0-9]+(?:[.,][0-9]+)?)', text)
    elif row['estate_type_clean'] == 'pozemek':
        match = re.search(r'([0-9]+(?:[.,][0-9]+)?)\s*(?:m2|m²|m\^2)', text)
    else:
        return None
    return float(match.group(1).replace(',', '.')) if match else None

df['area_land'] = df.apply(parse_area_land, axis=1)

df['address'] = df['address'].fillna(',,')
df['street'] = df['address'].str.split(',').str[0].str.strip()
df['city_raw'] = df['address'].str.split(',').str[1].str.strip()
df['district'] = df['address'].str.split(',').str[2].str.strip()
df['city'] = df['address'].apply(lambda x: 'Praha' if 'Praha' in x else x.split(',')[1].strip())
df['city_part'] = df['city_raw'].where(df['city_raw'].str.contains('-', na=False), None).str.split('-').str[1].str.strip()
for col in ['house_number', 'longitude', 'latitude', 'district_id', 'municipality_id', 'region_id', 'street_id']:
    df[col] = None
df['house_type'] = df['estate_type_clean']

# ========== LOAD EXISTING (standartize) ==========
log.info("4. Loading current listings from standartize...")
existing_ids = pd.read_sql(
    "SELECT site_id FROM public.standartize WHERE archived_date IS NULL AND source_id = 2", con=engine)
existing_count = len(existing_ids)
stat_row("Active listings (current)", existing_count)

# ========== NEW / ARCHIVED LOGIC ==========
log.info("5. Determining new and archived listings...")
new_ids = df[~df['site_id'].isin(existing_ids['site_id'])].copy()
archived_ids = existing_ids[~existing_ids['site_id'].isin(df['site_id'])].copy()
stat_row("New listings to add", len(new_ids))
stat_row("Listings to archive", len(archived_ids))

cols = [
    'site_id', 'added_date', 'avalaible', 'archived_date', 'source_id',
    'category_value', 'category_name', 'name', 'deal_type', 'price',
    'rooms', 'area_build', 'area_land', 'house_type',
    'district', 'city', 'city_part', 'street',
    'house_number', 'longitude', 'latitude',
    'district_id', 'municipality_id', 'region_id', 'street_id'
]
for c in cols:
    if c not in new_ids.columns:
        new_ids[c] = None
df_final = new_ids[cols]

# ========== BULK INSERT NEW + ARCHIVE ==========
with engine.begin() as conn:
    if not df_final.empty:
        log.info(f"  → Adding {len(df_final)} new listings...")
        df_final.to_sql('standartize', con=conn, schema='public', if_exists='append', index=False)
    if not archived_ids.empty:
        log.info(f"  → Archiving {len(archived_ids)} listings...")
        conn.execute(
            text("""
                UPDATE public.standartize
                SET archived_date = :today, avalaible = FALSE
                WHERE site_id = ANY(:ids) AND source_id = 2 AND archived_date IS NULL
            """),
            {'today': today, 'ids': archived_ids['site_id'].tolist()}
        )

# ========== RELOAD site_id ↔ internal_id MAPPING ==========
log.info("6. Reload mapping for new listings...")
all_map = pd.read_sql(
    "SELECT site_id, internal_id FROM public.standartize WHERE source_id = 2", con=engine
)

# ========== SELLER INFO (DE-DUP & APPEND) ==========
seller_cols = ['agent_name', 'agent_phone', 'agent_email', 'site_id']
seller_new = pd.DataFrame()
if all(col in df.columns for col in seller_cols):
    log.info("7. Matching sellers with internal_id...")
    seller_df = df[seller_cols].dropna().copy()
    seller_df = pd.merge(seller_df, all_map, on='site_id', how='left')
    seller_df['added_date'] = today
    seller_df = seller_df[seller_df['internal_id'].notnull()]
    seller_existing = pd.read_sql("SELECT internal_id, agent_email FROM public.new_seller_info", con=engine)
    seller_new = pd.merge(
        seller_df,
        seller_existing,
        on=['internal_id', 'agent_email'],
        how='left',
        indicator=True
    )
    seller_new = seller_new[seller_new['_merge'] == 'left_only'].drop(columns=['_merge'])
stat_row("New sellers to add", seller_new.shape[0] if not seller_new.empty else 0)

if not seller_new.empty:
    with engine.begin() as conn:
        log.info(f"  → Adding {len(seller_new)} new sellers...")
        seller_new[['internal_id', 'agent_name', 'agent_phone', 'agent_email', 'added_date']].to_sql(
            'new_seller_info', con=conn, schema='public', if_exists='append', index=False)

# ========== PRICE SNAPSHOT LOGIC ==========
log.info("8. Creating today's price snapshot...")
yesterday = today - timedelta(days=1)
day_before = today - timedelta(days=2)

price_snap_table = f"prices_{today.strftime('%Y_%m_%d')}"
price_yest_table = f"prices_{yesterday.strftime('%Y_%m_%d')}"
price_old_table = f"prices_{day_before.strftime('%Y_%m_%d')}"

with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = :today_table
    """), {'today_table': price_snap_table})
    today_exists = result.scalar() is not None

try:
    price_snapshot = pd.read_sql(
        "SELECT internal_id, price FROM public.standartize WHERE price IS NOT NULL",
        con=engine
    )
except Exception as e:
    log.error(f"Error loading prices from standartize: {e}")
    price_snapshot = pd.DataFrame(columns=['internal_id', 'price'])

if not price_snapshot.empty:
    with engine.begin() as conn:
        price_snapshot.to_sql(price_snap_table, con=conn, schema="public", if_exists="replace", index=False)
    stat_row("Snapshot table created/updated", price_snap_table)
else:
    stat_row("No data for price snapshot!", price_snap_table)

# ========== PRICE CHANGE DETECTION ==========
changed_price_count = 0
try:
    with engine.connect() as conn:
        # Check if yesterday's snapshot table exists
        result = conn.execute(text("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = :yesterday_table
        """), {'yesterday_table': price_yest_table})
        yesterday_exists = result.scalar() is not None

    if yesterday_exists and not price_snapshot.empty:
        try:
            # Load yesterday's prices (internal_id, price)
            price_yesterday = pd.read_sql(
                f"SELECT internal_id, price as price_yesterday FROM public.{price_yest_table}", con=engine
            )
            if not price_yesterday.empty:
                # Merge today's and yesterday's prices by internal_id
                df_compare = price_snapshot.merge(price_yesterday, on="internal_id", how="inner")
                # Detect all listings where the price changed
                df_changed = df_compare[df_compare['price'] != df_compare['price_yesterday']].copy()
                df_changed['old_price'] = df_changed['price_yesterday']
                df_changed['new_price'] = df_changed['price']
                df_changed['price_date'] = pd.to_datetime(today)
                changed_price_count = len(df_changed)
                stat_row("Price changes detected (by snapshot)", changed_price_count)
                if changed_price_count > 0:
                    # Bulk insert each price change as a new row in the history table
                    with engine.begin() as conn2:
                        df_changed[['internal_id', 'old_price', 'new_price', 'price_date']].to_sql(
                            "new_price_change", con=conn2, schema="public", if_exists="append", index=False
                        )
            else:
                stat_row("Yesterday's price snapshot is empty!", price_yest_table)
        except Exception as e:
            log.error(f"Error comparing with yesterday's snapshot: {e}")
    else:
        stat_row("Yesterday's snapshot table does not exist, skipping price change detection", price_yest_table)
except Exception as e:
    log.error(f"Error checking yesterday's snapshot table: {e}")

# ========== OLD SNAPSHOT TABLE DELETION ==========
try:
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = :old_table
        """), {'old_table': price_old_table})
        old_exists = result.scalar() is not None

    if old_exists:
        with engine.begin() as conn2:
            conn2.execute(text(f"DROP TABLE IF EXISTS public.{price_old_table}"))
        stat_row("Deleted old snapshot table", price_old_table)
    else:
        stat_row("No old snapshot table to delete", price_old_table)
except Exception as e:
    log.error(f"Error deleting old snapshot table: {e}")

# ========== LOGGING ETL EVENT ==========
log.info("9. Writing ETL log event...")
log_data = {
    "parser_name": "bzereality_daily_parser",
    "table_name": table_name,
    "processed_at": pd.Timestamp.now(),
    "total_new_ids": len(new_ids),
    "total_archived_ids": len(archived_ids),
    "new_records": len(df_final),
    "archived_records": len(archived_ids),
    "price_changes": changed_price_count,
    "status": "success",
    "message": None
}
with engine.begin() as conn:
    pd.DataFrame([log_data]).to_sql("etl_log", con=conn, schema="public", if_exists="append", index=False)

print("\n" + "-"*70)
print("ETL Bzereality completed. Stats:")
stat_row("Total listings processed", len(df))
stat_row("New listings added", len(df_final))
stat_row("Archived listings", len(archived_ids))
stat_row("Price changes", changed_price_count)
stat_row("New sellers", seller_new.shape[0] if not seller_new.empty else 0)
print("-"*70)
log.info("All steps successfully completed!")