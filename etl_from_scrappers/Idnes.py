import pandas as pd
import re
import json
import logging
from datetime import date, timedelta
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# ===================== LOAD ENV ============================
load_dotenv()

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")

# ===================== LOGGING AND UTILS =======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

def stat_row(label, value):
    print(f"{label:<50}: {value}")

print("="*70)
print("CUSTOM ETL SCRIPT: Listings Loading (source_id=4)".center(70))
print("="*70)
# ===================== Address Parse Utility ====================
def parse_address(addr):
    result = {
        "street": None,
        "city": None,
        "city_part": None,
        "district": None,
        "house_number": None
    }
    if not isinstance(addr, str):
        return result
    addr = addr.strip()
    # Extract 'okres' as district
    district_match = re.search(r"okres\s+([^\-,]+)", addr)
    if district_match:
        result['district'] = district_match.group(1).strip()
        addr = addr.replace(district_match.group(0), "").strip(", ")
    # Split by comma
    parts = [x.strip() for x in addr.split(",")]
    if len(parts) == 2:
        first, second = parts
    else:
        first = parts[0]
        second = None
    # City/city_part
    city_match = re.match(r"(.+?)\s*-\s*(.+)", first)
    if city_match:
        result['city'] = city_match.group(1).strip()
        result['city_part'] = city_match.group(2).strip()
    else:
        result['city'] = first
    # Street and house number (basic, improve as needed)
    street_match = re.match(r"^([^\d,]+)\s+(\d+\w*)$", first)
    if street_match:
        result['street'] = street_match.group(1).strip()
        result['house_number'] = street_match.group(2).strip()
    else:
        result['street'] = None
    return result

# ===================== DB CONNECTION ===========================
log.info("1. Connecting to database...")
db_url = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
engine = create_engine(db_url)

# ===================== GET LATEST SOURCE TABLE ================
log.info("2. Finding the latest source table for source_id=4...")
with engine.connect() as conn:
    table_name = conn.execute(text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name LIKE 'idnes\\_%' ESCAPE '\\'
          AND table_name ~ '^idnes_[0-9]{8}$'
        ORDER BY table_name DESC LIMIT 1
    """)).scalar()
    if not table_name:
        raise ValueError("No idnes table found")
stat_row("Latest input table", table_name)

# ===================== LOAD AND PROCESS DATA ===================
log.info("3. Loading all data into RAM...")
today = pd.Timestamp(date.today())
source_id = 4

# Load raw table from database (with jsonb 'data' column)
df_raw = pd.read_sql(f"SELECT * FROM public.{table_name}", con=engine)

# Unpack JSON from 'data' column into flat DataFrame
df_data = pd.json_normalize(df_raw['data'])
df_data['site_id'] = df_data['id'].astype(str)

# Parse address field into components
address_parsed = df_data['address'].apply(parse_address).apply(pd.Series)
df_data = pd.concat([df_data, address_parsed], axis=1)

# Map columns for standartize table
df_standart = pd.DataFrame()
df_standart['site_id'] = df_data['site_id']
df_standart['added_date'] = today
df_standart['avalaible'] = True
df_standart['archived_date'] = pd.NaT
df_standart['source_id'] = source_id

df_standart['category_name'] = df_data['type'] if 'type' in df_data.columns else None
df_standart['category_value'] = None
df_standart['name'] = df_data['title'] if 'title' in df_data.columns else None
df_standart['deal_type'] = df_data['title'].str.extract(r'^(Prodej|Pronájem)', expand=False).str.lower() if 'title' in df_data.columns else None

# Price
if 'Cena_numeric' in df_data.columns:
    df_standart['price'] = df_data['Cena_numeric']
elif 'Cena' in df_data.columns:
    df_standart['price'] = (
        df_data['Cena']
        .str.replace(r'[^\d]', '', regex=True)
        .replace('', None)
        .astype('float64')
    )
else:
    df_standart['price'] = None

# Living area
if 'Užitná plocha_numeric' in df_data.columns:
    df_standart['area_build'] = df_data['Užitná plocha_numeric']
elif 'Užitná plocha' in df_data.columns:
    df_standart['area_build'] = (
        df_data['Užitná plocha']
        .str.replace(r'[^\d,\.]', '', regex=True)
        .str.replace(',', '.')
        .replace('', None)
        .astype('float64')
    )
else:
    df_standart['area_build'] = None

# Land area
if 'Plocha pozemku_numeric' in df_data.columns:
    df_standart['area_land'] = df_data['Plocha pozemku_numeric']
elif 'Plocha pozemku' in df_data.columns:
    df_standart['area_land'] = (
        df_data['Plocha pozemku']
        .str.replace(r'[^\d,\.]', '', regex=True)
        .str.replace(',', '.')
        .replace('', None)
        .astype('float64')
    )
else:
    df_standart['area_land'] = None

df_standart['house_type'] = df_data['type'] if 'type' in df_data.columns else None

# Address columns filled from parsed address
df_standart['district'] = df_data['district']
df_standart['city'] = df_data['city']
df_standart['city_part'] = df_data['city_part']
df_standart['street'] = df_data['street']
df_standart['address'] = df_data['address'] if 'address' in df_data.columns else None
df_standart['house_number'] = df_data['house_number']

df_standart['longitude'] = None
df_standart['latitude'] = None
df_standart['district_id'] = None
df_standart['municipality_id'] = None
df_standart['region_id'] = None
df_standart['street_id'] = None

# Rooms
df_standart['rooms'] = df_data['title'].str.extract(r'(\d+\+\d+|\d+\+kk)', expand=False) if 'title' in df_data.columns else None
numeric_columns = ['price', 'area_build', 'area_land']
for col in numeric_columns:
    if col in df_standart.columns:
        df_standart[col] = df_standart[col].replace('', None)
        df_standart[col] = pd.to_numeric(df_standart[col], errors='coerce')
# Ensure all required columns are present
required_columns = [
    'site_id', 'added_date', 'avalaible', 'archived_date', 'source_id',
    'category_value', 'category_name', 'name', 'deal_type', 'price',
    'rooms', 'area_build', 'area_land', 'house_type', 'district', 'city',
    'city_part', 'street', 'house_number', 'longitude', 'latitude',
    'district_id', 'municipality_id', 'region_id', 'street_id'
]
for col in required_columns:
    if col not in df_standart.columns:
        df_standart[col] = None

df_standart = df_standart[required_columns]
# ===================== LOAD EXISTING LISTINGS ==================
log.info("4. Loading existing listings and sellers...")
existing_df = pd.read_sql(
    f"SELECT site_id, internal_id, price FROM public.standartize WHERE source_id = {source_id}",
    con=engine)
stat_row("Current listings in standartize", len(existing_df))

seller_existing = pd.read_sql("SELECT internal_id, agent_email FROM public.new_seller_info", con=engine)
stat_row("Unique sellers in new_seller_info", seller_existing.drop_duplicates(['internal_id','agent_email']).shape[0])

# ===================== DETECT NEW & ARCHIVED ===================
log.info("5. Detecting new and archived listings...")
df_new = df_standart[~df_standart['site_id'].isin(existing_df['site_id'])].copy()
archived_ids = existing_df[~existing_df['site_id'].isin(df_standart['site_id'])]['internal_id'].tolist()
stat_row("New listings to add", len(df_new))
stat_row("Listings to archive", len(archived_ids))

# ===================== WRITE NEW & ARCHIVE =====================
with engine.begin() as conn:
    if archived_ids:
        log.info(f"  → Archiving {len(archived_ids)} listings...")
        conn.execute(
            text("""
                UPDATE public.standartize
                SET avalaible = FALSE, archived_date = :today
                WHERE internal_id = ANY(:ids)
            """),
            {'today': today, 'ids': archived_ids}
        )
    if not df_new.empty:
        log.info(f"  → Adding {len(df_new)} new listings...")
        df_new.to_sql('standartize', con=conn, schema='public', if_exists='append', index=False)

# ===================== RELOAD MAPPING (site_id <-> internal_id) ==========
log.info("6. Refreshing mapping of site_id to internal_id from DB...")
all_map = pd.read_sql(
    f"SELECT site_id, internal_id FROM public.standartize WHERE source_id = {source_id}", con=engine
)

# ===================== SELLER INFO (DE-DUP & APPEND) ======================
log.info("7. Matching sellers with internal_id...")
seller_cols = ['agent_name', 'agent_phone', 'agent_email', 'site_id']
seller_new = pd.DataFrame()
if all(col in df_data.columns for col in seller_cols):
    seller_df = df_data[seller_cols].dropna().copy()
    seller_df = pd.merge(seller_df, all_map, on='site_id', how='left')
    seller_df['added_date'] = today
    seller_df = seller_df[seller_df['internal_id'].notnull()]
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

## ===================== PRICE SNAPSHOT BLOCK ================================
log.info("8. Creating today's price snapshot...")
yesterday = today - timedelta(days=1)
day_before = today - timedelta(days=2)

price_snap_table = f"prices_{today.strftime('%Y_%m_%d')}"
price_yest_table = f"prices_{yesterday.strftime('%Y_%m_%d')}"
price_old_table = f"prices_{day_before.strftime('%Y_%m_%d')}"

# Проверяем есть ли таблица с сегодняшним снэпшотом
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = :today_table
    """), {'today_table': price_snap_table})
    today_exists = result.scalar() is not None

try:
    # Только internal_id и price — никакого source_id
    price_snapshot = pd.read_sql(
        "SELECT internal_id, price FROM public.standartize WHERE price IS NOT NULL",
        con=engine
    )
except Exception as e:
    log.error(f"Error loading prices from standartize: {e}")
    price_snapshot = pd.DataFrame(columns=['internal_id', 'price'])

if not price_snapshot.empty:
    with engine.begin() as conn:
        if today_exists:
            price_snapshot.to_sql(price_snap_table, con=conn, schema="public", if_exists="append", index=False)
            stat_row("Appended price snapshot to existing table", price_snap_table)
        else:
            price_snapshot.to_sql(price_snap_table, con=conn, schema="public", if_exists="replace", index=False)
            stat_row("Created new price snapshot table", price_snap_table)
else:
    stat_row("No data for price snapshot!", price_snap_table)

# ===================== PRICE CHANGE DETECTION =============================
changed_price_count = 0
try:
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = :yesterday_table
        """), {'yesterday_table': price_yest_table})
        yesterday_exists = result.scalar() is not None

    if yesterday_exists and not price_snapshot.empty:
        try:
            price_yesterday = pd.read_sql(
                f"SELECT internal_id, price as price_yesterday FROM public.{price_yest_table}", con=engine
            )
            if not price_yesterday.empty:
                df_compare = price_snapshot.merge(price_yesterday, on="internal_id", how="inner")
                df_changed = df_compare[df_compare['price'] != df_compare['price_yesterday']].copy()
                df_changed['old_price'] = df_changed['price_yesterday']
                df_changed['new_price'] = df_changed['price']
                df_changed['price_date'] = pd.to_datetime(today)
                changed_price_count = len(df_changed)
                stat_row("Price changes detected (by snapshot)", changed_price_count)

                if changed_price_count > 0:
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

# ===================== OLD SNAPSHOT TABLE DELETION ========================
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
# ===================== CORRECT PHOTO LINKS FILL =========================
log.info("10. Extracting and saving photo links for ALL active listings (unique by internal_id)...")

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS public.idnes_links_photo (
            internal_id INTEGER,
            site_id TEXT,
            photo_url TEXT,
            added_date TIMESTAMP,
            source_id INTEGER
        );
    """))

active_stand = pd.read_sql("""
    SELECT internal_id, site_id
    FROM public.standartize
    WHERE source_id = %s AND archived_date IS NULL
""", con=engine, params=(source_id,))

existing_photo_links = pd.read_sql("""
    SELECT internal_id, photo_url
    FROM public.idnes_links_photo
""", con=engine)

to_process = active_stand[~active_stand['internal_id'].isin(existing_photo_links['internal_id'])].copy()
stat_row("Active listings without photo links", len(to_process))

photo_links = []
for idx, row in to_process.iterrows():
    sid = row['site_id']
    iid = row['internal_id']

    src = df_data[df_data['site_id'] == sid]
    if src.empty or 'image_links' not in src.columns:
        continue
    images = src.iloc[0]['image_links']
    try:
        if isinstance(images, str):
            img_list = json.loads(images)
        else:
            img_list = images
        if isinstance(img_list, list):
            for img in img_list:
                url = img if isinstance(img, str) else img.get('url')
                if url:
                    if not ((existing_photo_links['internal_id'] == iid) & (existing_photo_links['photo_url'] == url)).any():
                        photo_links.append({
                            'internal_id': iid,
                            'site_id': sid,
                            'photo_url': url,
                            'added_date': pd.Timestamp.now(),
                            'source_id': source_id
                        })
    except Exception as e:
        log.warning(f"Photo links parse failed for site_id={sid}: {e}")

df_photos = pd.DataFrame(photo_links)
if not df_photos.empty:
    with engine.begin() as conn:
        log.info(f"  → Adding {len(df_photos)} new photo links...")
        df_photos.to_sql(
            'idnes_links_photo',
            con=conn,
            schema='public',
            if_exists='append',
            index=False
        )
    stat_row("New photo links added", len(df_photos))
else:
    stat_row("New photo links added", 0)

# ===================== ETL PROCESS LOGGING ===============================
log.info("11. Writing ETL log event...")
log_data = {
    "parser_name": "idnes_daily_parser",
    "table_name": table_name,
    "processed_at": pd.Timestamp.now(),
    "total_new_ids": len(df_new),
    "total_archived_ids": len(archived_ids),
    "new_records": len(df_standart),
    "archived_records": len(archived_ids),
    "price_changes": changed_price_count,
    "status": "success",
    "message": None
}
with engine.begin() as conn:
    pd.DataFrame([log_data]).to_sql("etl_log", con=conn, schema="public", if_exists="append", index=False)

print("\n" + "-"*70)
print("ETL IDNES completed. Stats:")
stat_row("Total listings processed", len(df_data))
stat_row("New listings added", len(df_new))
stat_row("Archived listings", len(archived_ids))
stat_row("Price changes", changed_price_count)
stat_row("New sellers", seller_new.shape[0] if not seller_new.empty else 0)
print("-"*70)
log.info("All steps successfully completed!")