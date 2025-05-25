import os
from dotenv import load_dotenv
import pandas as pd
import json
import logging
from datetime import date, timedelta
from sqlalchemy import create_engine, text
from config import DB_URL

# ===================== ENV LOADING ============================
load_dotenv()

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
print("IDNES ETL SCRIPT: Listings Loading".center(70))
print("="*70)

# ===================== DB CONNECTION ===========================
log.info("1. Connecting to database...")
engine = create_engine(DB_URL)

# ===================== GET LATEST SOURCE TABLE ================
log.info("2. Finding the latest idnes table...")
with engine.connect() as conn:
    table_name = conn.execute(text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name LIKE 'hyper\\_%' ESCAPE '\\'
          AND table_name ~ '^hyper_[0-9]{8}$'
        ORDER BY table_name DESC LIMIT 1
    """)).scalar()
    if not table_name:
        raise ValueError("No hyper table found")
stat_row("Latest input table", table_name)

# ===================== LOAD AND PROCESS DATA ===================
log.info("3. Loading all data into RAM...")
today = pd.Timestamp(date.today())
source_id = 3  # например, для idnes

# Загрузим с JSON колонкой data (jsonb)
df_raw = pd.read_sql(f"SELECT * FROM public.{table_name} LIMIT 5000", con=engine)

# Парсим JSON из колонки 'data' в датафрейм
df_data = pd.json_normalize(df_raw['data'])

# Добавим site_id из listing_id в строковом формате
df_data['site_id'] = df_data['ID inzerátu'].astype(str)
df_data['added_date'] = today
df_data['avalaible'] = True
df_data['archived_date'] = pd.NaT
df_data['source_id'] = source_id

# Маппинг колонок в standartize
df_standart = pd.DataFrame()

df_standart['site_id'] = df_data['site_id']
df_standart['added_date'] = df_data['added_date']
df_standart['avalaible'] = df_data['avalaible']
df_standart['archived_date'] = df_data['archived_date']
df_standart['source_id'] = df_data['source_id']

df_standart['category_name'] = df_data.get('Kategorie')
df_standart['name'] = df_data.get('Title')
df_standart['deal_type'] = df_data.get('Typ nabídky').str.lower()
# Очистка цены — оставляем только цифры и преобразуем к float
df_standart['price'] = df_data.get('Celkem').str.replace(r'[^\d]', '', regex=True).replace('', None).astype('float64')

# Кол-во комнат — можно оставить как есть или привести к удобному виду
df_standart['rooms'] = df_data.get('Podkategorie').str.lower()

# Площади — чистим от единиц измерения, меняем запятую на точку
def clean_area(col):
    if col is None:
        return None
    return (
        col.str.replace(r'[^\d,\.]', '', regex=True)
        .str.replace(',', '.')
        .replace('', None)
        .astype('float64')
    )

df_standart['area_build'] = clean_area(df_data.get('Plocha'))
df_standart['area_land'] = clean_area(df_data.get('Pozemku'))

df_standart['house_type'] = df_data.get('Typ domu')
df_standart['district'] = None
df_standart['city'] = df_data.get('Lokalita')
df_standart['city_part'] = None
df_standart['street'] = None
df_standart['house_number'] = None
df_standart['longitude'] = None
df_standart['latitude'] = None
df_standart['district_id'] = None
df_standart['municipality_id'] = None
df_standart['region_id'] = None
df_standart['street_id'] = None

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

# ===================== PRICE SNAPSHOT BLOCK ================================
log.info("8. Creating today's price snapshot...")
yesterday = today - timedelta(days=1)
day_before = today - timedelta(days=2)

price_snap_table = f"prices_{today.strftime('%Y_%m_%d')}"
price_yest_table = f"prices_{yesterday.strftime('%Y_%m_%d')}"
price_old_table = f"prices_{day_before.strftime('%Y_%m_%d')}"

try:
    # Сохраняем только internal_id и price (никаких source_id!)
    price_snapshot = pd.read_sql(
        "SELECT internal_id, price FROM public.standartize WHERE price IS NOT NULL",
        con=engine
    )
except Exception as e:
    log.error(f"Error loading prices from standartize: {e}")
    price_snapshot = pd.DataFrame(columns=['internal_id', 'price'])

if not price_snapshot.empty:
    with engine.begin() as conn:
        # Проверяем, есть ли уже снапшот за сегодня
        result = conn.execute(text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = :today_table
        """), {'today_table': price_snap_table})
        today_exists = result.scalar() is not None

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
        # Check if yesterday's snapshot table exists
        result = conn.execute(text("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = :yesterday_table
        """), {'yesterday_table': price_yest_table})
        yesterday_exists = result.scalar() is not None

    if yesterday_exists and not price_snapshot.empty:
        try:
            # Load only internal_id and yesterday's price
            price_yesterday = pd.read_sql(
                f"SELECT internal_id, price as price_yesterday FROM public.{price_yest_table}",
                con=engine
            )
            # Ensure both today's and yesterday's dataframes are not empty
            if not price_yesterday.empty and not price_snapshot.empty:
                # Merge by internal_id to compare prices
                df_compare = price_snapshot.merge(
                    price_yesterday, on="internal_id", how="inner"
                )
                # Find rows where the price has changed since yesterday
                df_changed = df_compare[df_compare['price'] != df_compare['price_yesterday']].copy()
                df_changed['old_price'] = df_changed['price_yesterday']
                df_changed['new_price'] = df_changed['price']
                df_changed['price_date'] = pd.to_datetime(today)
                changed_price_count = len(df_changed)
                stat_row("Price changes detected (by snapshot)", changed_price_count)

                if changed_price_count > 0:
                    # Bulk insert all changes as new rows in history table
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
# ===================== PHOTO LINKS EXTRACTION =========================
log.info("9. Extracting and saving photo links for all active listings...")

# Создаем таблицу, если ее нет
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
log.info("10. Writing ETL log event...")
log_data = {
    "parser_name": "idnes_daily_parser",
    "table_name": table_name,
    "processed_at": pd.Timestamp.now(),
    "total_new_ids": len(df_new),
    "total_archived_ids": len(archived_ids),
    "new_records": len(df_new),
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
log.info("✅ All steps successfully completed!")