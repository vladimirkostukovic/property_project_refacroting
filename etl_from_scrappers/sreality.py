import os
from dotenv import load_dotenv
import pandas as pd
import re
import logging
from datetime import date, timedelta
from sqlalchemy import create_engine, text
from config import DB_URL

# ===================== ENV LOADING ============================
load_dotenv()

# ===================== LOGGING SETUP ===========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

def stat_row(label, value):
    print(f"{label:<50}: {value}")

print("="*70)
print("SREALITY ETL SCRIPT: Listings Loading".center(70))
print("="*70)

# ===================== DB CONNECTION ===========================
log.info("1. Connecting to database...")
engine = create_engine(DB_URL)

# ===================== GET LATEST SOURCE TABLE ================
log.info("2. Finding the latest sreality table...")
with engine.connect() as conn:
    table_name = conn.execute(text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name LIKE 'sreality\_%' ESCAPE '\\'
          AND table_name ~ '^sreality_[0-9]{8}$'
        ORDER BY table_name DESC
        LIMIT 1
    """)).scalar()
    if not table_name:
        raise ValueError("No sreality table found")
stat_row("Latest input table", table_name)

# ===================== LOAD AND PROCESS DATA ===================
log.info("3. Loading all data into RAM...")
today = date.today()
source_id = 1

df = pd.read_sql(f"SELECT * FROM public.{table_name}", con=engine)
# Ensure price column exists and numeric
if 'price' in df.columns:
    df['price'] = pd.to_numeric(df['price'], errors='coerce')

df['site_id']      = df['id'].astype(str)
df['source_id']    = source_id
df['added_date']   = pd.to_datetime(today)
df['archived_date']= pd.NaT
df['avalaible']    = True

log.info("   → Listings loaded: %d", len(df))

# ===================== LOAD EXISTING LISTINGS ==================
log.info("4. Loading existing listings from standartize...")
existing_df = pd.read_sql(
    f"SELECT site_id, internal_id, price FROM public.standartize WHERE source_id = {source_id}",
    con=engine
)
stat_row("Current listings in standartize", len(existing_df))

seller_existing = pd.read_sql(
    "SELECT internal_id, agent_email FROM public.new_seller_info",
    con=engine
)
stat_row("Unique sellers in new_seller_info",
         seller_existing.drop_duplicates(['internal_id','agent_email']).shape[0])

# ===================== DETECT NEW & ARCHIVED ===================
log.info("5. Detecting new and archived listings...")
df_new      = df[~df['site_id'].isin(existing_df['site_id'])].copy()
archived_ids = existing_df[~existing_df['site_id'].isin(df['site_id'])]['internal_id'].tolist()
stat_row("New listings to add", len(df_new))
stat_row("Listings to archive", len(archived_ids))

# ===================== STANDARDIZE COLUMNS =====================
cols = [
    'site_id','added_date','avalaible','archived_date','source_id',
    'category_value','category_name','name','deal_type','price',
    'rooms','area_build','area_land','house_type','district','city',
    'city_part','street','house_number','longitude','latitude',
    'district_id','municipality_id','region_id','street_id'
]
for c in cols:
    if c not in df_new.columns:
        df_new[c] = None

df_standart = df_new[cols]

# ===================== WRITE NEW & ARCHIVE =====================
with engine.begin() as conn:
    if archived_ids:
        log.info(f"  → Archiving {len(archived_ids)} listings...")
        conn.execute(
            text("""
                UPDATE public.standartize
                   SET avalaible = FALSE
                     , archived_date = :today
                 WHERE internal_id = ANY(:ids)
            """),
            {'today': today, 'ids': archived_ids}
        )
    if not df_standart.empty:
        log.info(f"  → Adding {len(df_standart)} new listings...")
        df_standart.to_sql(
            'standartize', con=conn, schema='public', if_exists='append', index=False
        )

# ===================== RELOAD MAPPING ============================
log.info("6. Refreshing site_id->internal_id mapping...")
all_map = pd.read_sql(
    f"SELECT site_id, internal_id FROM public.standartize WHERE source_id = {source_id}",
    con=engine
)

# ===================== SELLER INFO ==============================
log.info("7. Matching sellers with internal_id...")
seller_cols = ['agent_name','agent_phone','agent_email','site_id']
seller_new = pd.DataFrame()
if all(col in df.columns for col in seller_cols):
    seller_df = df[seller_cols].dropna().copy()
    seller_df = seller_df.merge(all_map, on='site_id', how='left')
    seller_df['added_date'] = today
    seller_df = seller_df[seller_df['internal_id'].notnull()]
    seller_new = seller_df.merge(
        seller_existing, on=['internal_id','agent_email'], how='left', indicator=True
    ).query("_merge=='left_only'").drop(columns=['_merge'])
stat_row("New sellers to add", seller_new.shape[0] if not seller_new.empty else 0)

if not seller_new.empty:
    with engine.begin() as conn:
        log.info(f"  → Adding {len(seller_new)} new sellers...")
        seller_new[['internal_id','agent_name','agent_phone','agent_email','added_date']]
            .to_sql('new_seller_info', con=conn, schema='public', if_exists='append', index=False)

# ===================== PRICE SNAPSHOT & CHANGE DETECTION =========
log.info("8. Creating today's price snapshot...")
yesterday        = today - timedelta(days=1)
price_snap_table = f"prices_{today:%Y_%m_%d}"
price_yest_table = f"prices_{yesterday:%Y_%m_%d}"

# build today snapshot
price_snapshot = pd.read_sql(
    "SELECT internal_id, price FROM public.standartize WHERE price IS NOT NULL",
    con=engine
)

if price_snapshot.empty:
    log.info("No data for today's price snapshot → skipping detection")
else:
    with engine.begin() as conn:
        price_snapshot.to_sql(
            price_snap_table, con=conn, schema='public', if_exists='replace', index=False
        )
    stat_row("Snapshot table created/replaced", price_snap_table)

    # detect changes
    log.info("9. Detecting price changes vs yesterday...")
    yesterday_exists = engine.dialect.has_table(engine.connect(), price_yest_table, schema='public')
    if not yesterday_exists:
        stat_row("Yesterday's snapshot missing", price_yest_table)
        changed_price_count = 0
    else:
        price_yesterday = pd.read_sql(
            f"SELECT internal_id, price AS old_price FROM public.{price_yest_table}",
            con=engine
        )
        df_compare = price_snapshot.merge(price_yesterday, on='internal_id', how='inner')
        df_changed = df_compare.query('price != old_price').copy()
        if df_changed.empty:
            stat_row("No price changes detected", 0)
            changed_price_count = 0
        else:
            df_changed['new_price']  = df_changed['price']
            df_changed['price_date'] = pd.to_datetime(today)
            df_history = df_changed[['internal_id','old_price','new_price','price_date']]
            with engine.begin() as conn2:
                df_history.to_sql(
                    'price_change', con=conn2, schema='public', if_exists='append', index=False
                )
            stat_row("Price changes logged", len(df_history))
            changed_price_count = len(df_history)

# ===================== OLD SNAPSHOT CLEANUP =====================
log.info("10. Cleaning up old snapshots...")
day_before      = today - timedelta(days=2)
price_old_table = f"prices_{day_before:%Y_%m_%d}"
with engine.connect() as conn:
    old_exists = conn.execute(text("""
        SELECT EXISTS(
          SELECT 1 FROM information_schema.tables
          WHERE table_schema='public' AND table_name=:old_table
        )
    """), {'old_table': price_old_table}).scalar()
if old_exists:
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE public.{price_old_table}"))
    stat_row("Deleted old snapshot table", price_old_table)
else:
    stat_row("No old snapshot to delete", price_old_table)

# ==================== PHOTO LINKS EXTRACTION =====================
import json
log.info("11. Extracting photo links for active listings...")
with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS public.sreality_links_photo (
            internal_id INTEGER,
            site_id TEXT,
            photo_url TEXT,
            added_date TIMESTAMP,
            source_id INTEGER
        );
    """))
active = pd.read_sql(
    "SELECT internal_id, site_id FROM public.standartize WHERE source_id=1 AND archived_date IS NULL",
    con=engine
)
existing = pd.read_sql("SELECT internal_id, photo_url FROM public.sreality_links_photo", con=engine)
seen = set(zip(existing['internal_id'], existing['photo_url']))
site_map = dict(zip(df['site_id'], df['images']))
new_links=[]
for sid,iid in active.itertuples(index=False):
    imgs = site_map.get(sid)
    if not imgs: continue
    try:
        lst = json.loads(imgs) if isinstance(imgs,str) else imgs
        if isinstance(lst,list):
            for it in lst:
                url = it if isinstance(it,str) else it.get('url')
                if url and (iid,url) not in seen:
                    new_links.append({'internal_id':iid,'site_id':sid,'photo_url':url,
                                      'added_date':pd.Timestamp.now(),'source_id':1})
    except:
        continue
if new_links:
    dfp=pd.DataFrame(new_links)
    with engine.begin() as conn:
        conn.execute(text("CREATE TEMP TABLE tmp_photos ON COMMIT DROP AS SELECT * FROM public.sreality_links_photo LIMIT 0"))
        dfp.to_sql('tmp_photos', con=conn, if_exists='append', index=False)
        conn.execute(text("""
            INSERT INTO public.sreality_links_photo
            SELECT t.* FROM tmp_photos t
            LEFT JOIN public.sreality_links_photo p
              ON t.internal_id=p.internal_id AND t.photo_url=p.photo_url
            WHERE p.internal_id IS NULL;
        """))
    stat_row("New photo links added", len(new_links))
else:
    stat_row("New photo links added", 0)

# ===================== ETL EVENT LOG ============================
log.info("12. Writing ETL log event...")
log_data = {
    'parser_name':'sreality_daily_parser',
    'table_name':table_name,
    'processed_at':pd.Timestamp.now(),
    'total_new_ids':len(df_new),
    'total_archived_ids':len(archived_ids),
    'new_records':len(df_standart),
    'archived_records':len(archived_ids),
    'price_changes':changed_price_count,
    'status':'success',
    'message':None
}
pd.DataFrame([log_data]).to_sql(
    'etl_log', con=engine, schema='public', if_exists='append', index=False
)

print("\n" + "-"*70)
print("ETL Sreality completed. Stats:")
stat_row("Total listings processed", len(df))
stat_row("New listings added", len(df_new))
stat_row("Archived listings", len(archived_ids))
stat_row("Price changes", changed_price_count)
print("-"*70)
log.info("All steps successfully completed!")