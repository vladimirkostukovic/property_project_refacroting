#!/usr/bin/env python3
import re
import json
import logging
from datetime import date, timedelta

import pandas as pd
from dotenv import load_dotenv
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

# ===================== ADDRESS PARSER =========================
def parse_address(addr):
    result = {"street": None, "city": None, "city_part": None, "district": None, "house_number": None}
    if not isinstance(addr, str):
        return result

    addr = addr.strip()
    # extract district (\"okres X\")
    m = re.search(r"okres\s+([^\-,]+)", addr, flags=re.IGNORECASE)
    if m:
        result['district'] = m.group(1).strip()
        addr = addr.replace(m.group(0), "").strip(", ")

    parts = [p.strip() for p in addr.split(",")]
    first = parts[0] if parts else ""
    # city / city_part
    m = re.match(r"(.+?)\s*-\s*(.+)", first)
    if m:
        result['city'], result['city_part'] = m.group(1).strip(), m.group(2).strip()
    else:
        result['city'] = first

    # street + house_number
    m = re.match(r"^([^\d,]+)\s+(\d+\w*)$", first)
    if m:
        result['street'], result['house_number'] = m.group(1).strip(), m.group(2).strip()

    return result

# ===================== DB CONNECTION ===========================
log.info("1. Connecting to database...")
engine = create_engine(DB_URL)

# ===================== FIND LATEST TABLE =======================
log.info("2. Finding the latest idnes_YYYYMMDD table...")
with engine.connect() as conn:
    table_name = conn.execute(text("""
        SELECT table_name
          FROM information_schema.tables
         WHERE table_schema = 'public'
           AND table_name LIKE 'idnes\\_%' ESCAPE '\\'
           AND table_name ~ '^idnes_[0-9]{8}$'
         ORDER BY table_name DESC
         LIMIT 1
    """)).scalar()
    if not table_name:
        log.error("No idnes_YYYYMMDD table found")
        raise SystemExit(1)
stat_row("Latest input table", table_name)

# ===================== LOAD & PROCESS RAW DATA =================
log.info("3. Loading raw JSON data into RAM...")
today = pd.Timestamp(date.today())
source_id = 4

# load raw
df_raw = pd.read_sql(f"SELECT * FROM public.{table_name}", con=engine)
# normalize JSON column 'data'
df_data = pd.json_normalize(df_raw['data'])
df_data['site_id'] = df_data['id'].astype(str)

# parse addresses
addr_parsed = df_data['address'].apply(parse_address).apply(pd.Series)
df_data = pd.concat([df_data, addr_parsed], axis=1)

# map into standard schema
df = pd.DataFrame({
    'site_id':        df_data['site_id'],
    'added_date':     today,
    'avalaible':      True,
    'archived_date':  pd.NaT,
    'source_id':      source_id,
    'category_name':  df_data.get('type'),
    'category_value': None,
    'name':           df_data.get('title'),
    'deal_type':      df_data.get('title').str.extract(r'^(Prodej|Pronájem)', expand=False).str.lower(),
    'price':          None,
    'rooms':          df_data.get('title').str.extract(r'(\d+\+\d+|\d+\+kk)', expand=False),
    'area_build':     None,
    'area_land':      None,
    'house_type':     df_data.get('type'),
    'district':       df_data['district'],
    'city':           df_data['city'],
    'city_part':      df_data['city_part'],
    'street':         df_data['street'],
    'house_number':   df_data['house_number'],
    'longitude':      None,
    'latitude':       None,
    'district_id':    None,
    'municipality_id':None,
    'region_id':      None,
    'street_id':      None,
})

# price
if 'Cena_numeric' in df_data:
    df['price'] = df_data['Cena_numeric']
elif 'Cena' in df_data:
    df['price'] = (df_data['Cena']
                   .str.replace(r'[^\d]', '', regex=True)
                   .replace('', None)
                   .astype('float64'))

# area_build
if 'Užitná plocha_numeric' in df_data:
    df['area_build'] = df_data['Užitná plocha_numeric']
elif 'Užitná plocha' in df_data:
    df['area_build'] = (df_data['Užitná plocha']
                        .str.replace(r'[^\d,\.]', '', regex=True)
                        .str.replace(',', '.')
                        .replace('', None)
                        .astype('float64'))

# area_land
if 'Plocha pozemku_numeric' in df_data:
    df['area_land'] = df_data['Plocha pozemku_numeric']
elif 'Plocha pozemku' in df_data:
    df['area_land'] = (df_data['Plocha pozemku']
                       .str.replace(r'[^\d,\.]', '', regex=True)
                       .str.replace(',', '.')
                       .replace('', None)
                       .astype('float64'))

stat_row("Listings loaded in df_standart", len(df))

# ===================== LOAD EXISTING ============================
log.info("4. Loading existing listings from standartize...")
existing = pd.read_sql(
    f"SELECT site_id, internal_id, price FROM public.standartize WHERE source_id={source_id}",
    con=engine)
stat_row("Current in standartize", len(existing))

seller_existing = pd.read_sql(
    "SELECT internal_id, agent_email FROM public.new_seller_info", con=engine)
stat_row("Existing sellers", seller_existing.drop_duplicates(
    ['internal_id','agent_email']).shape[0])

# ===================== NEW & ARCHIVED ===========================
log.info("5. Detecting new & archived listings...")
df_new      = df[~df['site_id'].isin(existing['site_id'])].copy()
archived_ids= existing[~existing['site_id'].isin(df['site_id'])]['internal_id'].tolist()
stat_row("To add",   len(df_new))
stat_row("To archive",len(archived_ids))

# ===================== WRITE NEW & ARCHIVE ======================
with engine.begin() as conn:
    if archived_ids:
        log.info(f"  → Archiving {len(archived_ids)} listings...")
        conn.execute(text("""
            UPDATE public.standartize
               SET avalaible    = FALSE
                 , archived_date= :today
             WHERE internal_id = ANY(:ids)
        """), {'today': today, 'ids': archived_ids})

    if not df_new.empty:
        log.info(f"  → Adding {len(df_new)} new listings...")
        df_new.to_sql('standartize', con=conn,
                      schema='public', if_exists='append', index=False)

# ===================== REFRESH MAPPING ===========================
log.info("6. Refreshing site_id→internal_id mapping")
mapping = pd.read_sql(
    f"SELECT site_id, internal_id FROM public.standartize WHERE source_id={source_id}",
    con=engine)

# ===================== SELLER INFO =============================
log.info("7. Matching and appending new sellers...")
seller_cols = ['agent_name','agent_phone','agent_email','site_id']
if all(c in df_data.columns for c in seller_cols):
    s = df_data[seller_cols].dropna().merge(mapping,
        on='site_id', how='left')
    s['added_date']=today
    s = s[s['internal_id'].notna()]
    new_sellers = s.merge(seller_existing,
        on=['internal_id','agent_email'], how='left', indicator=True
    ).query("_merge=='left_only'").drop(columns=['_merge'])
    if not new_sellers.empty:
        with engine.begin() as conn:
            log.info(f"  → Adding {len(new_sellers)} new sellers")
            new_sellers[['internal_id','agent_name','agent_phone','agent_email','added_date']] \
                .to_sql('new_seller_info', con=conn,
                        schema='public', if_exists='append', index=False)

# ===================== PRICE SNAPSHOT & CHANGE DETECTION =========
log.info("8. Creating today's price snapshot...")
yesterday        = today - timedelta(days=1)
snap_today       = f"prices_{today:%Y_%m_%d}"
snap_yest        = f"prices_{yesterday:%Y_%m_%d}"

price_snapshot = pd.read_sql(
    "SELECT internal_id, price FROM public.standartize WHERE price IS NOT NULL",
    con=engine)

if price_snapshot.empty:
    log.info("No prices to snapshot today, skipping change detection")
    changes_count = 0
else:
    with engine.begin() as conn:
        price_snapshot.to_sql(snap_today, con=conn,
                              schema='public', if_exists='replace', index=False)
    stat_row("Snapshot created", snap_today)

    log.info("9. Detecting price changes vs yesterday...")
    exists = engine.dialect.has_table(engine.connect(), snap_yest, schema='public')
    if not exists:
        stat_row("No yesterday snapshot", snap_yest)
        changes_count = 0
    else:
        prev = pd.read_sql(
            f"SELECT internal_id, price AS old_price FROM public.{snap_yest}", con=engine)
        merged = price_snapshot.merge(prev, on='internal_id', how='inner')
        changed = merged[merged['price'] != merged['old_price']].copy()
        if changed.empty:
            stat_row("No price changes", 0)
            changes_count = 0
        else:
            changed['new_price']  = changed['price']
            changed['price_date'] = pd.to_datetime(today)
            hist = changed[['internal_id','old_price','new_price','price_date']]
            with engine.begin() as conn:
                hist.to_sql('new_price_change', con=conn,
                            schema='public', if_exists='append', index=False)
            stat_row("Price changes logged", len(hist))
            changes_count = len(hist)

# ===================== CLEAN OLD SNAPSHOT =======================
log.info("10. Cleaning up old snapshot...")
day_before = today - timedelta(days=2)
snap_old   = f"prices_{day_before:%Y_%m_%d}"
with engine.begin() as conn:
    conn.execute(text(f"DROP TABLE IF EXISTS public.{snap_old}"))
stat_row("Old snapshot dropped", snap_old)

# ===================== PHOTO LINKS ============================
log.info("11. Extracting photo links...")
with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS public.idnes_links_photo (
            internal_id INTEGER,
            site_id TEXT,
            photo_url TEXT,
            added_date TIMESTAMP,
            source_id INTEGER
        )
    """))

active = pd.read_sql(
    "SELECT internal_id, site_id FROM public.standartize WHERE source_id=4 AND archived_date IS NULL",
    con=engine)
exists = pd.read_sql("SELECT internal_id, photo_url FROM public.idnes_links_photo", con=engine)
seen = set(zip(exists['internal_id'], exists['photo_url']))

new_links = []
for sid, iid in active.itertuples(index=False):
    imgs = df_data.loc[df_data['site_id']==sid, 'image_links']
    if imgs.empty: continue
    try:
        lst = json.loads(imgs.iloc[0]) if isinstance(imgs.iloc[0], str) else imgs.iloc[0]
        for it in lst if isinstance(lst, list) else []:
            url = it if isinstance(it, str) else it.get('url')
            if url and (iid, url) not in seen:
                new_links.append({
                    'internal_id': iid,
                    'site_id': sid,
                    'photo_url': url,
                    'added_date': pd.Timestamp.now(),
                    'source_id': source_id
                })
    except Exception:
        continue

if new_links:
    dfp = pd.DataFrame(new_links)
    with engine.begin() as conn:
        dfp.to_sql('idnes_links_photo', con=conn,
                   schema='public', if_exists='append', index=False)
stat_row("Photo links added", len(new_links))

# ===================== ETL LOGGING ============================
log.info("12. Writing ETL log event...")
evt = {
    'parser_name':      'idnes_daily_parser',
    'table_name':       table_name,
    'processed_at':     pd.Timestamp.now(),
    'total_new_ids':    len(df_new),
    'total_archived_ids': len(archived_ids),
    'new_records':      len(df_new),
    'archived_records': len(archived_ids),
    'price_changes':    changes_count,
    'status':           'success',
    'message':          None
}
pd.DataFrame([evt]).to_sql('etl_log', con=engine,
                          schema='public', if_exists='append', index=False)

print("\n" + "="*70)
print("ETL IDNES completed. Stats:")
stat_row("Total processed", len(df_data))
stat_row("New listings", len(df_new))
stat_row("Archived", len(archived_ids))
stat_row("Price changes", changes_count)
print("="*70)

log.info("All steps successfully completed!")