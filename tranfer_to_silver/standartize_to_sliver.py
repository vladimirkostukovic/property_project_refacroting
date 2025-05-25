import pandas as pd
import numpy as np
import logging
from sqlalchemy import create_engine, text
from datetime import date
from config import DB_URL  # <--- вот так

# ========== Logging Setup ==========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

engine = create_engine(DB_URL)

# ========== Category Mapping ==========
category_map = {
    'byty':      (1, 'Byty'),
    'apartments': (1, 'Byty'),
    'byty k prodeji': (1, 'Byty'),
    'byty k pronájmu': (1, 'Byty'),
    'byty k pronajmu': (1, 'Byty'),

    'domy':      (2, 'Domy'),
    'houses':    (2, 'Domy'),
    'rodinné domy': (2, 'Domy'),
    'chaty a chalupy': (2, 'Domy'),
    'novostavby': (2, 'Domy'),

    'pozemky':   (3, 'Pozemky'),
    'land':      (3, 'Pozemky'),
    'pozemky, zahrady a historické stavby': (3, 'Pozemky'),

    'komerční':  (4, 'Komerční'),
    'komercni':  (4, 'Komerční'),
    'commercial': (4, 'Komerční'),
    'komerční objekty': (4, 'Komerční'),

    'ostatní':   (5, 'Ostatní'),
    'ostatni':   (5, 'Ostatní'),
    'ostatní nemovitosti': (5, 'Ostatní'),
    'garages':   (5, 'Ostatní'),
    'garáže a drobné objekты': (5, 'Ostatní'),
    'garáže':    (5, 'Ostatní'),
    'nemovitosti v zahraničí': (5, 'Ostatní'),
    'prodej':    (5, 'Ostatní'),  # fallback
}

def map_category(name):
    if not isinstance(name, str):
        return (5, 'Ostatní')
    key = name.strip().lower()
    for k in category_map:
        if k in key:
            return category_map[k]
    return (5, 'Ostatní')

# ========== Загружаем обе таблицы ==========
log.info("Loading public.standartize...")
df = pd.read_sql("SELECT * FROM public.standartize", con=engine)
log.info("Loading silver.standartize_silver...")
df_silver = pd.read_sql("SELECT * FROM silver.standartize_silver", con=engine)

# ========== Преобразования и категоризация ==========
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

# ========== Сравнение ==========
# Для новых строк: нет такого internal_id в silver
new_mask = ~df['internal_id'].isin(df_silver['internal_id'])
df_new = df[new_mask].copy()

# Для изменённых строк: есть, но хотя бы один из важных столбцов изменился
compare_cols = ['avalaible', 'archived_date']
df_merged = df.merge(df_silver[['internal_id'] + compare_cols], on='internal_id', how='inner', suffixes=('', '_old'))
changed_mask = (
    (df_merged['avalaible'] != df_merged['avalaible_old']) |
    (df_merged['archived_date'] != df_merged['archived_date_old'])
)
df_changed = df_merged[changed_mask].copy()

log.info(f"New rows: {len(df_new)}, changed rows: {len(df_changed)}")

# ========== Сохраняем новые строки ==========
if not df_new.empty:
    log.info(f"Inserting {len(df_new)} NEW rows...")
    df_new.to_sql('standartize_silver', con=engine, schema='silver', if_exists='append', index=False)

# ========== Массовое обновление изменённых ==========
# 1. Кладём изменённые строки во временную таблицу
if not df_changed.empty:
    temp_table = 'tmp_standartize_updates'
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS silver.{temp_table}"))
    df_changed[['internal_id', 'avalaible', 'archived_date']].to_sql(
        temp_table, con=engine, schema='silver', if_exists='replace', index=False
    )
    log.info(f"Updating {len(df_changed)} CHANGED rows in silver.standartize_silver via UPDATE JOIN...")
    update_sql = f"""
        UPDATE silver.standartize_silver t
        SET
            avalaible = tmp.avalaible,
            archived_date = tmp.archived_date
        FROM silver.{temp_table} tmp
        WHERE t.internal_id = tmp.internal_id
    """
    with engine.begin() as conn:
        conn.execute(text(update_sql))
        conn.execute(text(f"DROP TABLE silver.{temp_table}"))

# ========== Summary ==========
log.info(f"Summary: {len(df_new)} added, {len(df_changed)} updated.")
print("="*60)
print(f"Summary for {date.today()}:")
print(f"Rows added: {len(df_new)}")
print(f"Rows changed (availability/archive): {len(df_changed)}")
print("="*60)