import os
import sys
import json
import psycopg2
import logging

# === LOGGING SETUP ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
log = logging.getLogger(__name__)

# === CONFIG LOADING ===
def load_config(config_path="config.json"):
    if not os.path.exists(config_path):
        log.error(f"Config file {config_path} not found.")
        sys.exit(1)
    with open(config_path, "r") as f:
        return json.load(f)

# === MAIN COPY FUNCTION ===
def copy_table(cfg):
    remote = cfg["remote_db"]
    local = cfg["local_db"]
    schema = cfg.get("schema", "public")
    table = cfg["table"]
    batch_size = cfg.get("batch_size", 1000)

    remote_conn = None
    local_conn = None

    try:
        # === CONNECTIONS ===
        log.info("Connecting to remote database...")
        remote_conn = psycopg2.connect(**remote)
        log.info("Connecting to local database...")
        local_conn = psycopg2.connect(**local)

        # === GET TABLE STRUCTURE ===
        with remote_conn.cursor() as c:
            c.execute("""
                SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod),
                    CASE WHEN a.attnotnull = false THEN 'YES' ELSE 'NO' END,
                    pg_get_expr(d.adbin, d.adrelid)
                FROM pg_catalog.pg_attribute a
                LEFT JOIN pg_catalog.pg_attrdef d ON (a.attrelid, a.attnum) = (d.adrelid, d.adnum)
                WHERE a.attrelid = (
                    SELECT c.oid FROM pg_catalog.pg_class c
                    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = %s AND n.nspname = %s
                ) AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum;
            """, (table, schema))
            columns = c.fetchall()
            if not columns:
                log.error(f"Table {schema}.{table} not found in remote DB")
                return False

        # === CREATE TABLE IF NOT EXISTS ===
        with local_conn.cursor() as c:
            c.execute("""
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            """, (schema, table))
            if c.fetchone() is None:
                log.info(f"Creating table {schema}.{table} in local DB...")
                col_defs = []
                for name, typ, nullable, default in columns:
                    s = f'"{name}" {typ}'
                    if nullable == 'NO':
                        s += " NOT NULL"
                    if default is not None:
                        s += f" DEFAULT {default}"
                    col_defs.append(s)
                create_sql = f'CREATE TABLE "{schema}"."{table}" (\n  {", ".join(col_defs)}\n)'
                c.execute(create_sql)
                local_conn.commit()
                log.info("Table created.")
            else:
                log.info(f"Table {schema}.{table} already exists.")

        # === GET COLUMN NAMES ===
        with remote_conn.cursor() as c:
            c.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table))
            col_names = [r[0] for r in c.fetchall()]
        columns_str = ', '.join(f'"{col}"' for col in col_names)

        # === COUNT ROWS ===
        with remote_conn.cursor() as c:
            c.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            total_rows = c.fetchone()[0]
            log.info(f"Total rows to copy: {total_rows}")

        # === DATA COPY ===
        offset = 0
        total_copied = 0
        placeholders = ', '.join(['%s'] * len(col_names))
        insert_sql = f'INSERT INTO "{schema}"."{table}" ({columns_str}) VALUES ({placeholders})'

        while offset < total_rows:
            with remote_conn.cursor() as c:
                c.execute(
                    f'SELECT {columns_str} FROM "{schema}"."{table}" LIMIT %s OFFSET %s',
                    (batch_size, offset)
                )
                batch = c.fetchall()
                if not batch:
                    break
            with local_conn.cursor() as c:
                c.executemany(insert_sql, batch)
            local_conn.commit()
            total_copied += len(batch)
            offset += batch_size
            log.info(f"Copied {total_copied}/{total_rows} rows ({(total_copied / total_rows) * 100:.2f}%)")

        log.info(f"Successfully copied {total_copied} rows to local database")
        return True

    except Exception as e:
        log.error(f"Error copying table: {e}")
        if local_conn and local_conn.status != psycopg2.extensions.STATUS_READY:
            local_conn.rollback()
        return False
    finally:
        if remote_conn:
            remote_conn.close()
            log.info("Remote connection closed")
        if local_conn:
            local_conn.close()
            log.info("Local connection closed")

# === ENTRY POINT ===
def main():
    cfg = load_config()
    result = copy_table(cfg)
    if result:
        log.info("Table copy completed successfully")
        sys.exit(0)
    else:
        log.error("Table copy failed")
        sys.exit(1)

if __name__ == "__main__":
    main()