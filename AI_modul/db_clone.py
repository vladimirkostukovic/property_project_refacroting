import psycopg2
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Connection parameters
REMOTE_HOST = "176.102.66.84"
REMOTE_PORT = 5432
REMOTE_DB = "reality_data"
REMOTE_USER = "scrapper"
REMOTE_PASSWORD = "DR0IKJw7UrEbZkQK"

LOCAL_HOST = "localhost"
LOCAL_PORT = 5432
LOCAL_DB = "sreality"
LOCAL_USER = "postgres"
LOCAL_PASSWORD = "postgres"

TABLE_NAME = "image_links"
SCHEMA_NAME = "public"


def copy_table():
    """Copy table from remote database to local database."""
    remote_conn = None
    local_conn = None

    try:
        # Connect to remote database
        logger.info("Connecting to remote database...")
        remote_conn = psycopg2.connect(
            host=REMOTE_HOST,
            port=REMOTE_PORT,
            dbname=REMOTE_DB,
            user=REMOTE_USER,
            password=REMOTE_PASSWORD
        )

        # Connect to local database
        logger.info("Connecting to local database...")
        local_conn = psycopg2.connect(
            host=LOCAL_HOST,
            port=LOCAL_PORT,
            dbname=LOCAL_DB,
            user=LOCAL_USER,
            password=LOCAL_PASSWORD
        )

        # Get detailed table structure from remote database using pg_catalog
        with remote_conn.cursor() as cursor:
            logger.info(f"Retrieving table structure for {SCHEMA_NAME}.{TABLE_NAME}...")
            cursor.execute("""
                SELECT 
                    a.attname AS column_name,
                    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                    CASE 
                        WHEN a.attnotnull = false THEN 'YES' 
                        ELSE 'NO' 
                    END AS is_nullable,
                    pg_get_expr(d.adbin, d.adrelid) AS column_default
                FROM 
                    pg_catalog.pg_attribute a
                LEFT JOIN 
                    pg_catalog.pg_attrdef d ON (a.attrelid, a.attnum) = (d.adrelid, d.adnum)
                WHERE 
                    a.attrelid = (
                        SELECT c.oid 
                        FROM pg_catalog.pg_class c
                        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relname = %s AND n.nspname = %s
                    )
                    AND a.attnum > 0
                    AND NOT a.attisdropped
                ORDER BY 
                    a.attnum;
            """, (TABLE_NAME, SCHEMA_NAME))
            columns = cursor.fetchall()

            if not columns:
                raise Exception(f"Table {SCHEMA_NAME}.{TABLE_NAME} not found in remote database")

        # Check if table exists in local database
        with local_conn.cursor() as cursor:
            cursor.execute("""
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            """, (SCHEMA_NAME, TABLE_NAME))

            table_exists = cursor.fetchone() is not None

        # Create table in local database if it doesn't exist
        if not table_exists:
            logger.info(f"Creating table {SCHEMA_NAME}.{TABLE_NAME} in local database...")

            with local_conn.cursor() as cursor:
                # Create column definitions
                column_defs = []
                for column_name, data_type, is_nullable, default in columns:
                    column_def = f'"{column_name}" {data_type}'

                    # Add nullable constraint
                    if is_nullable == 'NO':
                        column_def += " NOT NULL"

                    # Add default value
                    if default is not None:
                        column_def += f" DEFAULT {default}"

                    column_defs.append(column_def)

                # Create table
                create_table_query = f"""
                    CREATE TABLE "{SCHEMA_NAME}"."{TABLE_NAME}" (
                        {", ".join(column_defs)}
                    )
                """
                cursor.execute(create_table_query)
                local_conn.commit()
                logger.info(f"Table {SCHEMA_NAME}.{TABLE_NAME} created successfully")
        else:
            logger.info(f"Table {SCHEMA_NAME}.{TABLE_NAME} already exists in local database")

        # Get column names
        with remote_conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (SCHEMA_NAME, TABLE_NAME))
            column_names = [row[0] for row in cursor.fetchall()]
            columns_str = ', '.join(f'"{col}"' for col in column_names)

        # Count rows to be copied
        with remote_conn.cursor() as cursor:
            cursor.execute(f'SELECT COUNT(*) FROM "{SCHEMA_NAME}"."{TABLE_NAME}"')
            total_rows = cursor.fetchone()[0]
            logger.info(f"Found {total_rows} rows to copy")

        # Copy data in batches
        batch_size = 1000
        offset = 0
        total_copied = 0

        # Prepare insert query with placeholders
        placeholders = ', '.join(['%s'] * len(column_names))
        insert_query = f'INSERT INTO "{SCHEMA_NAME}"."{TABLE_NAME}" ({columns_str}) VALUES ({placeholders})'

        while offset < total_rows:
            with remote_conn.cursor() as remote_cursor:
                # Fetch a batch of data
                remote_cursor.execute(
                    f'SELECT {columns_str} FROM "{SCHEMA_NAME}"."{TABLE_NAME}" LIMIT %s OFFSET %s',
                    (batch_size, offset)
                )
                batch = remote_cursor.fetchall()

                if not batch:
                    break

                batch_count = len(batch)

            # Insert data into local database
            with local_conn.cursor() as local_cursor:
                local_cursor.executemany(insert_query, batch)
            local_conn.commit()

            # Update counters
            total_copied += batch_count
            offset += batch_size

            logger.info(f"Copied {total_copied}/{total_rows} rows ({(total_copied / total_rows) * 100:.2f}%)")

        logger.info(f"Successfully copied {total_copied} rows to local database")
        return True

    except Exception as e:
        logger.error(f"Error copying table: {e}")
        if local_conn and local_conn.status != psycopg2.extensions.STATUS_READY:
            local_conn.rollback()
        return False

    finally:
        # Close connections
        if remote_conn:
            remote_conn.close()
            logger.info("Remote connection closed")

        if local_conn:
            local_conn.close()
            logger.info("Local connection closed")


def main():
    try:
        success = copy_table()
        if success:
            logger.info("Table copy completed successfully")
            sys.exit(0)
        else:
            logger.error("Table copy failed")
            sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)


if __name__ == "__main__":
    main()