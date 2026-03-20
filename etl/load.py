"""
load.py
-------
Handles all database interactions.
Connects to PostgreSQL and inserts transformed crypto records.
"""

import os
import logging
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_db_connection():
    """
    Create and return a PostgreSQL connection using environment variables.
    
    Required environment variables:
        DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            dbname=os.getenv("DB_NAME", "crypto_db"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "")
        )
        logger.info("[LOAD] Database connection established.")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"[LOAD] Failed to connect to database: {e}")
        raise


def insert_crypto_records(records: list[dict]) -> int:
    """
    Insert a batch of transformed crypto records into PostgreSQL.

    Uses execute_batch for efficient bulk insertion.

    Args:
        records: List of transformed coin dictionaries.

    Returns:
        Number of rows successfully inserted.
    """
    if not records:
        logger.warning("[LOAD] No records to insert.")
        return 0

    insert_query = """
        INSERT INTO crypto_prices (
            coin_id, coin_name, symbol,
            current_price_usd, market_cap, total_volume,
            price_change_24h, price_change_percentage_24h,
            high_24h, low_24h, circulating_supply, fetched_at
        ) VALUES (
            %(coin_id)s, %(coin_name)s, %(symbol)s,
            %(current_price_usd)s, %(market_cap)s, %(total_volume)s,
            %(price_change_24h)s, %(price_change_percentage_24h)s,
            %(high_24h)s, %(low_24h)s, %(circulating_supply)s, %(fetched_at)s
        );
    """

    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cursor:
                psycopg2.extras.execute_batch(cursor, insert_query, records)
        logger.info(f"[LOAD] Successfully inserted {len(records)} records.")
        return len(records)
    except psycopg2.Error as e:
        logger.error(f"[LOAD] Database insertion error: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()
        logger.info("[LOAD] Database connection closed.")


def init_database():
    """
    Run the SQL schema file to create tables if they don't exist.
    Call this once on first setup.
    """
    sql_path = os.path.join(os.path.dirname(__file__), "../sql/create_tables.sql")

    conn = get_db_connection()
    try:
        with open(sql_path, "r") as f:
            sql = f.read()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
        logger.info("[LOAD] Database initialised successfully.")
    except Exception as e:
        logger.error(f"[LOAD] Error initialising database: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    init_database()
    print("Database ready.")
      
