import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Database configuration
DB_URL = os.getenv('DATABASE_URL')
if not DB_URL:
    logger.error("DATABASE_URL environment variable not found. Using default connection string")
    DB_URL = "postgresql://etl_user:etl_password@localhost:5432/etl_db"

def create_tables(engine):
    """Create all necessary database tables."""
    try:
        with engine.connect() as conn:
            # Create tables if they don't exist
            tables = [
                """
                CREATE TABLE IF NOT EXISTS dim_retailer (
                    retailer_id VARCHAR(50) PRIMARY KEY,
                    retailer_name VARCHAR(255),
                    created_at DATE DEFAULT CURRENT_DATE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS dim_store (
                    store_id VARCHAR(50) PRIMARY KEY,
                    retailer_id VARCHAR(50) REFERENCES dim_retailer(retailer_id),
                    store_name VARCHAR(100),
                    location VARCHAR(100),
                    created_at DATE DEFAULT CURRENT_DATE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS dim_sku (
                    sku_id VARCHAR(50) PRIMARY KEY,
                    brand VARCHAR(100),
                    sku_name VARCHAR(255),
                    category VARCHAR(100),
                    department VARCHAR(100),
                    pack_size VARCHAR(50),
                    created_at DATE DEFAULT CURRENT_DATE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS dim_time (
                    date DATE PRIMARY KEY,
                    day_of_week INTEGER,
                    day_name VARCHAR(10),
                    week_number INTEGER,
                    month_number INTEGER,
                    month_name VARCHAR(10),
                    quarter INTEGER,
                    year INTEGER,
                    is_weekend BOOLEAN
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS fact_sales (
                    retailer_id VARCHAR(50) REFERENCES dim_retailer(retailer_id),
                    store_id VARCHAR(50) REFERENCES dim_store(store_id),
                    sku_id VARCHAR(50) REFERENCES dim_sku(sku_id),
                    date DATE REFERENCES dim_time(date),
                    units_sold INTEGER,
                    sales_value NUMERIC(12, 2),
                    stock_level INTEGER,
                    promo_active BOOLEAN DEFAULT FALSE,
                    price NUMERIC(10, 2),
                    cost NUMERIC(10, 2),
                    created_at DATE DEFAULT CURRENT_DATE,
                    PRIMARY KEY (retailer_id, store_id, sku_id, date)
                )
                """
            ]

            for table_sql in tables:
                try:
                    conn.execute(text(table_sql))
                    conn.commit()
                    logger.info(f"Created table successfully")
                except SQLAlchemyError as e:
                    logger.error(f"Error creating table: {str(e)}")
                    conn.rollback()

            # Create default retailer
            try:
                conn.execute(
                    text("""
                    INSERT INTO dim_retailer (retailer_id, retailer_name)
                    VALUES ('DEFAULT', 'Default Retailer')
                    ON CONFLICT (retailer_id) DO NOTHING
                    """)
                )
                conn.commit()
                logger.info("Created default retailer")
            except SQLAlchemyError as e:
                logger.error(f"Error creating default retailer: {str(e)}")
                conn.rollback()

    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        engine = create_engine(DB_URL)
        logger.info("Initializing database schema...")
        create_tables(engine)
        logger.info("Database schema initialization completed successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {str(e)}")
        raise
