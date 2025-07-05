import os
import re
import sys
import logging
import argparse
import pandas as pd
import numpy as np
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    Date,
    DateTime,
    Boolean,
    text,
    Table,
    MetaData,
    inspect,
    event,
    func,
    Numeric,
    ForeignKey,
    case,
)
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timedelta
import traceback
from typing import Dict, List, Optional, Tuple


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("logs/etl_pipeline.log")],
)
logger = logging.getLogger(__name__)

# Database configuration
DB_URL = "postgresql://postgres:postgres@localhost:5435/etl_db"
DEFAULT_RETAILER_ID = "DEFAULT_RETAILER"
DEFAULT_RETAILER_NAME = "Default Retailer"

# Define the database schema using SQLAlchemy ORM
Base = declarative_base()


class DimRetailer(Base):
    __tablename__ = "dim_retailer"
    retailer_id = Column(String(50), primary_key=True)
    retailer_name = Column(String(100))
    created_at = Column(Date, default=datetime.utcnow)


class DimStore(Base):
    __tablename__ = "dim_store"
    store_id = Column(String(50), primary_key=True)
    retailer_id = Column(String(50), ForeignKey("dim_retailer.retailer_id"))
    store_name = Column(String(100))
    location = Column(String(100))
    created_at = Column(Date, default=datetime.utcnow)


class DimSku(Base):
    __tablename__ = "dim_sku"
    sku_id = Column(String(50), primary_key=True)
    brand = Column(String(100))
    sku_name = Column(String(255))
    category = Column(String(100))
    department = Column(String(100))
    pack_size = Column(String(50))
    created_at = Column(Date, default=datetime.utcnow)


class DimTime(Base):
    __tablename__ = "dim_time"
    date = Column(Date, primary_key=True)
    day_of_week = Column(Integer)
    day_name = Column(String(10))
    week_number = Column(Integer)
    month_number = Column(Integer)
    month_name = Column(String(10))
    quarter = Column(Integer)
    year = Column(Integer)
    is_weekend = Column(Boolean)


class FactSales(Base):
    __tablename__ = "fact_sales"
    retailer_id = Column(
        String(50), ForeignKey("dim_retailer.retailer_id"), primary_key=True
    )
    store_id = Column(String(50), ForeignKey("dim_store.store_id"), primary_key=True)
    sku_id = Column(String(50), ForeignKey("dim_sku.sku_id"), primary_key=True)
    date = Column(Date, ForeignKey("dim_time.date"), primary_key=True)
    units_sold = Column(Integer)
    sales_value = Column(Numeric(12, 2))
    stock_level = Column(Integer)
    promo_active = Column(Boolean, default=False)
    price = Column(Numeric(10, 2))
    cost = Column(Numeric(10, 2))
    created_at = Column(Date, default=datetime.utcnow)


def ensure_retailer_exists(engine, retailer_id):
    """Ensure the retailer exists in the database, create if it doesn't."""
    logger.info(f"Ensuring retailer exists: {retailer_id}")
    with engine.connect() as conn:
        # First, check if the table exists
        result = conn.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'dim_retailer'
                )
            """
            )
        )
        table_exists = result.scalar()

        if not table_exists:
            # Create the table if it doesn't exist
            logger.info("Creating dim_retailer table...")
            conn.execute(
                text(
                    """
                CREATE TABLE dim_retailer (
                    retailer_id VARCHAR(50) PRIMARY KEY,
                    retailer_name VARCHAR(255),
                    created_date DATE DEFAULT CURRENT_DATE
                )
            
            """
                )
            )
            conn.commit()

        # Check if retailer exists
        result = conn.execute(
            text("SELECT COUNT(*) FROM dim_retailer WHERE retailer_id = :retailer_id"),
            {"retailer_id": retailer_id},
        )
        count = result.scalar()

        if count == 0:
            # Insert the retailer
            logger.info(f"Creating new retailer with ID: {retailer_id}")
            conn.execute(
                text(
                    """
                    INSERT INTO dim_retailer (retailer_id, retailer_name)
                    VALUES (:retailer_id, :retailer_name)
                """
                ),
                {
                    "retailer_id": retailer_id,
                    "retailer_name": f"Retailer {retailer_id}",
                },
            )
            conn.commit()
            logger.info(f"Successfully created retailer: {retailer_id}")
        else:
            logger.info(f"Found existing retailer: {retailer_id}")


def ensure_dimension_tables(engine, df):
    """Ensure all dimension tables exist and are populated."""
    with engine.connect() as conn:
        # Create dim_sku if it doesn't exist
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS dim_sku (
                sku_id VARCHAR(50) PRIMARY KEY,
                item_name VARCHAR(255),
                category VARCHAR(255),
                department VARCHAR(255),
                supplier_name VARCHAR(255),
                rrp DECIMAL(10, 2),
                cost DECIMAL(10, 2),
                created_date DATE DEFAULT CURRENT_DATE
            )
        """
            )
        )

        # Create dim_store if it doesn't exist
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS dim_store (
                store_id VARCHAR(50) PRIMARY KEY,
                store_name VARCHAR(255),
                created_date DATE DEFAULT CURRENT_DATE
            )
        
        """
            )
        )

        # Populate dim_sku with unique SKUs from the data
        # Handle both uppercase and lowercase column names
        sku_columns = {
            "id_col": "ITEM_CODE" if "ITEM_CODE" in df.columns else "sku_id",
            "name_col": "ITEM_NAME" if "ITEM_NAME" in df.columns else "item_name",
            "category_col": "CATEGORY" if "CATEGORY" in df.columns else "category",
            "dept_col": "DEPARTMENT" if "DEPARTMENT" in df.columns else "department",
            "supplier_col": (
                "SUPPLIER_NAME" if "SUPPLIER_NAME" in df.columns else "supplier_name"
            ),
            "rrp_col": "RRP" if "RRP" in df.columns else "rrp",
            "cost_col": "COST" if "COST" in df.columns else "cost",
        }

        required_cols = [sku_columns["id_col"], sku_columns["name_col"]]
        if all(col in df.columns for col in required_cols):
            # Get all columns that exist in the dataframe
            available_cols = [
                col
                for col in [
                    sku_columns["id_col"],
                    sku_columns["name_col"],
                    sku_columns["category_col"],
                    sku_columns["dept_col"],
                    sku_columns["supplier_col"],
                    sku_columns["rrp_col"],
                    sku_columns["cost_col"],
                ]
                if col in df.columns
            ]

            # Get unique SKUs that don't exist in dim_sku
            skus_to_insert = df[available_cols].drop_duplicates(
                subset=[sku_columns["id_col"]]
            )

            # Convert to list of dicts for batch insert
            rename_map = {
                sku_columns["id_col"]: "sku_id",
                sku_columns["name_col"]: "item_name",
                sku_columns["category_col"]: "category",
                sku_columns["dept_col"]: "department",
                sku_columns["supplier_col"]: "supplier_name",
                sku_columns["rrp_col"]: "rrp",
                sku_columns["cost_col"]: "cost",
            }

            # Only include columns that exist in the dataframe
            rename_map = {
                k: v for k, v in rename_map.items() if k in skus_to_insert.columns
            }
            sku_records = skus_to_insert.rename(columns=rename_map).to_dict("records")

            # Insert new SKUs
            if sku_records:
                conn.execute(
                    text(
                        """
                        INSERT INTO dim_sku (sku_id, item_name, category, department, supplier_name, rrp, cost)
                        VALUES (:sku_id, :item_name, :category, :department, :supplier_name, :rrp, :cost)
                        ON CONFLICT (sku_id) DO NOTHING
                    """
                    ),
                    sku_records,
                )
                conn.commit()
                logger.info(f"Inserted/updated {len(sku_records)} SKUs in dim_sku")

        # Populate dim_store with unique stores from the data
        # Handle both uppercase and lowercase column names
        store_id_col = "STORE_ID" if "STORE_ID" in df.columns else "store_id"
        store_name_col = "STORE_NAME" if "STORE_NAME" in df.columns else "store_name"

        if store_id_col in df.columns and store_name_col in df.columns:
            # Get unique stores that don't exist in dim_store
            stores_to_insert = df[[store_id_col, store_name_col]].drop_duplicates(
                subset=[store_id_col]
            )

            # Convert to list of dicts for batch insert
            store_records = stores_to_insert.rename(
                columns={store_id_col: "store_id", store_name_col: "store_name"}
            ).to_dict("records")

            # Insert new stores
            if store_records:
                conn.execute(
                    text(
                        """
                        INSERT INTO dim_store (store_id, store_name)
                        VALUES (:store_id, :store_name)
                        ON CONFLICT (store_id) DO NOTHING
                    """
                    ),
                    store_records,
                )
                conn.commit()
                logger.info(
                    f"Inserted/updated {len(store_records)} stores in dim_store"
                )


def update_database_schema(engine):
    """Ensure the database schema is up to date with required columns."""
    try:
        with engine.connect() as conn:
            # List of columns to check/add
            columns_to_add = [("stock_level", "INTEGER"), ("cost", "NUMERIC(10,2)")]

            # Check each column
            for column_name, column_type in columns_to_add:
                result = conn.execute(
                    text(
                        f"""
                    SELECT EXISTS (
                        SELECT 1 
                        FROM information_schema.columns 
                        WHERE table_name = 'fact_sales' 
                        AND column_name = '{column_name}'
                    )
                    """
                    )
                )
                column_exists = result.scalar()

                if not column_exists:
                    logger.info(
                        f"Adding missing '{column_name}' column to fact_sales table"
                    )
                    conn.execute(
                        text(
                            f"ALTER TABLE fact_sales ADD COLUMN {column_name} {column_type}"
                        )
                    )
                    conn.commit()
                    logger.info(f"Successfully added '{column_name}' column")

    except Exception as e:
        logger.error(f"Error updating database schema: {e}")
        raise


def create_db_connection(
    host="localhost",
    port=5432,
    database="etl_db",
    user="postgres",
    password="postgres",
    **kwargs,
):
    """
    Create and return a database connection.

    Args:
        host: Database host
        port: Database port
        database: Database name
        user: Database user
        password: Database password
        **kwargs: Additional connection parameters

    Returns:
        SQLAlchemy engine instance or None if connection fails
    """
    try:
        # Construct the database URL
        db_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"

        # Create engine with connection parameters
        engine = create_engine(db_url, **kwargs)

        # Test the connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        # Create tables if they don't exist
        Base.metadata.create_all(engine)

        # Update schema if needed
        update_database_schema(engine)

        logger.info(f"Successfully connected to database: {database} on {host}:{port}")
        return engine

    except Exception as e:
        logger.error(f"Error creating database connection: {e}")
        return None


def clean_text(text: str) -> str:
    """Clean and standardize text data."""
    if not isinstance(text, str) or pd.isna(text):
        return ""
    # Remove extra whitespace and convert to title case
    return " ".join(str(text).strip().split()).title()


def extract_pack_size(sku_name: str) -> str:
    """Extract pack size from SKU name."""
    if not isinstance(sku_name, str):
        return ""
    # Look for patterns like 100g, 1kg, 500ml, etc.
    match = re.search(
        r"(\d+\s*(?:g|kg|ml|l|oz|lb|pcs?|pack|pk|ct))", sku_name, re.IGNORECASE
    )
    return match.group(1) if match else ""


def validate_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Validate the input DataFrame for data quality issues.

    Args:
        df: Input DataFrame to validate

    Returns:
        Tuple of (validated DataFrame, validation_stats)
    """
    validation_stats = {
        "total_records": len(df),
        "missing_required_fields": 0,
        "invalid_numeric_values": 0,
        "negative_values": 0,
        "invalid_dates": 0,
        "duplicate_records": 0,
    }

    # Make a copy to avoid modifying the original
    df_validated = df.copy()

    # 1. Map actual column names to expected names
    column_mapping = {
        "STORE_NAME": "STORE_ID",
        "ITEM_CODE": "ITEM_CODE",
        "QTY": "QTY",
        "SALES_PRE_VAT": "SALES_AMOUNT",
        "Date": "DATE",
    }

    # Rename columns to standardize them
    df_validated = df_validated.rename(columns=column_mapping)

    # 2. Check for required fields
    required_fields = ["STORE_ID", "ITEM_CODE", "QTY", "SALES_AMOUNT", "DATE"]

    # Check which required fields are missing
    missing_columns = [
        col for col in required_fields if col not in df_validated.columns
    ]
    if missing_columns:
        logger.warning(f"Missing required columns: {', '.join(missing_columns)}")

    # Check for null values in existing required columns
    existing_required = [col for col in required_fields if col in df_validated.columns]
    if existing_required:
        missing_required = df_validated[existing_required].isnull().any(axis=1)
        validation_stats["missing_required_fields"] = missing_required.sum()
    else:
        validation_stats["missing_required_fields"] = len(df_validated)

    # 2. Check for invalid numeric values (negative values where not allowed)
    numeric_fields = ["QTY", "SALES_AMOUNT", "PRICE", "COST", "STOCK_LEVEL"]

    # First, ensure all numeric fields are actually numeric
    for field in numeric_fields:
        if field in df_validated.columns:
            # Convert to string, clean, then to numeric
            df_validated[field] = (
                df_validated[field].astype(str).str.replace(r"[^\d.-]", "", regex=True)
            )
            df_validated[field] = pd.to_numeric(df_validated[field], errors="coerce")
            # Fill any remaining NaN with 0
            df_validated[field] = df_validated[field].fillna(0)

    # Now check for negative values in non-negative fields
    non_negative_fields = ["QTY", "SALES_AMOUNT", "PRICE", "STOCK_LEVEL"]
    for field in non_negative_fields:
        if field in df_validated.columns:
            # Check for negative values
            negative_mask = df_validated[field] < 0
            if negative_mask.any():
                validation_stats["negative_values"] += negative_mask.sum()
                # Replace negative values with 0
                df_validated.loc[negative_mask, field] = 0

    # 3. Validate dates
    if "Date" in df_validated.columns:
        df_validated["date_parsed"] = pd.to_datetime(
            df_validated["Date"], errors="coerce"
        )
        invalid_dates = df_validated["date_parsed"].isna()
        validation_stats["invalid_dates"] = invalid_dates.sum()

        # Remove rows with invalid dates
        df_validated = df_validated[~invalid_dates].copy()

    # 4. Remove exact duplicates
    duplicate_mask = df_validated.duplicated()
    validation_stats["duplicate_records"] = duplicate_mask.sum()
    df_validated = df_validated[~duplicate_mask].copy()

    # Log validation results
    logger.info("Data Validation Results:")
    for stat, count in validation_stats.items():
        logger.info(f"  - {stat.replace('_', ' ').title()}: {count:,}")

    return df_validated, validation_stats


def ingest_and_clean_data(file_path: str) -> pd.DataFrame:
    """
    Ingest, validate, and transform data from the CSV file.

    This function performs the following steps:
    1. Reads the CSV file with appropriate data types
    2. Validates the data for quality issues
    3. Transforms the data into the target schema

    Args:
        file_path: Path to the CSV file

    Returns:
        DataFrame: Processed DataFrame ready for database insertion

    Raises:
        Exception: If there's an error during the process
    """
    logger.info(f"Starting data ingestion from {file_path}")

    try:
        # Define expected columns and their data types
        dtypes = {
            "STORE_NAME": "str",
            "ITEM_CODE": "str",
            "ITEM_NAME": "str",
            "CATEGORY": "str",
            "DEPARTMENT": "str",
            "QTY": "str",  # Will be converted to float after cleaning
            "COST": "str",  # Will be converted to float after cleaning
            "SALES_PRE_VAT": "str",  # Will be converted to float after cleaning
            "SUPPLIER_NAME": "str",
            "Date": "str",
            "RRP": "str",  # Will be converted to float after cleaning
            "PRICE": "str",  # Will be converted to float after cleaning
            "TRANS_DATE": "str",
            "PROMO_FLAG": "str",
            "STOCK_LEVEL": "str",  # Will be converted to float after cleaning
            "STOCK": "str",  # Will be converted to float after cleaning
        }

        # Read CSV file with error handling for different encodings
        logger.info("Reading CSV file...")
        try:
            df = pd.read_csv(
                file_path,
                dtype=dtypes,
                low_memory=False,
                encoding="utf-8-sig",
                thousands=",",
            )
        except UnicodeDecodeError:
            logger.info("UTF-8 with BOM failed, trying latin-1 encoding...")
            df = pd.read_csv(
                file_path,
                dtype=dtypes,
                low_memory=False,
                encoding="latin1",
                thousands=",",
            )

        # Clean and convert numeric columns
        numeric_columns = [
            "QTY",
            "COST",
            "SALES_PRE_VAT",
            "RRP",
            "PRICE",
            "STOCK_LEVEL",
            "STOCK",
        ]
        for col in numeric_columns:
            if col in df.columns:
                # Remove any non-numeric characters except decimal point and negative sign
                df[col] = df[col].astype(str).str.replace(r"[^\d.-]", "", regex=True)
                # Convert to numeric, coerce errors to NaN
                df[col] = pd.to_numeric(df[col], errors="coerce")
                # Fill NaN with 0 for numeric columns
                df[col] = df[col].fillna(0)

        logger.info(f"Successfully read {len(df):,} rows from {file_path}")

        # Clean up column names (remove extra spaces, BOM, and standardize)
        logger.info("Standardizing column names...")
        df.columns = df.columns.str.strip()
        df.columns = df.columns.str.replace("\ufeff", "")  # Remove BOM

        # Perform data validation
        logger.info("Starting data validation...")
        df, validation_stats = validate_data(df)

        # Log validation summary
        logger.info("\n=== Data Validation Summary ===")
        logger.info(f"Total records processed: {validation_stats['total_records']:,}")

        if validation_stats["missing_required_fields"] > 0:
            logger.warning(
                f"  - Records with missing required fields: {validation_stats['missing_required_fields']:,}"
            )
        if validation_stats["invalid_numeric_values"] > 0:
            logger.warning(
                f"  - Records with invalid numeric values: {validation_stats['invalid_numeric_values']:,}"
            )
        if validation_stats["negative_values"] > 0:
            logger.warning(
                f"  - Records with unexpected negative values: {validation_stats['negative_values']:,}"
            )
        if validation_stats["invalid_dates"] > 0:
            logger.warning(
                f"  - Records with invalid dates: {validation_stats['invalid_dates']:,}"
            )
        if validation_stats["duplicate_records"] > 0:
            logger.warning(
                f"  - Duplicate records removed: {validation_stats['duplicate_records']:,}"
            )

        valid_records = len(df)
        logger.info(
            f"\nValid records remaining: {valid_records:,} "
            f"({valid_records/validation_stats['total_records']*100:.1f}% of original)"
        )
        logger.info("================================\n")

        if valid_records == 0:
            logger.warning("No valid records remaining after validation!")
            return pd.DataFrame()

        # Add retailer_id to the dataframe if it doesn't exist
        if "RETAILER_ID" not in df.columns and "retailer_id" not in df.columns:
            # Get retailer_id from command line arguments if available
            try:
                args = parse_arguments()
                df["retailer_id"] = args.retailer_id
                logger.info(f"Added retailer_id: {args.retailer_id} to the data")
            except:
                logger.warning("Could not determine retailer_id, using default")
                df["retailer_id"] = "DEFAULT_RETAILER"

        # Transform the data into the target schema
        logger.info("Starting data transformation...")
        df = transform_data(df)

        logger.info("Data ingestion and transformation completed successfully!")
        return df

    except pd.errors.EmptyDataError:
        error_msg = f"The file {file_path} is empty"
        logger.error(error_msg)
        raise ValueError(error_msg)
    except FileNotFoundError:
        error_msg = f"The file {file_path} was not found"
        logger.error(error_msg)
        raise
    except PermissionError:
        error_msg = f"Permission denied when accessing {file_path}"
        logger.error(error_msg)
        raise
    except Exception as e:
        error_msg = f"Error during data ingestion and cleaning: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        raise


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the validated data into the target schema.

    Args:
        df: Validated DataFrame from validate_data()

    Returns:
        DataFrame: Transformed data ready for database insertion
    """
    logger.info("Starting data transformation")

    try:
        # Create a copy to avoid modifying the original
        df_transformed = df.copy()

        # Map source columns to target columns
        # Using RRP as price if PRICE is not available
        column_mapping = {
            "STORE_ID": "store_id",
            "ITEM_CODE": "sku_id",
            "QTY": "units_sold",
            "SALES_AMOUNT": "sales_value",
            "DATE": "date",
            "RRP": "price",  # Use RRP as price if PRICE is not available
            "COST": "cost",
            "STOCK": "stock_level",  # Use STOCK if STOCK_LEVEL is not available
            "RETAILER_ID": "retailer_id",  # Map RETAILER_ID to retailer_id
            "retailer_id": "retailer_id",  # Also handle if already in lowercase
        }

        # Initialize all required columns with None
        required_columns = [
            "store_id",
            "sku_id",
            "units_sold",
            "sales_value",
            "date",
            "price",
            "cost",
            "stock_level",
            "promo_active",
            "retailer_id",
        ]

        # Create a new DataFrame with all required columns
        result = pd.DataFrame(columns=required_columns)

        # Map and copy columns that exist in the source
        for src_col, tgt_col in column_mapping.items():
            if src_col in df_transformed.columns:
                result[tgt_col] = df_transformed[src_col]

        # If retailer_id is not in the source, try to get it from the command line args
        if (
            "retailer_id" not in result.columns
            and "RETAILER_ID" not in df_transformed.columns
        ):
            try:
                args = parse_arguments()
                result["retailer_id"] = args.retailer_id
                logger.info(f"Added retailer_id from command line: {args.retailer_id}")
            except Exception as e:
                logger.warning(f"Could not get retailer_id from command line: {e}")
                result["retailer_id"] = "DEFAULT_RETAILER"

        # Set default values for missing required columns
        if "promo_active" not in result.columns:
            result["promo_active"] = False

        # If price is still missing, try to calculate from sales_value/units_sold
        if (
            "price" not in result.columns
            and "sales_value" in result.columns
            and "units_sold" in result.columns
        ):
            result["price"] = result["sales_value"] / result["units_sold"].replace(0, 1)

        # Set default stock level if missing
        if "stock_level" not in result.columns:
            result["stock_level"] = 0

        # Set default cost if missing
        if "cost" not in result.columns:
            result["cost"] = 0

        # Convert date to datetime.date
        if "date" in result.columns:
            # First try to parse with specific format if possible
            try:
                result["date"] = pd.to_datetime(
                    result["date"], format="%d/%m/%Y", errors="coerce"
                )
                # If parsing failed for all, try without format
                if result["date"].isna().all():
                    result["date"] = pd.to_datetime(result["date"], errors="coerce")
                result["date"] = result["date"].dt.date
                # If still no dates, use today's date as fallback
                if result["date"].isna().all():
                    result["date"] = pd.Timestamp.today().date()
            except Exception as e:
                logger.warning(
                    f"Error parsing dates: {e}. Using today's date as fallback."
                )
                result["date"] = pd.Timestamp.today().date()
        else:
            # If date column is missing, use today's date
            result["date"] = pd.Timestamp.today().date()

        # Ensure promo_active is boolean
        if "promo_active" not in result.columns:
            result["promo_active"] = False
        result["promo_active"] = result["promo_active"].fillna(False).astype(bool)

        # Ensure numeric columns have the correct type and handle any non-numeric values
        numeric_columns = ["units_sold", "sales_value", "price", "cost", "stock_level"]
        for col in numeric_columns:
            if col in result.columns:
                # Convert to string first to handle any non-numeric characters
                result[col] = (
                    result[col].astype(str).str.replace(r"[^\d.-]", "", regex=True)
                )
                result[col] = pd.to_numeric(result[col], errors="coerce")
                result[col] = result[col].fillna(0)

        # Check for missing required columns
        missing_columns = [
            col
            for col in required_columns
            if col not in result.columns or result[col].isnull().all()
        ]
        if missing_columns:
            raise ValueError(
                f"Missing required columns after transformation: {', '.join(missing_columns)}"
            )

        logger.info(f"Successfully transformed {len(result)} records")
        return result

    except Exception as e:
        logger.error(f"Error during data transformation: {e}")
        logger.error(traceback.format_exc())
        raise
        df.columns = df.columns.str.strip()
        df.columns = df.columns.str.replace("\ufeff", "")  # Remove BOM

        # Define column mapping with case-insensitive matching
        column_mapping = {
            "STORE_NAME": "store_name",
            "STORE_ID": "store_id",
            "ITEM_CODE": "sku_id",
            "ITEM_NAME": "sku_name",
            "ITEM_DESC": "sku_name",  # Alternative column name
            "CATEGORY": "category",
            "DEPT": "department",
            "DEPARTMENT": "department",
            "QTY": "units_sold",
            "QUANTITY": "units_sold",
            "SALES_PRE_VAT": "sales_value",
            "SALES_AMOUNT": "sales_value",
            "RRP": "price",
            "PRICE": "price",
            "Date": "date",
            "TRANS_DATE": "date",
            "COST": "cost",
            "PROMO_FLAG": "promo_flag",
            "PROMOTION": "promo_flag",
            "STOCK_LEVEL": "stock_level",
            "STOCK": "stock_level",
        }

        # Apply case-insensitive column mapping
        df.columns = df.columns.str.upper()
        column_mapping = {k.upper(): v for k, v in column_mapping.items()}

        # Only include columns that exist in the dataframe
        column_mapping = {
            k: v
            for k, v in column_mapping.items()
            if k in df.columns
            and v
            in [
                "store_name",
                "store_id",
                "sku_id",
                "sku_name",
                "category",
                "department",
                "units_sold",
                "sales_value",
                "price",
                "date",
                "cost",
                "promo_flag",
                "stock_level",
            ]
        }

        # Rename columns
        df = df.rename(columns=column_mapping)

        # Add retailer information (default if not provided)
        df["retailer_id"] = DEFAULT_RETAILER_ID

        # Only include columns that exist in the dataframe
        column_mapping = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=column_mapping)

        # Standardize and clean text fields
        text_columns = [
            "store_name",
            "store_id",
            "sku_id",
            "sku_name",
            "category",
            "department",
        ]
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str).apply(clean_text)

        # Ensure sku_id is treated as string and clean it
        if "sku_id" not in df.columns and "ITEM_CODE" in df.columns:
            df["sku_id"] = df["ITEM_CODE"]
        df["sku_id"] = df["sku_id"].astype(str).str.strip()

        # Handle dates - try multiple date formats
        date_columns = ["date", "transaction_date", "sale_date"]
        for date_col in date_columns:
            if date_col in df.columns:
                try:
                    # Try parsing as full date first
                    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
                    # If that fails, try assuming it's a day number in May 2025
                    if df[date_col].isna().any():
                        df[date_col] = pd.to_datetime(
                            "2025-05-" + df[date_col].astype(str),
                            format="%Y-%m-%d",
                            errors="coerce",
                        )
                    break
                except:
                    continue

        # If still no valid date, use today's date
        if "date" not in df.columns or df["date"].isna().all():
            df["date"] = datetime.today()

        # Create store_id if not provided
        if "store_id" not in df.columns and "store_name" in df.columns:
            df["store_id"] = df["store_name"].apply(
                lambda x: str(abs(hash(str(x))))[-10:] if pd.notna(x) else None
            )

        # Ensure numeric columns are the right type and handle negatives
        numeric_cols = ["units_sold", "sales_value", "price", "cost", "stock_level"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                # Convert negative values to positive (assuming data entry error)
                if col in ["units_sold", "stock_level"]:
                    df[col] = df[col].abs()

        # Handle promo flag - convert various formats to boolean
        if "promo_flag" in df.columns:
            df["promo_active"] = (
                df["promo_flag"]
                .astype(str)
                .str.upper()
                .isin(["TRUE", "YES", "Y", "1", "PROMO"])
            )
        else:
            df["promo_active"] = False

        # Extract pack size from SKU name
        if "sku_name" in df.columns:
            df["pack_size"] = df["sku_name"].apply(extract_pack_size)
        else:
            df["pack_size"] = ""

        # Calculate derived metrics
        if (
            "price" not in df.columns
            and "sales_value" in df.columns
            and "units_sold" in df.columns
        ):
            df["price"] = df["sales_value"] / df["units_sold"].replace(0, np.nan)

        if "cost" not in df.columns and "price" in df.columns:
            # Default to 70% of price if cost not provided
            df["cost"] = df["price"] * 0.7

        if "stock_level" not in df.columns:
            # Default stock level to 0 if not provided
            df["stock_level"] = 0

        # Define required columns with fallbacks
        required_columns = [
            "store_id",
            "sku_id",
            "units_sold",
            "sales_value",
            "price",
            "date",
        ]

        # Log data quality issues before dropping
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.warning(f"Missing required columns: {', '.join(missing_columns)}")

        # Drop rows with missing values in key columns
        df_cleaned = df.copy()
        missing_before = len(df_cleaned)
        df_cleaned = df_cleaned.dropna(
            subset=[col for col in required_columns if col in df_cleaned.columns]
        )
        missing_after = len(df_cleaned)

        # Log data quality metrics
        logger.info("\n=== Data Quality Report ===")
        logger.info(f"Original rows: {len(df):,}")
        logger.info(f"Rows after cleaning: {len(df_cleaned):,}")
        logger.info(
            f"Rows dropped: {missing_before - missing_after:,} ({(missing_before - missing_after) / missing_before * 100:.1f}%)"
        )

        # Log sample of the data
        sample_cols = [
            "retailer_id",
            "store_id",
            "sku_id",
            "date",
            "units_sold",
            "sales_value",
            "price",
            "promo_active",
        ]
        sample_cols = [col for col in sample_cols if col in df_cleaned.columns]
        logger.info("\n=== Sample of cleaned data ===")
        logger.info(df_cleaned[sample_cols].head().to_string())

        # Log data types and memory usage
        logger.info("\n=== Data Types ===")
        logger.info(df_cleaned.dtypes)

        return df_cleaned

    except Exception as e:
        print(f"Error reading or cleaning data: {e}")
        traceback.print_exc()
        return None


def populate_time_dimension(engine, start_date, end_date):
    """Populate the time dimension table with dates between start_date and end_date."""
    try:
        logger.info(f"Populating time dimension from {start_date} to {end_date}")

        # Convert string dates to datetime if needed
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date).date()
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date).date()

        # Generate all dates in the range
        dates = pd.date_range(start=start_date, end=end_date, freq="D")

        # Prepare time dimension data
        time_data = []
        for date in dates:
            time_data.append(
                {
                    "date": date.date(),
                    "day_of_week": date.dayofweek + 1,  # 1-7 for Monday-Sunday
                    "day_name": date.strftime("%A"),
                    "week_number": date.isocalendar()[1],
                    "month_number": date.month,
                    "month_name": date.strftime("%B"),
                    "quarter": (date.month - 1) // 3 + 1,
                    "year": date.year,
                    "is_weekend": date.dayofweek >= 5,  # 5=Saturday, 6=Sunday
                }
            )

        if not time_data:
            logger.warning("No dates to insert into time dimension")
            return 0

        # Insert in batches
        batch_size = 1000
        total_inserted = 0

        with engine.connect() as conn:
            for i in range(0, len(time_data), batch_size):
                batch = time_data[i : i + batch_size]
                try:
                    result = conn.execute(
                        text(
                            """
                        INSERT INTO dim_time (
                            date, day_of_week, day_name, week_number, 
                            month_number, month_name, quarter, year, is_weekend
                        ) VALUES (
                            :date, :day_of_week, :day_name, :week_number,
                            :month_number, :month_name, :quarter, :year, :is_weekend
                        ) ON CONFLICT (date) DO NOTHING
                        """
                        ),
                        batch,
                    )
                    total_inserted += result.rowcount
                    conn.commit()

                    if i % (batch_size * 10) == 0:
                        logger.info(
                            f"  - Processed {min(i + len(batch), len(time_data))}/{len(time_data)} dates"
                        )

                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error inserting batch {i//batch_size + 1}: {e}")
                    # Try inserting rows one by one to identify problematic rows
                    for j, row in enumerate(batch):
                        try:
                            result = conn.execute(
                                text(
                                    """
                                INSERT INTO dim_time (
                                    date, day_of_week, day_name, week_number, 
                                    month_number, month_name, quarter, year, is_weekend
                                ) VALUES (
                                    :date, :day_of_week, :day_name, :week_number,
                                    :month_number, :month_name, :quarter, :year, :is_weekend
                                ) ON CONFLICT (date) DO NOTHING
                                """
                                ),
                                row,
                            )
                            total_inserted += result.rowcount
                            conn.commit()
                        except Exception as inner_e:
                            logger.error(
                                f"  - Failed to insert date {row.get('date')}: {inner_e}"
                            )
                            conn.rollback()
                            continue

        logger.info(f"Successfully inserted/updated {total_inserted} rows in dim_time")
        return total_inserted

    except Exception as e:
        logger.error(f"Error populating time dimension: {e}")
        if "conn" in locals():
            conn.rollback()
        raise


def insert_dimension_data(engine, df, table_name, id_col, value_cols, retailer_id=None):
    """
    Insert or update data in a dimension table with improved error handling and logging.

    Args:
        engine: SQLAlchemy engine
        df: DataFrame containing the data
        table_name: Name of the target table
        id_col: Name of the ID column
        value_cols: List of value columns to insert
        retailer_id: Optional retailer ID for tables that need it (like dim_store)

    Returns:
        Tuple of (total_inserted, total_errors)
    """
    total_inserted = 0
    total_errors = 0

    try:
        if df.empty:
            logger.warning(f"No data to insert into {table_name}")
            return 0, 0

        # Get unique values for this dimension
        try:
            dim_data = df[
                [id_col] + [col for col in value_cols if col in df.columns]
            ].drop_duplicates()
            logger.info(
                f"Preparing to insert/update {len(dim_data)} rows in {table_name}"
            )
        except KeyError as e:
            logger.error(f"Missing required columns in DataFrame for {table_name}: {e}")
            return 0, 0

        # Process in batches
        batch_size = 1000

        for i in range(0, len(dim_data), batch_size):
            batch = dim_data.iloc[i : i + batch_size]
            batch_errors = 0

            with engine.connect() as conn:
                # Start a transaction for this batch
                with conn.begin():
                    for idx, row in batch.iterrows():
                        try:
                            # Prepare the values dictionary with proper type handling
                            values = {
                                id_col: (
                                    str(row[id_col])
                                    if pd.notna(row.get(id_col))
                                    else None
                                )
                            }

                            # Add value columns
                            for col in value_cols:
                                if col in row:
                                    val = row[col]
                                    if pd.isna(val):
                                        values[col] = None
                                    elif isinstance(val, (int, float)) and pd.notna(
                                        val
                                    ):
                                        # Handle numeric types
                                        values[col] = (
                                            float(val) if "." in str(val) else int(val)
                                        )
                                    else:
                                        # Handle strings and other types
                                        values[col] = (
                                            str(val).strip()
                                            if val is not None
                                            else None
                                        )

                            # Build the upsert query
                            columns = [id_col] + [
                                col for col in value_cols if col in values
                            ]
                            placeholders = {
                                f"param_{i}": values[col]
                                for i, col in enumerate(columns, 1)
                            }

                            # Generate the SET clause for updates
                            set_clause = ", ".join(
                                [
                                    f"{col} = :param_{i+1}"
                                    for i, col in enumerate(columns[1:], 1)
                                ]
                            )

                            # Build the query with parameterized values
                            query = f"""
                                INSERT INTO {table_name} ({', '.join(columns)})
                                VALUES ({', '.join([f':param_{i+1}' for i in range(len(columns))])})
                                ON CONFLICT ({id_col}) 
                                DO UPDATE SET {set_clause}
                                RETURNING 1
                            """

                            # Execute with parameters to prevent SQL injection
                            result = conn.execute(text(query), placeholders)
                            total_inserted += 1

                            # Log progress
                            if total_inserted % 1000 == 0:
                                logger.info(
                                    f"  - Processed {total_inserted} rows in {table_name}"
                                )

                        except Exception as row_error:
                            batch_errors += 1
                            total_errors += 1
                            if batch_errors <= 3:  # Log first few errors
                                logger.warning(
                                    f"  Error in {table_name} row {idx}: {str(row_error)[:200]}"
                                )
                                if batch_errors == 3:
                                    logger.warning(
                                        "  Additional errors in this batch will be suppressed..."
                                    )
                            continue

                    # Commit the transaction for this batch
                    conn.commit()

            # Log batch completion
            logger.info(
                f"  - Completed batch {i//batch_size + 1}/{(len(dim_data)-1)//batch_size + 1} for {table_name}"
            )

        # Log final results
        if total_errors > 0:
            logger.warning(
                f"Completed {table_name} with {total_inserted} rows processed and {total_errors} errors"
            )
        else:
            logger.info(f"Successfully processed {total_inserted} rows in {table_name}")

        # Verify the final count
        with engine.connect() as conn:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            logger.info(f"  - Final row count in {table_name}: {count:,}")

        return total_inserted, total_errors

    except Exception as e:
        logger.error(f"Error in insert_dimension_data for {table_name}: {e}")
        if "conn" in locals():
            conn.rollback()
        raise

    except Exception as e:
        print(f"\nError in {table_name} insertion: {e}")
        traceback.print_exc()
        return 0


def insert_fact_data(engine, df, retailer_id):
    """
    Insert data into the fact_sales table with upsert functionality.

    Args:
        engine: SQLAlchemy engine
        df: DataFrame containing fact data to insert
        retailer_id: ID of the retailer

    Returns:
        Tuple of (total_inserted, total_errors)
    """
    total_inserted = 0
    total_errors = 0
    batch_size = 1000

    if df.empty:
        logger.warning("No fact data to insert")
        return 0, 0

    required_columns = [
        "retailer_id",
        "store_id",
        "sku_id",
        "date",
        "units_sold",
        "sales_value",
        "price",
        "promo_active",
    ]

    # Ensure all required columns exist
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(
            f"Missing required columns in fact data: {', '.join(missing_columns)}"
        )
        return 0, len(df)  # All rows are considered errors

    # Prepare data for insertion
    fact_data = df[required_columns].copy()

    # Convert date to string for SQL
    fact_data["date"] = pd.to_datetime(fact_data["date"]).dt.date

    # Get valid SKUs and stores from dimension tables
    with engine.connect() as conn:
        # Get all valid SKU IDs
        valid_skus = (
            pd.read_sql("SELECT DISTINCT sku_id FROM dim_sku", conn)["sku_id"]
            .astype(str)
            .tolist()
        )
        # Get all valid store IDs
        valid_stores = (
            pd.read_sql("SELECT DISTINCT store_id FROM dim_store", conn)["store_id"]
            .astype(str)
            .tolist()
        )

    # Filter out rows with invalid SKUs or stores
    original_count = len(fact_data)
    fact_data = fact_data[
        fact_data["sku_id"].astype(str).isin(valid_skus)
        & fact_data["store_id"].astype(str).isin(valid_stores)
    ]

    filtered_count = original_count - len(fact_data)
    if filtered_count > 0:
        logger.warning(
            f"Filtered out {filtered_count} rows with invalid SKUs or stores"
        )

    if fact_data.empty:
        logger.error("No valid fact data to insert after filtering")
        return 0, original_count  # All rows are considered errors

    # Process in batches
    for i in range(0, len(fact_data), batch_size):
        batch = fact_data.iloc[i : i + batch_size]
        batch_errors = 0

        with engine.connect() as conn:
            # Start a transaction for this batch
            with conn.begin():
                for idx, row in batch.iterrows():
                    try:
                        # Prepare values with proper type handling
                        values = {
                            "retailer_id": str(row["retailer_id"]),
                            "store_id": str(row["store_id"]),
                            "sku_id": str(row["sku_id"]),
                            "date": row["date"],
                            "units_sold": int(row["units_sold"]),
                            "sales_value": float(row["sales_value"]),
                            "price": float(row["price"]),
                            "promo_active": bool(row["promo_active"]),
                            "stock_level": int(row.get("stock_level", 0)),
                            "cost": float(
                                row.get("cost", row["price"] * 0.7)
                            ),  # Default cost if not provided
                        }

                        # Build the upsert query
                        columns = list(values.keys())
                        placeholders = {
                            f"param_{i}": val
                            for i, val in enumerate(values.values(), 1)
                        }

                        # Generate the SET clause for updates
                        set_clause = ", ".join(
                            [
                                f"{col} = EXCLUDED.{col}"
                                for col in columns
                                if col
                                not in ["retailer_id", "store_id", "sku_id", "date"]
                            ]
                        )

                        # Build the query with parameterized values
                        query = f"""
                            INSERT INTO fact_sales ({', '.join(columns)})
                            VALUES ({', '.join([f':param_{i+1}' for i in range(len(columns))])})
                            ON CONFLICT (retailer_id, store_id, sku_id, date) 
                            DO UPDATE SET {set_clause}
                            RETURNING 1
                        """

                        # Execute with parameters to prevent SQL injection
                        result = conn.execute(text(query), placeholders)
                        total_inserted += 1

                        # Log progress
                        if total_inserted % 1000 == 0:
                            logger.info(f"  - Processed {total_inserted} fact records")

                    except Exception as row_error:
                        batch_errors += 1
                        total_errors += 1
                        if batch_errors <= 3:  # Log first few errors
                            logger.warning(
                                f"  Error in fact row {idx}: {str(row_error)[:200]}"
                            )
                            if batch_errors == 3:
                                logger.warning(
                                    "  Additional errors in this batch will be suppressed..."
                                )
                        continue

                # Commit the transaction for this batch
                conn.commit()

        # Log batch completion
        logger.info(
            f"  - Completed batch {i//batch_size + 1}/{(len(fact_data)-1)//batch_size + 1} for fact_sales"
        )

    # Log final results
    if total_errors > 0:
        logger.warning(
            f"Completed fact_sales with {total_inserted} rows processed and {total_errors} errors"
        )
    else:
        logger.info(f"Successfully processed {total_inserted} rows in fact_sales")

    # Verify the final count
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM fact_sales")).scalar()
        logger.info(f"  - Final row count in fact_sales: {count:,}")

    return total_inserted, total_errors
    try:
        with engine.connect() as conn:
            # Prepare data for insertion - ensure we have all required columns
            required_cols = [
                "store_id",
                "sku_id",
                "date",
                "units_sold",
                "sales_value",
                "price",
            ]

            # Check if all required columns exist
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                print(
                    f"Error: Missing required columns in data: {', '.join(missing_cols)}"
                )
                return 0

            fact_data = df[required_cols].copy()

            # Ensure data types are correct
            fact_data["store_id"] = fact_data["store_id"].astype(str)
            fact_data["sku_id"] = fact_data["sku_id"].astype(str)
            fact_data["date"] = pd.to_datetime(
                fact_data["date"], errors="coerce"
            ).dt.strftime("%Y-%m-%d")

            # Convert numeric columns, handling any string formatting
            for col in ["units_sold", "sales_value", "price"]:
                if fact_data[col].dtype == "object":
                    fact_data[col] = (
                        fact_data[col].astype(str).str.replace(",", "").str.strip()
                    )
                fact_data[col] = pd.to_numeric(fact_data[col], errors="coerce")

            # Drop rows with missing values in required fields
            fact_data = fact_data.dropna(subset=["store_id", "sku_id", "date"])

            # Process in chunks of 1000
            chunk_size = 1000
            total_rows = len(fact_data)
            total_inserted = 0
            total_errors = 0
            error_messages = {}

            print(f"Starting to process {total_rows} fact rows...")

            for i in range(0, total_rows, chunk_size):
                chunk = fact_data.iloc[i : i + chunk_size]
                chunk_errors = 0
                chunk_inserted = 0

                # Process each row individually to isolate errors
                for _, row in chunk.iterrows():
                    try:
                        # Convert row to dict and handle None/NaN values
                        values = {}
                        for col in required_cols:
                            val = row[col]
                            if pd.isna(val):
                                values[col] = None
                            else:
                                values[col] = val

                        # Skip if we're missing required values
                        if not all(
                            values.get(col) for col in ["store_id", "sku_id", "date"]
                        ):
                            chunk_errors += 1
                            total_errors += 1
                            continue

                        # Prepare the INSERT statement with ON CONFLICT DO UPDATE
                        insert_stmt = text(
                            """
                            INSERT INTO fact_sales (
                                store_id, sku_id, date, 
                                units_sold, sales_value, price,
                                retailer_id
                            ) VALUES (
                                :store_id, :sku_id, :date, 
                                :units_sold, :sales_value, :price,
                                :retailer_id
                            )
                            ON CONFLICT (retailer_id, store_id, sku_id, date) 
                            DO UPDATE SET
                                units_sold = EXCLUDED.units_sold,
                                sales_value = EXCLUDED.sales_value,
                                price = EXCLUDED.price
                            RETURNING retailer_id, store_id, sku_id, date
                        """
                        )

                        # Execute with parameters to prevent SQL injection
                        result = conn.execute(insert_stmt, values)
                        total_inserted += 1

                    except Exception as e:
                        chunk_errors += 1
                        total_errors += 1
                        error_msg = str(e).split("\n")[0]  # Get first line of error
                        error_messages[error_msg] = error_messages.get(error_msg, 0) + 1

                        # Only show first 10 unique errors to avoid flooding logs
                        if len(error_messages) <= 10:
                            print(f"Error: {error_msg}")

                        # Skip this row and continue with the next one
                        continue

                # Commit after each chunk
                conn.commit()

                # Print progress
                print(
                    f"Processed chunk {i//chunk_size + 1}: {chunk_inserted}/{len(chunk)} rows inserted, {chunk_errors} errors"
                )

                # Print progress every 10 chunks
                if (i // chunk_size + 1) % 10 == 0:
                    print(
                        f"Progress: {min(i + chunk_size, total_rows)}/{total_rows} rows processed"
                    )

                # Suppress further error messages if we've seen too many
                if len(error_messages) > 10 and total_errors % 1000 == 0:
                    print(
                        f"Suppressing further error messages. Total errors so far: {total_errors}"
                    )

            # Print summary of errors
            if error_messages:
                print("\nError summary:")
                for msg, count in error_messages.items():
                    print(f"- {count}x: {msg}")

            print(
                f"\nSuccessfully inserted/updated {total_inserted} rows in fact_sales"
            )
            print(f"Total rows with errors: {total_errors}")

            return total_inserted

    except Exception as e:
        print(f"Error inserting fact data: {e}")


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="ETL Pipeline for Supermarket POS Data"
    )
    parser.add_argument("input_file", help="Path to the input CSV file")
    parser.add_argument(
        "--retailer-id",
        default=DEFAULT_RETAILER_ID,
        help=f"Retailer ID (default: {DEFAULT_RETAILER_ID})",
    )
    parser.add_argument(
        "--master-file",
        help="Path to master file for SKU enrichment (Excel)",
        default=None,
    )
    parser.add_argument(
        "--start-date", help="Start date for time dimension (YYYY-MM-DD)"
    )
    parser.add_argument("--end-date", help="End date for time dimension (YYYY-MM-DD)")
    return parser.parse_args()


def process_master_file(master_file_path, session):
    """Process the master Excel file for SKU enrichment."""
    if not master_file_path or not os.path.isfile(master_file_path):
        logger.warning(f"Master file not found or not provided: {master_file_path}")
        return

    try:
        logger.info(f"Processing master file: {master_file_path}")
        # Read the master file
        master_df = pd.read_excel(master_file_path)

        # Ensure required columns exist (adjust column names as per your master file)
        required_columns = ["CODE", "NAME", "CATEGORY", "DEPARTMENT"]
        if not all(col in master_df.columns for col in required_columns):
            logger.error(
                f"Master file missing required columns. Expected: {required_columns}"
            )
            return

        # Update SKU information in the database
        updated = 0
        for _, row in master_df.iterrows():
            sku = session.query(DimSku).filter_by(sku_id=str(row["CODE"])).first()
            if sku:
                sku.sku_name = row["NAME"]
                sku.category = row["CATEGORY"]
                sku.department = row["DEPARTMENT"]
                updated += 1

        session.commit()
        logger.info(f"Updated {updated} SKUs from master file")

    except Exception as e:
        logger.error(f"Error processing master file: {str(e)}")
        session.rollback()
        raise


def main():
    """Main function to run the ETL process."""
    # Track start time for performance metrics
    start_time = datetime.now()

    try:
        # Parse command line arguments
        args = parse_arguments()

        logger.info("=" * 80)
        logger.info(f"Starting ETL Pipeline at {start_time}")
        logger.info(f"Processing file: {args.input_file}")
        if args.master_file:
            logger.info(f"Using master file: {args.master_file}")

        # Validate input file exists
        if not os.path.isfile(args.input_file):
            logger.error(f"Input file not found: {args.input_file}")
            return 1

        # Create database connection with extended timeout settings
        logger.info("Establishing database connection...")
        engine = create_db_connection(
            host="localhost",
            port=5435,
            database="etl_db",
            user="postgres",
            password="postgres",
            # These parameters need to be passed in the query string
            connect_args={
                "connect_timeout": 60,  # Increased timeout for connection
                "keepalives": 1,  # Enable keepalive
                "keepalives_idle": 60,  # Idle time before sending keepalive
                "keepalives_interval": 10,  # Interval between keepalives
                "keepalives_count": 5,  # Number of keepalives before dropping connection
                "options": "-c statement_timeout=3600000",  # 1 hour statement timeout
            },
            pool_pre_ping=True,  # Enable connection health checks
            pool_recycle=3600,  # Recycle connections after 1 hour
        )

        if not engine:
            logger.error("Failed to connect to the database. Exiting.")
            return 1

        # Set database parameters for this session
        with engine.connect() as conn:
            conn.execute(text("SET statement_timeout = 3600000"))  # 1 hour timeout
            conn.execute(text("SET lock_timeout = 300000"))  # 5 minute lock timeout
            conn.execute(
                text("SET idle_in_transaction_session_timeout = 60000")
            )  # 1 minute
            conn.commit()

        # Ensure the retailer exists
        logger.info("\n=== Step 1: Verifying retailer ===")
        ensure_retailer_exists(engine, args.retailer_id)

        # Update database schema if needed
        logger.info("\n=== Step 2: Checking and updating database schema ===")
        update_database_schema(engine)

        # Run the ETL pipeline
        logger.info("\n=== Step 3: Running ETL pipeline ===")

        # Step 1: Ingest and clean data
        logger.info("\n=== Step 3.1: Ingesting, cleaning, and transforming data ===")
        df = ingest_and_clean_data(args.input_file)
        if df is None or df.empty:
            logger.error("No data to process after cleaning. Exiting.")
            return 1

        # Step 2: Ensure dimension tables are populated
        logger.info("\n=== Step 3.2: Populating dimension tables ===")
        ensure_dimension_tables(engine, df)

        # Step 3: Insert data into the fact table
        logger.info("\n=== Step 3.3: Loading data into fact table ===")
        total_inserted, total_errors = insert_fact_data(engine, df, args.retailer_id)

        # Log basic statistics about the transformed data
        logger.info("\n=== Transformed Data Summary ===")
        logger.info(f"Total records for processing: {len(df):,}")
        logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
        logger.info(f"Number of unique stores: {df['store_id'].nunique():,}")
        logger.info(f"Number of unique SKUs: {df['sku_id'].nunique():,}")
        logger.info(f"Total sales value: ${df['sales_value'].sum():,.2f}")
        logger.info(f"Total units sold: {df['units_sold'].sum():,}")
        logger.info("================================\n")

        # Step 7: Populate time dimension based on fact data dates if not already done
        if not df.empty and not (args.start_date and args.end_date):
            min_date = df["date"].min()
            max_date = df["date"].max()
            logger.info(
                f"\n=== Step 7: Ensuring time dimension is populated from {min_date} to {max_date} ==="
            )
            populate_time_dimension(engine, min_date, max_date)

        # Log final summary
        logger.info("\n=== ETL Process Completed Successfully ===")
        logger.info(f"Total fact records processed: {len(df):,}")
        logger.info(f"Successfully inserted/updated: {total_inserted:,}")
        logger.info(f"Total errors: {total_errors}")

        # Calculate and log performance metrics
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        records_per_second = total_inserted / duration if duration > 0 else 0

        logger.info(f"\nPerformance Metrics:")
        logger.info(f"Start time: {start_time}")
        logger.info(f"End time: {end_time}")
        logger.info(f"Total duration: {duration:.2f} seconds")
        logger.info(f"Records processed per second: {records_per_second:.2f}")

        if total_errors > 0:
            logger.warning(
                f"Completed with {total_errors} errors. Check the logs for details."
            )
            return 1

        return 0

    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}", exc_info=True)
        return 1
    finally:
        # Ensure all database connections are closed
        if "engine" in locals():
            engine.dispose()
        logger.info("=" * 80)


if __name__ == "__main__":

    try:
        sys.exit(main())
    except KeyboardInterrupt:
        logger.warning("ETL Pipeline was interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)
