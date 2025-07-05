#!/usr/bin/env python3
"""
create_bi_views.py - Create optimized views for BI tools
"""

import pandas as pd
from sqlalchemy import create_engine, text
import logging
from typing import Dict, Optional
from datetime import datetime, timedelta
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            f'logs/bi_views_creation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        ),
    ],
)
logger = logging.getLogger(__name__)


class BIViewsCreator:
    """Class to create and manage BI views in the database"""

    def __init__(
        self, db_url: str = "postgresql://postgres:mini@localhost:5435/etl_db"
    ):
        """Initialize with database connection"""
        self.db_url = db_url
        self.engine = create_engine(self.db_url)

    def create_views(self):
        """Create all BI views"""
        try:
            logger.info("Starting creation of BI views...")

            # Drop existing views if they exist
            self._drop_existing_views()

            # Create each view
            self._create_sales_summary_view()
            self._create_sku_store_matrix_view()
            self._create_pricing_trends_view()
            self._create_promo_analysis_view()

            logger.info("Successfully created all BI views")
            return True

        except Exception as e:
            logger.error(f"Error creating BI views: {str(e)}", exc_info=True)
            return False

    def _drop_existing_views(self):
        """Drop existing views if they exist"""
        views = [
            "sales_summary_view",
            "sku_store_matrix_view",
            "pricing_trends_view",
            "promo_analysis_view",
        ]

        with self.engine.connect() as conn:
            for view in views:
                conn.execute(text(f"DROP VIEW IF EXISTS {view} CASCADE"))
            conn.commit()
        logger.info("Dropped existing views")

    def _create_sales_summary_view(self):
        """Create sales summary view with fallback to store_id when store_name is empty"""
        query = """
        CREATE OR REPLACE VIEW sales_summary_view AS
SELECT 
    fs.date,
    fs.store_id,
    COALESCE(NULLIF(ds.store_name, ''), ds.store_id)::varchar(255) as store_name,
    ds.retailer_id,
    dr.retailer_name,
    fs.sku_id,
    dk.sku_name,
    dk.category,
    dk.department,
    fs.units_sold,
    fs.sales_value,
    fs.price,
    fs.stock_level,
    fs.promo_active,
    fs.cost,
    (fs.sales_value / NULLIF(fs.units_sold, 0)::numeric)::numeric(10,2) as avg_selling_price,
    (fs.price * fs.units_sold)::numeric(10,2) as gross_sales,
    ((fs.price - fs.cost) * fs.units_sold)::numeric(10,2) as gross_profit,
    CASE 
        WHEN fs.sales_value = 0::numeric THEN 0::numeric
        ELSE ((fs.price - fs.cost) / NULLIF(fs.price, 0) * 100)::numeric(10,2)
    END as gross_margin_pct,
    dt.day_name,
    dt.month_name,
    dt.quarter,
    dt.year,
    dt.is_weekend
FROM fact_sales fs
JOIN dim_store ds ON fs.store_id = ds.store_id
JOIN dim_retailer dr ON fs.retailer_id = dr.retailer_id
JOIN dim_sku dk ON fs.sku_id = dk.sku_id
JOIN dim_time dt ON fs.date = dt.date;
        """
        with self.engine.connect() as conn:
            conn.execute(text(query))
            conn.commit()
        logger.info("Created sales_summary_view with store_name fallback to store_id")

    def _create_sku_store_matrix_view(self):
        """Create SKU-store matrix view"""
        query = """
        CREATE OR REPLACE VIEW sku_store_matrix_view AS
        SELECT 
            fs.sku_id,
            dk.sku_name,
            dk.category,
            dk.department,
            fs.store_id,
            ds.store_name,
            ds.location,
            COUNT(DISTINCT fs.date) as days_available,
            SUM(fs.units_sold) as total_units_sold,
            SUM(fs.sales_value) as total_sales_value,
            AVG(fs.stock_level) as avg_stock_level,
            SUM(CASE WHEN fs.stock_level = 0 THEN 1 ELSE 0 END) as days_out_of_stock,
            ROUND(COUNT(DISTINCT fs.date) * 100.0 / 
                NULLIF((SELECT COUNT(DISTINCT date) FROM fact_sales), 0), 2) as numeric_distribution_pct,
            SUM(fs.units_sold * fs.price) as gross_sales,
            SUM((fs.price - fs.cost) * fs.units_sold) as gross_profit,
            CASE 
                WHEN SUM(fs.units_sold * fs.price) = 0 THEN 0 
                ELSE ROUND(SUM((fs.price - fs.cost) * fs.units_sold) * 100.0 / 
                    NULLIF(SUM(fs.units_sold * fs.price), 0), 2) 
            END as gross_margin_pct
        FROM 
            fact_sales fs
            JOIN dim_sku dk ON fs.sku_id = dk.sku_id
            JOIN dim_store ds ON fs.store_id = ds.store_id
        GROUP BY 
            fs.sku_id, dk.sku_name, dk.category, dk.department, 
            fs.store_id, ds.store_name, ds.location
        """
        with self.engine.connect() as conn:
            conn.execute(text(query))
            conn.commit()
        logger.info("Created sku_store_matrix_view")

    def _create_pricing_trends_view(self):
        """Create pricing trends view"""
        query = """
        CREATE OR REPLACE VIEW pricing_trends_view AS
        WITH weekly_metrics AS (
            SELECT 
                sku_id,
                store_id,
                DATE_TRUNC('week', date) as week_start_date,
                EXTRACT(YEAR FROM date) as year,
                EXTRACT(WEEK FROM date) as week_number,
                AVG(price) as avg_weekly_price,
                AVG(cost) as avg_weekly_cost,
                SUM(CASE WHEN promo_active THEN 1 ELSE 0 END) as promo_days,
                COUNT(DISTINCT date) as total_days,
                SUM(units_sold) as units_sold,
                SUM(sales_value) as total_sales_value,
                SUM((price - cost) * units_sold) as gross_profit
            FROM fact_sales
            GROUP BY sku_id, store_id, DATE_TRUNC('week', date), EXTRACT(YEAR FROM date), EXTRACT(WEEK FROM date)
        )
        SELECT 
            wm.sku_id,
            dk.sku_name,
            dk.category,
            dk.department,
            wm.store_id,
            ds.store_name,
            ds.location,
            wm.year,
            wm.week_number,
            wm.week_start_date,
            wm.avg_weekly_price,
            wm.avg_weekly_cost,
            wm.promo_days,
            wm.total_days,
            ROUND(wm.promo_days * 100.0 / NULLIF(wm.total_days, 0), 2) as promo_discount_pct,
            wm.units_sold,
            wm.total_sales_value,
            wm.gross_profit,
            CASE 
                WHEN wm.total_sales_value = 0 THEN 0 
                ELSE ROUND(wm.gross_profit * 100.0 / NULLIF(wm.total_sales_value, 0), 2) 
            END as gross_margin_pct,
            LAG(wm.avg_weekly_price) OVER (PARTITION BY wm.sku_id, wm.store_id ORDER BY wm.week_start_date) AS prev_week_price,
            (wm.avg_weekly_price - LAG(wm.avg_weekly_price) OVER (PARTITION BY wm.sku_id, wm.store_id ORDER BY wm.week_start_date)) AS price_change,
            ROUND((wm.avg_weekly_price - LAG(wm.avg_weekly_price) OVER (PARTITION BY wm.sku_id, wm.store_id ORDER BY wm.week_start_date)) * 100.0 / 
                 NULLIF(LAG(wm.avg_weekly_price) OVER (PARTITION BY wm.sku_id, wm.store_id ORDER BY wm.week_start_date), 0), 2) as price_change_pct
        FROM 
            weekly_metrics wm
            JOIN dim_sku dk ON wm.sku_id = dk.sku_id
            JOIN dim_store ds ON wm.store_id = ds.store_id
        """
        with self.engine.connect() as conn:
            conn.execute(text(query))
            conn.commit()
        logger.info("Created pricing_trends_view")

    def update_sku_dimension(self, master_file_path):
        """Update dim_sku table with master file data

        Args:
            master_file_path (str): Path to the master Excel file
        """
        try:
            import pandas as pd
            from sqlalchemy import text

            logger.info(f"Reading master file from {master_file_path}")
            df = pd.read_excel(master_file_path)

            # Clean and map columns
            column_mapping = {
                "CODE": "sku_id",
                "NAME": "sku_name",
                "CATEGORY": "category",
                "DEPARTMENT": "department",
                "SUB DEPARTMENT": "sub_department",
                "MICRO DEPARTMENT": "micro_department",
                "Brand": "brand",
                "VAT": "vat_rate",
                "SUPPLIER": "supplier",
            }

            # Rename and select only the columns we need
            df = df.rename(columns=column_mapping)
            df = df[column_mapping.values()]

            # Clean up any whitespace in string columns
            for col in df.select_dtypes(include=["object"]).columns:
                df[col] = df[col].str.strip()

            logger.info(f"Updating dim_sku with {len(df)} SKUs")

            # Create or replace the dim_sku table
            with self.engine.connect() as conn:
                # Create the table if it doesn't exist
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS dim_sku (
                    sku_id VARCHAR(50) PRIMARY KEY,
                    sku_name TEXT,
                    category TEXT,
                    department TEXT,
                    sub_department TEXT,
                    micro_department TEXT,
                    brand TEXT,
                    vat_rate VARCHAR(10),
                    supplier TEXT
                )
                """
                conn.execute(text(create_table_sql))
                conn.commit()

                # Clear existing data
                conn.execute(text("TRUNCATE TABLE dim_sku CASCADE"))

                # Insert new data
                df.to_sql("dim_sku", con=conn, if_exists="append", index=False)
                conn.commit()

                # Verify the update
                result = conn.execute(text("SELECT COUNT(*) FROM dim_sku")).scalar()
                logger.info(f"Successfully updated dim_sku with {result} records")

        except Exception as e:
            logger.error(f"Error updating dim_sku: {str(e)}")
            raise

    def _create_promo_analysis_view(self):
        """Create promotion analysis view with simplified date handling"""
        query = """
        CREATE OR REPLACE VIEW promo_analysis_view AS
        WITH promo_periods AS (
            SELECT 
                sku_id,
                store_id,
                date,
                promo_active,
                price as promo_price,
                cost,
                units_sold,
                sales_value,
                LEAD(date) OVER (PARTITION BY sku_id, store_id ORDER BY date) AS next_date
            FROM fact_sales
            WHERE promo_active = true
        ),
        promo_metrics AS (
            SELECT 
                pp.sku_id,
                pp.store_id,
                pp.date AS promo_start_date,
                COALESCE(pp.next_date - INTERVAL '1 day', 
                        (SELECT MAX(date) FROM fact_sales)) AS promo_end_date,
                pp.promo_price,
                -- Pre-promo period (7 days before promo)
                (SELECT COALESCE(SUM(fs.units_sold), 0) / 7.0 
                 FROM fact_sales fs 
                 WHERE fs.sku_id = pp.sku_id 
                 AND fs.store_id = pp.store_id 
                 AND fs.date BETWEEN pp.date - INTERVAL '7 days' AND pp.date - INTERVAL '1 day'
                 AND fs.promo_active = false) AS avg_daily_units_pre_promo,
                -- Promo period (use 7 days as default duration to simplify)
                (SELECT COALESCE(SUM(fs2.units_sold), 0) / 7.0
                 FROM fact_sales fs2 
                 WHERE fs2.sku_id = pp.sku_id 
                 AND fs2.store_id = pp.store_id 
                 AND fs2.date BETWEEN pp.date AND (pp.date + INTERVAL '6 days')
                 AND fs2.promo_active = true) AS avg_daily_units_during_promo,
                -- Post-promo period (7 days after promo)
                (SELECT COALESCE(SUM(fs3.units_sold), 0) / 7.0 
                 FROM fact_sales fs3 
                 WHERE fs3.sku_id = pp.sku_id 
                 AND fs3.store_id = pp.store_id 
                 AND fs3.date BETWEEN (pp.date + INTERVAL '7 days') AND (pp.date + INTERVAL '13 days')
                 AND fs3.promo_active = false) AS avg_daily_units_post_promo,
                -- Pre-promo revenue
                (SELECT COALESCE(SUM(fs4.sales_value), 0) / 7.0 
                 FROM fact_sales fs4
                 WHERE fs4.sku_id = pp.sku_id 
                 AND fs4.store_id = pp.store_id 
                 AND fs4.date BETWEEN (pp.date - INTERVAL '7 days') AND (pp.date - INTERVAL '1 day')
                 AND fs4.promo_active = false) AS avg_daily_revenue_pre_promo,
                -- Promo revenue (use 7 days as default duration to simplify)
                (SELECT COALESCE(SUM(fs5.sales_value), 0) / 7.0
                 FROM fact_sales fs5
                 WHERE fs5.sku_id = pp.sku_id 
                 AND fs5.store_id = pp.store_id 
                 AND fs5.date BETWEEN pp.date AND (pp.date + INTERVAL '6 days')
                 AND fs5.promo_active = true) AS avg_daily_revenue_during_promo
            FROM promo_periods pp
            WHERE NOT EXISTS (
                SELECT 1 
                FROM promo_periods pp2 
                WHERE pp2.sku_id = pp.sku_id 
                AND pp2.store_id = pp.store_id 
                AND pp2.date = pp.date - INTERVAL '1 day'
                AND pp2.promo_active = true
            )
        )
        SELECT 
            pm.sku_id,
            dk.sku_name,
            dk.category,
            dk.department,
            pm.store_id,
            ds.store_name,
            ds.location,
            pm.promo_start_date,
            pm.promo_end_date,
            7 AS promo_duration_days,  -- Fixed 7-day promotion period
            pm.promo_price,
            pm.avg_daily_units_pre_promo,
            pm.avg_daily_units_during_promo,
            pm.avg_daily_units_post_promo,
            pm.avg_daily_revenue_pre_promo,
            pm.avg_daily_revenue_during_promo,
            CASE 
                WHEN pm.avg_daily_units_pre_promo = 0 THEN 0 
                ELSE ROUND((pm.avg_daily_units_during_promo - pm.avg_daily_units_pre_promo) * 100.0 / 
                      NULLIF(pm.avg_daily_units_pre_promo, 0), 2)
            END AS units_uplift_pct,
            CASE 
                WHEN pm.avg_daily_revenue_pre_promo = 0 THEN 0 
                ELSE ROUND((pm.avg_daily_revenue_during_promo - pm.avg_daily_revenue_pre_promo) * 100.0 / 
                      NULLIF(pm.avg_daily_revenue_pre_promo, 0), 2)
            END AS revenue_uplift_pct
        FROM 
            promo_metrics pm
            JOIN dim_sku dk ON pm.sku_id = dk.sku_id
            JOIN dim_store ds ON pm.store_id = ds.store_id
        WHERE 
            pm.avg_daily_units_during_promo > 0  -- Only include promotions with actual sales
        ORDER BY 
            pm.promo_start_date DESC, 
            units_uplift_pct DESC
        """
        with self.engine.connect() as conn:
            conn.execute(text(query))
            conn.commit()
            logger.info("Created promo_analysis_view")


def main():
    """Main function to update dim_sku and create all BI views"""
    try:
        # Initialize with your database URL
        creator = BIViewsCreator()

        # Create all BI views
        creator.create_views()

        # List all available views
        with creator.engine.connect() as conn:
            # Get list of views
            result = conn.execute(
                text(
                    """
                SELECT table_name 
                FROM information_schema.views 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """
                )
            )
            views = [row[0] for row in result]
            logger.info(f"\nAvailable views: {', '.join(views)}")

            # Print column info for each view
            for view in views:
                result = conn.execute(
                    text(
                        """
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = :view_name
                    ORDER BY ordinal_position
                """
                    ),
                    {"view_name": view},
                )
                logger.info(f"\nView: {view}")
                for row in result:
                    logger.info(f"  {row[0]} ({row[1]})")

        logger.info("\nBI views created successfully!")
        return 0

    except Exception as e:
        logger.error(f"Error in main: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
