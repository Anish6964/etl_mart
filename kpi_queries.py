#!/usr/bin/env python3
"""
kpi_queries.py - Calculate advanced KPIs for supermarket POS data
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import sys
from dateutil.relativedelta import relativedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/kpi_calculations.log"),
    ],
)
logger = logging.getLogger(__name__)


class KPICalculator:
    def __init__(self, db_url: str):
        self.engine = create_engine(db_url)
        self.today = datetime.now().date()
        self.last_month = (self.today.replace(day=1) - timedelta(days=1)).replace(day=1)
        self.two_months_ago = (
            self.last_month.replace(day=1) - timedelta(days=1)
        ).replace(day=1)

    def calculate_all_kpis(self) -> Dict[str, pd.DataFrame]:
        """Calculate all KPIs and return as a dictionary of DataFrames"""
        results = {}

        try:
            logger.info("Starting KPI calculations...")

            # Basic metrics
            logger.info("Calculating sales summary...")
            results["sales_summary"] = self.calculate_sales_summary()

            logger.info("Calculating sales growth month-over-month...")
            results["sales_growth_mom"] = self.calculate_sales_growth_mom()

            logger.info("Calculating ASP analysis...")
            results["asp_analysis"] = self.calculate_asp_analysis()

            logger.info("Calculating numeric distribution...")
            results["numeric_distribution"] = self.calculate_numeric_distribution()

            logger.info("Calculating promotional uplift...")
            results["promo_uplift"] = self.calculate_promo_uplift()

            logger.info("Identifying SKU velocity...")
            results["sku_velocity"] = self.identify_sku_velocity()

            logger.info("Identifying problem stores...")
            results["problem_stores"] = self.identify_problem_stores()

            # Flag issues
            logger.info("Flagging inventory and promotion issues...")
            try:
                issue_flags = self.flag_issues()
                results.update(
                    {
                        "stockouts": issue_flags["stockouts"],
                        "dead_stock": issue_flags["dead_stock"],
                        "promo_gaps": issue_flags["promo_gaps"],
                        "issue_summary": issue_flags["issue_summary"],
                    }
                )
                logger.info(
                    f"Found {len(issue_flags['stockouts'])} stockouts, {len(issue_flags['dead_stock'])} dead stock items, and {len(issue_flags['promo_gaps'])} promo gaps"
                )
            except Exception as e:
                logger.error(f"Error in issue flagging: {str(e)}", exc_info=True)

            # Save results
            logger.info("Saving results...")
            self.save_results(results)

            logger.info("KPI calculations completed successfully")
            return results

        except Exception as e:
            logger.error(f"Error in KPI calculations: {str(e)}", exc_info=True)
            raise

    def calculate_sales_summary(self) -> pd.DataFrame:
        """Calculate basic sales summary metrics"""
        query = f"""
            SELECT 
                date,
                COUNT(DISTINCT store_id) AS store_count,
                COUNT(DISTINCT sku_id) AS sku_count,
                SUM(units_sold) AS total_units_sold,
                SUM(sales_value) AS total_sales_value,
                SUM(CASE WHEN promo_active THEN units_sold ELSE 0 END) AS promo_units_sold,
                SUM(CASE WHEN promo_active THEN sales_value ELSE 0 END) AS promo_sales_value
            FROM fact_sales
            WHERE date >= '{self.two_months_ago}'
            GROUP BY date
            ORDER BY date DESC
        """
        return pd.read_sql(query, self.engine)

    def calculate_sales_growth_mom(self) -> pd.DataFrame:
        """Calculate month-over-month sales growth"""
        query = f"""
            WITH monthly_sales AS (
                SELECT 
                    DATE_TRUNC('month', date) AS month,
                    SUM(sales_value) AS total_sales,
                    LAG(SUM(sales_value), 1) OVER (ORDER BY DATE_TRUNC('month', date)) AS prev_month_sales
                FROM fact_sales
                WHERE date >= '{self.two_months_ago}'
                GROUP BY DATE_TRUNC('month', date)
            )
            SELECT 
                month,
                total_sales,
                prev_month_sales,
                ROUND(((total_sales - prev_month_sales) / NULLIF(prev_month_sales::numeric, 0) * 100)::numeric, 2) AS mom_growth_pct
            FROM monthly_sales
            WHERE prev_month_sales IS NOT NULL
            ORDER BY month DESC
        """
        return pd.read_sql(query, self.engine)

    def calculate_asp_analysis(self) -> pd.DataFrame:
        """Calculate average selling price analysis"""
        query = f"""
            WITH monthly_asp AS (
                SELECT 
                    sku_id,
                    DATE_TRUNC('month', date) AS month,
                    ROUND((SUM(sales_value) / NULLIF(SUM(units_sold)::numeric, 0))::numeric, 2) AS asp
                FROM fact_sales
                WHERE date >= '{self.two_months_ago}'
                GROUP BY sku_id, DATE_TRUNC('month', date)
            )
            SELECT 
                month,
                AVG(asp::numeric) AS avg_asp,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY asp) AS median_asp,
                MIN(asp) AS min_asp,
                MAX(asp) AS max_asp,
                COUNT(DISTINCT sku_id) AS sku_count
            FROM monthly_asp
            GROUP BY month
            ORDER BY month DESC
        """
        return pd.read_sql(query, self.engine)

    def calculate_numeric_distribution(self) -> pd.DataFrame:
        """Calculate numeric distribution metrics"""
        query = f"""
            WITH store_sku_month AS (
                SELECT 
                    DATE_TRUNC('month', date) AS month,
                    store_id,
                    COUNT(DISTINCT sku_id) AS sku_count
                FROM fact_sales
                WHERE date >= '{self.two_months_ago}'
                GROUP BY DATE_TRUNC('month', date), store_id
            ),
            total_skus AS (
                SELECT 
                    DATE_TRUNC('month', date) AS month,
                    COUNT(DISTINCT sku_id) AS total_skus
                FROM fact_sales
                WHERE date >= '{self.two_months_ago}'
                GROUP BY DATE_TRUNC('month', date)
            )
            SELECT 
                sm.month,
                AVG(sm.sku_count::float / ts.total_skus) * 100 AS avg_numeric_distribution_pct,
                MIN(sm.sku_count::float / ts.total_skus) * 100 AS min_numeric_distribution_pct,
                MAX(sm.sku_count::float / ts.total_skus) * 100 AS max_numeric_distribution_pct
            FROM store_sku_month sm
            JOIN total_skus ts ON sm.month = ts.month
            GROUP BY sm.month
            ORDER BY sm.month DESC
        """
        return pd.read_sql(query, self.engine)

    def calculate_promo_uplift(self) -> pd.DataFrame:
        """Calculate promotional uplift metrics"""
        query = f"""
            WITH promo_weeks AS (
                SELECT 
                    sku_id,
                    DATE_TRUNC('week', date) AS week,
                    SUM(CASE WHEN promo_active THEN units_sold ELSE 0 END) AS promo_units,
                    SUM(CASE WHEN NOT promo_active THEN units_sold ELSE 0 END) AS non_promo_units,
                    SUM(CASE WHEN promo_active THEN sales_value ELSE 0 END) AS promo_sales,
                    SUM(CASE WHEN NOT promo_active THEN sales_value ELSE 0 END) AS non_promo_sales
                FROM fact_sales
                WHERE date >= '{self.two_months_ago}'
                GROUP BY sku_id, DATE_TRUNC('week', date)
                HAVING 
                    SUM(CASE WHEN promo_active THEN 1 ELSE 0 END) > 0 AND
                    SUM(CASE WHEN NOT promo_active THEN 1 ELSE 0 END) > 0
            )
            SELECT 
                sku_id,
                COUNT(*) AS promo_weeks,
                SUM(promo_units) AS total_promo_units,
                SUM(non_promo_units) AS total_non_promo_units,
                SUM(promo_sales) AS total_promo_sales,
                SUM(non_promo_sales) AS total_non_promo_sales,
                ROUND((SUM(promo_units)::numeric / NULLIF(SUM(non_promo_units)::numeric, 0) - 1) * 100, 2) AS uplift_pct
            FROM promo_weeks
            GROUP BY sku_id
            HAVING SUM(non_promo_units) > 0
            ORDER BY uplift_pct DESC
            LIMIT 50
        """
        return pd.read_sql(query, self.engine)

    def identify_sku_velocity(self) -> pd.DataFrame:
        """Identify fastest and slowest moving SKUs"""
        query = f"""
            WITH sku_sales AS (
                SELECT 
                    sku_id,
                    SUM(units_sold) AS total_units,
                    COUNT(DISTINCT date) AS days_with_sales,
                    SUM(units_sold) / NULLIF(COUNT(DISTINCT date), 0) AS avg_daily_units
                FROM fact_sales
                WHERE date >= '{self.last_month}'
                GROUP BY sku_id
                HAVING COUNT(DISTINCT date) >= 5
            )
            SELECT 
                sku_id,
                total_units,
                days_with_sales,
                ROUND(avg_daily_units::numeric, 2) AS avg_daily_units,
                NTILE(4) OVER (ORDER BY avg_daily_units) AS velocity_quartile
            FROM sku_sales
            ORDER BY avg_daily_units DESC
        """
        return pd.read_sql(query, self.engine)

    def identify_problem_stores(self) -> pd.DataFrame:
        """Identify underperforming stores"""
        query = f"""
            WITH store_metrics AS (
                SELECT 
                    store_id,
                    COUNT(DISTINCT sku_id) AS sku_count,
                    SUM(units_sold) AS total_units,
                    SUM(sales_value) AS total_sales,
                    COUNT(DISTINCT date) AS days_open,
                    SUM(CASE WHEN stock_level = 0 THEN 1 ELSE 0 END) AS out_of_stock_days
                FROM fact_sales
                WHERE date >= '{self.last_month}'
                GROUP BY store_id
            ),
            avg_metrics AS (
                SELECT
                    AVG(sku_count) AS avg_sku_count,
                    AVG(total_units) AS avg_units,
                    AVG(total_sales) AS avg_sales
                FROM store_metrics
            )
            SELECT 
                s.store_id,
                st.store_name,
                s.sku_count,
                s.total_units,
                s.total_sales,
                s.days_open,
                s.out_of_stock_days,
                ROUND((s.out_of_stock_days::numeric / NULLIF(s.days_open, 0) * 100)::numeric, 2) AS oos_pct,
                CASE 
                    WHEN s.sku_count < (am.avg_sku_count * 0.7) THEN 'Low SKU Count'
                    WHEN s.total_units < (am.avg_units * 0.5) THEN 'Low Volume'
                    WHEN (s.out_of_stock_days::float / s.days_open) > 0.2 THEN 'High OOS Rate'
                    ELSE 'Performing as Expected'
                END AS performance_issue
            FROM store_metrics s
            CROSS JOIN avg_metrics am
            JOIN dim_store st ON s.store_id = st.store_id
            WHERE 
                s.sku_count < (am.avg_sku_count * 0.7) OR
                s.total_units < (am.avg_units * 0.5) OR
                (s.out_of_stock_days::float / s.days_open) > 0.2
            ORDER BY 
                CASE 
                    WHEN s.sku_count < (am.avg_sku_count * 0.7) THEN 1
                    WHEN s.total_units < (am.avg_units * 0.5) THEN 2
                    ELSE 3
                END,
                s.total_units
        """
        return pd.read_sql(query, self.engine)

    def flag_issues(self) -> Dict[str, pd.DataFrame]:
        """Flag inventory and promotion issues

        Returns:
            Dict containing DataFrames for each type of issue:
            - stockouts: Items with stock_level = 0
            - dead_stock: Items with stock > 0 but no sales
            - promo_gaps: Promotions with low uplift
        """
        from datetime import datetime, timedelta

        def safe_read_sql(query, params=None):
            """Helper function to safely execute SQL queries"""
            try:
                if params:
                    return pd.read_sql(text(query), self.engine, params=params)
                return pd.read_sql(text(query), self.engine)
            except Exception as e:
                logger.error(f"Error executing query: {str(e)}")
                return pd.DataFrame()

        try:
            issues = {}

            # 1. Get Stockouts
            stockout_query = """
                SELECT 
                    fs.store_id,
                    ds.store_name,
                    fs.sku_id,
                    dk.sku_name,
                    fs.date,
                    fs.stock_level,
                    fs.units_sold,
                    'Stockout' as issue_type
                FROM fact_sales fs
                JOIN dim_sku dk ON fs.sku_id = dk.sku_id
                JOIN dim_store ds ON fs.store_id = ds.store_id
                WHERE fs.stock_level = 0
                AND fs.date = (SELECT MAX(date) FROM fact_sales)
                ORDER BY fs.store_id, fs.sku_id
            """
            issues["stockouts"] = safe_read_sql(stockout_query)

            # 2. Get Dead Stock (no sales in last 30 days but have stock)
            dead_stock_query = """
                WITH last_30_days AS (
                    SELECT 
                        store_id, 
                        sku_id,
                        SUM(units_sold) as total_units_sold,
                        MAX(stock_level) as current_stock
                    FROM fact_sales
                    WHERE date >= CURRENT_DATE - INTERVAL '30 days'
                    GROUP BY store_id, sku_id
                    HAVING SUM(units_sold) = 0
                )
                SELECT 
                    fs.store_id,
                    ds.store_name,
                    fs.sku_id,
                    dk.sku_name,
                    fs.current_stock as stock_level,
                    fs.total_units_sold as units_sold_last_30_days,
                    'Dead Stock' as issue_type
                FROM last_30_days fs
                JOIN dim_sku dk ON fs.sku_id = dk.sku_id
                JOIN dim_store ds ON fs.store_id = ds.store_id
                WHERE fs.current_stock > 0
                ORDER BY fs.store_id, fs.sku_id
            """
            issues["dead_stock"] = safe_read_sql(dead_stock_query)

            # 3. Get Promo Gaps (promotions with low uplift)
            promo_gap_query = """
                WITH promo_data AS (
                    SELECT 
                        sku_id,
                        store_id,
                        promo_active,
                        SUM(units_sold) as units_sold
                    FROM fact_sales
                    WHERE date >= CURRENT_DATE - INTERVAL '90 days'
                    GROUP BY sku_id, store_id, promo_active
                )
                SELECT 
                    p1.store_id,
                    ds.store_name,
                    p1.sku_id,
                    dk.sku_name,
                    COALESCE(p1.units_sold, 0) as promo_units,
                    COALESCE(p2.units_sold, 0) as non_promo_units,
                    CASE 
                        WHEN COALESCE(p2.units_sold, 0) = 0 THEN 0 
                        ELSE ROUND((COALESCE(p1.units_sold, 0)::numeric / NULLIF(COALESCE(p2.units_sold, 1), 0) - 1) * 100, 2)::numeric
                    END as uplift_pct,
                    'Low Promo Uplift' as issue_type
                FROM promo_data p1
                LEFT JOIN promo_data p2 
                    ON p1.sku_id = p2.sku_id 
                    AND p1.store_id = p2.store_id 
                    AND p1.promo_active = true 
                    AND p2.promo_active = false
                JOIN dim_sku dk ON p1.sku_id = dk.sku_id
                JOIN dim_store ds ON p1.store_id = ds.store_id
                WHERE p1.promo_active = true
                AND COALESCE(p2.units_sold, 0) > 0
                AND ((COALESCE(p1.units_sold, 0)::numeric / NULLIF(COALESCE(p2.units_sold, 1), 0) - 1) * 100) < 20
                ORDER BY uplift_pct ASC
            """
            issues["promo_gaps"] = safe_read_sql(promo_gap_query)

            # Add summary counts
            summary = {
                "issue_type": ["Stockouts", "Dead Stock", "Promo Gaps"],
                "count": [
                    len(issues["stockouts"]),
                    len(issues["dead_stock"]),
                    len(issues["promo_gaps"]),
                ],
            }
            issues["issue_summary"] = pd.DataFrame(summary)

            return issues

        except Exception as e:
            logger.error(f"Error in flag_issues: {str(e)}", exc_info=True)
            # Return empty DataFrames with proper structure
            empty_df = pd.DataFrame()
            return {
                "stockouts": empty_df,
                "dead_stock": empty_df,
                "promo_gaps": empty_df,
                "issue_summary": pd.DataFrame({"issue_type": [], "count": []}),
            }

    def save_results(self, results: Dict[str, pd.DataFrame]) -> None:
        """Save all results to both individual CSV files and a combined Excel report"""
        import os
        from pathlib import Path

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Create output directory if it doesn't exist
        output_dir = Path(f"kpi_reports_{timestamp}")
        output_dir.mkdir(exist_ok=True)

        # Process each DataFrame to handle timezone-aware datetimes
        processed_results = {}

        # Save individual CSV files
        for name, df in results.items():
            if df.empty:
                logger.warning(f"No data to save for {name}")
                continue

            df_processed = df.copy()

            # Convert datetime columns to timezone-naive
            for col in df_processed.select_dtypes(
                include=["datetime64[ns, UTC]"]
            ).columns:
                df_processed[col] = df_processed[col].dt.tz_localize(None)

            processed_results[name] = df_processed

            # Save individual CSV
            csv_path = output_dir / f"{name}.csv"
            df_processed.to_csv(csv_path, index=False)
            logger.info(f"Saved {name} to {csv_path}")

        # Save combined Excel report
        if processed_results:
            excel_path = output_dir / f"kpi_report_combined.xlsx"
            with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
                for name, df in processed_results.items():
                    sheet_name = name[:31]  # Excel sheet name limit
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            logger.info(f"\nSaved combined KPI report to {excel_path}")
        else:
            logger.warning("No data to save in any report")


def main():
    try:
        db_url = "postgresql://postgres:postgres@localhost:5435/etl_db"
        calculator = KPICalculator(db_url)
        results = calculator.calculate_all_kpis()

        # Print summary
        logger.info("\n=== KPI Calculation Summary ===")
        for name, df in results.items():
            logger.info(f"{name}: {len(df)} rows")

        return 0

    except Exception as e:
        logger.error(f"Error in KPI calculation: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
