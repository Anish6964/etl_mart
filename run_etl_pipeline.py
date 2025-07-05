#!/usr/bin/env python3
"""
ETL Pipeline Orchestrator
-------------------------
Processes data files through the ETL pipeline. Can be run directly or via API.
Runs the ETL pipeline in sequence:
1. Data ingestion (ingest_new.py)
2. KPI calculations (kpi_queries.py)
3. BI view creation (create_bi_views.py)

Usage:
    # Direct file specification
    python run_etl_pipeline.py --data-file <data_file> [--master-file <master_file>] [--retailer-id <id>]

    # Auto-discovery mode (looks for files in current directory)
    python run_etl_pipeline.py
"""

import subprocess
import sys
import logging
import os
import argparse
from pathlib import Path
from datetime import datetime
import glob

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def find_file(pattern, required=True):
    """Find a single file matching the pattern in the working directory."""
    # Handle patterns with wildcards
    files = list(Path(".").glob(pattern))

    # If no files found with wildcards, try exact match (for filenames with spaces)
    if not files and "?" not in pattern and "*" not in pattern and "[" not in pattern:
        exact_path = Path(pattern)
        if exact_path.exists():
            return str(exact_path)

    if not files:
        if required:
            logger.error(f"❌ No file found matching pattern: {pattern}")
            return None
        return None

    if len(files) > 1:
        logger.warning(f"⚠️  Multiple files match {pattern}, using: {files[0]}")

    # Return the path as a string without extra quotes
    return str(files[0].resolve())


def run_script(script_name, args=None):
    """Run a Python script and handle its output/errors."""
    if args is None:
        args = []

    script_path = Path(__file__).parent / script_name
    if not script_path.exists():
        logger.error(f"Script not found: {script_path}")
        return False

    # Handle the case where the script path contains spaces
    cmd = [sys.executable, str(script_path)]
    # Split any arguments that might be combined (like "--option=value")
    for arg in args:
        if "=" in arg and arg.startswith("--"):
            option, value = arg.split("=", 1)
            cmd.extend([option, value])
        else:
            cmd.append(arg)
    logger.info(f"Running: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, check=True, text=True, capture_output=True)
        logger.info(f"✅ {script_name} completed successfully")
        if result.stdout.strip():
            logger.debug(f"Output:\n{result.stdout}")
        if result.stderr.strip():
            logger.warning(f"Warnings:\n{result.stderr}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ {script_name} failed with error code {e.returncode}")
        if e.stdout.strip():
            logger.error(f"Output:\n{e.stdout}")
        if e.stderr.strip():
            logger.error(f"Error:\n{e.stderr}")
        return False


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run ETL Pipeline")
    parser.add_argument(
        "--data-file", type=str, help="Path to the input data file (CSV/Excel)"
    )
    parser.add_argument(
        "--master-file", type=str, help="Path to the master SKU file (Excel)"
    )
    parser.add_argument(
        "--retailer-id", type=str, default="1", help="Retailer ID (default: 1)"
    )
    parser.add_argument(
        "--auto-discover",
        action="store_true",
        help="Auto-discover files in current directory",
    )
    return parser.parse_args()


def find_data_file():
    """Find data file in current directory."""
    for pattern in ["*.csv", "*.xlsx"]:
        files = list(Path(".").glob(pattern))
        # Filter out any master files
        files = [f for f in files if "master" not in str(f).lower()]
        if files:
            data_file = str(files[0].resolve())
            logger.info(f"Found data file: {data_file}")
            return data_file
    return None


def find_master_file():
    """Find master file in current directory."""
    master_files = list(Path(".").glob("*master*.xlsx"))
    if master_files:
        master_file = str(master_files[0].resolve())
        logger.info(f"Found master file: {master_file}")
        return master_file
    logger.warning("⚠️  No master file found (looking for *master*.xlsx)")
    return None


def run_etl_pipeline(data_file: str, master_file: Optional[str] = None, retailer_id: str = "1") -> bool:
    """
    Run the ETL pipeline for the given files.
    
    Args:
        data_file: Path to the input data file (CSV/Excel)
        master_file: Optional path to the master SKU file (Excel)
        retailer_id: Retailer ID
        
    Returns:
        bool: True if pipeline completed successfully, False otherwise
    """
    logger.info(f"🚀 Starting ETL Pipeline for retailer {retailer_id}")
    logger.info("=" * 60)

    # Run data ingestion
    if not run_script("ingest_new.py", ["--data-file", data_file, "--retailer-id", retailer_id]):
        return False

    # Run KPI calculations
    if not run_script("kpi_queries.py", ["--retailer-id", retailer_id]):
        return False

    # Run BI view creation
    if not run_script("create_bi_views.py", ["--retailer-id", retailer_id]):
        return False

    logger.info("✅ ETL Pipeline completed successfully")
    return True

def main():
    args = parse_arguments()
    start_time = datetime.now()
    logger.info(f"🚀 Starting ETL Pipeline at {start_time}")
    logger.info("=" * 60)

    # Handle file discovery
    data_file = args.data_file
    master_file = args.master_file

    # Auto-discover files if no files provided and auto-discover is enabled
    if args.auto_discover or (not data_file and not args.master_file):
        logger.info("\n🔍 Looking for input files...")
        if not data_file:
            data_file = find_data_file()
            if not data_file:
                logger.error(
                    "❌ No data file found. Please specify with --data-file or place a CSV/Excel file in the directory."
                )
                sys.exit(1)

        if not master_file:
            master_file = find_master_file()

    # Verify data file exists
    if not data_file or not os.path.exists(data_file):
        logger.error(f"❌ Data file not found: {data_file}")
        sys.exit(1)

    # Verify master file exists if provided
    if master_file and not os.path.exists(master_file):
        logger.error(f"❌ Master file not found: {master_file}")
        sys.exit(1)

    # Build command arguments with proper file paths
    script_args = [data_file, "--retailer-id", args.retailer_id]
    if master_file:
        script_args.extend(["--master-file", master_file])

    # Step 1: Data Ingestion
    logger.info("\n" + "=" * 60)
    logger.info(f"📥 STEP 1: Running Data Ingestion on {Path(data_file).name}")
    if master_file:
        logger.info(f"Using master file: {Path(master_file).name}")
    logger.info("=" * 60)

    if not run_script("ingest_new.py", script_args):
        logger.error("❌ Pipeline failed at Data Ingestion step")
        sys.exit(1)

    # Step 2: KPI Calculations
    logger.info("\n" + "=" * 60)
    logger.info("📊 STEP 2: Running KPI Calculations")
    logger.info("=" * 60)
    if not run_script("kpi_queries.py"):
        logger.error("❌ Pipeline failed at KPI Calculations step")
        sys.exit(1)

    # Step 3: Create BI Views
    logger.info("\n" + "=" * 60)
    logger.info("📈 STEP 3: Creating BI Views")
    logger.info("=" * 60)
    if not run_script("create_bi_views.py"):
        logger.error("❌ Pipeline failed at BI View Creation step")
        sys.exit(1)

    # Completion
    duration = datetime.now() - start_time
    logger.info("\n" + "=" * 60)
    logger.info(f"✅ ETL Pipeline completed successfully in {duration}")
    logger.info("=" * 60)


def run_etl_pipeline(
    data_file: str, master_file: str = None, retailer_id: str = "1"
) -> bool:
    """Run the ETL pipeline with specified files.

    Args:
        data_file: Path to the input data file
        master_file: Optional path to the master SKU file
        retailer_id: Retailer ID (default: "1")

    Returns:
        bool: True if pipeline completed successfully, False otherwise
    """
    try:
        script_args = [data_file, "--retailer-id", str(retailer_id)]
        if master_file:
            script_args.extend(["--master-file", master_file])

        # Run the pipeline
        success = (
            run_script("ingest_new.py", script_args)
            and run_script("kpi_queries.py")
            and run_script("create_bi_views.py")
        )

        if success:
            logger.info("✅ ETL Pipeline completed successfully")
        else:
            logger.error("❌ ETL Pipeline failed")

        return success

    except Exception as e:
        logger.error(f"❌ Error in ETL pipeline: {str(e)}")
        logger.debug(traceback.format_exc())
        return False


if __name__ == "__main__":
    main()
