from fastapi import FastAPI, HTTPException, UploadFile, File, Form, status, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from typing import Optional, Dict, Any, List
import uvicorn
import os
import logging
from datetime import datetime, timedelta
import traceback
import shutil
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pathlib

# Configure logging first
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database configuration
DB_URL = os.getenv('DATABASE_URL')
if not DB_URL:
    logger.error("DATABASE_URL environment variable not found. Using default connection string")
    DB_URL = "postgresql://etl_user:etl_password@localhost:5432/etl_db"

# Create engine once and reuse it
engine = create_engine(DB_URL)

# Test database connection
try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).fetchone()
    logger.info("Database connection successful")
except Exception as e:
    logger.error(f"Database connection failed: {str(e)}")
    raise

app = FastAPI(
    title="Supermarket POS ETL API",
    description="API for managing the supermarket POS ETL pipeline with monitoring",
    version="1.0.0"
)

last_successful_load = None

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    port = int(os.getenv('PORT', 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

def get_table_columns(engine, table_name):
    """Get list of columns in a table"""
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = :table_name
                """
            ),
            {"table_name": table_name}
        )
        return [row[0] for row in result.fetchall()]

def check_data_ingestion():
    """Check if new data has been ingested"""
    global last_successful_load
    logger.info("Running scheduled data ingestion check...")
    
    try:
        engine = create_engine(DB_URL)
        
        # First, check available columns in fact_sales
        columns = get_table_columns(engine, 'fact_sales')
        logger.info(f"Available columns in fact_sales: {columns}")
        
        # Try to find a date column
        date_columns = [col for col in columns if 'date' in col.lower() or 'time' in col.lower()]
        
        if not date_columns:
            logger.error("No date column found in fact_sales table")
            return
            
        date_column = date_columns[0]  # Use the first date column found
        logger.info(f"Using column '{date_column}' for date checking")
        
        # Get the most recent date from the fact_sales table
        with engine.connect() as conn:
            result = conn.execute(
                text(f"""
                SELECT MAX({date_column}) as last_load 
                FROM fact_sales
                """)
            )
            row = result.fetchone()
            last_successful_load = row[0] if row and row[0] else None
            
            # If it's Friday and no data in the last 24 hours, log a warning
            if datetime.now().weekday() == 4:  # Friday is 4 (Monday is 0)
                if not last_successful_load:
                    logger.warning("No data found in fact_sales table")
                else:
                    # Convert both dates to datetime for comparison
                    last_load = last_successful_load if isinstance(last_successful_load, datetime) else \
                              datetime.combine(last_successful_load, datetime.min.time())
                    time_since_last_load = datetime.now() - last_load
                    
                    if time_since_last_load > timedelta(hours=24):
                        logger.warning(f"No new data detected in the last 24 hours - last load was {time_since_last_load} ago")
    except Exception as e:
        logger.error(f"Error in scheduled data ingestion check: {str(e)}")
        logger.error(traceback.format_exc())

# Initialize the scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(
    check_data_ingestion,
    trigger=CronTrigger(day_of_week='fri', hour=9, minute=0),
    id='check_data_ingestion',
    name='Check for new data every Friday',
    replace_existing=True
)

# Start the scheduler when the app starts
@app.on_event("startup")
async def startup_event():
    # Start the scheduler
    if not scheduler.running:
        scheduler.start()
        logger.info("Scheduler started")
    
    # Run the check immediately on startup
    check_data_ingestion()

# Properly shut down the scheduler when the app shuts down
@app.on_event("shutdown")
def shutdown_event():
    if scheduler.running:
        scheduler.shutdown()
        logger.info("Scheduler shut down")

# Directory to store uploaded files
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Allowed file extensions
ALLOWED_EXTENSIONS = {
    'data': ['.csv', '.xlsx', '.xls'],
    'master': ['.xlsx', '.xls']
}

@app.get("/")
async def root():
    """Root endpoint that provides API information"""
    return {
        "message": "Supermarket POS ETL API",
        "endpoints": {
            "POST /upload": "Upload a new data file",
            "POST /process": "Process the uploaded files with ETL",
            "GET /status": "Get current ETL process status"
        }
    }

def validate_file_extension(filename: str, file_type: str) -> bool:
    """Check if the file extension is allowed for the given file type."""
    if not filename:
        return False
    ext = Path(filename).suffix.lower()
    return ext in ALLOWED_EXTENSIONS.get(file_type, [])

# Health check endpoint
@app.get("/health")
async def health_check() -> Dict[str, str]:
    """Health check endpoint for monitoring"""
    return {"status": "healthy"}

# Add startup event to ensure upload directory exists and database schema is initialized
@app.on_event("startup")
async def startup_event():
    """Initialize application services and database schema."""
    try:
        from initialize_db import create_tables
        create_tables(engine)
        logger.info("Database schema initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database schema: {str(e)}")
        raise
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    logger.info("Application startup: Initialized upload directory")

@app.post("/upload-and-process/")
async def upload_and_process_files(
    data_file: UploadFile = File(...),
    master_file: Optional[UploadFile] = File(None),
    retailer_id: str = Form(...)
):
    """
    Upload data and master files and immediately process them through the ETL pipeline.
    
    - **data_file**: CSV file containing the sales data
    - **master_file**: (Optional) Excel file containing master SKU data
    - **retailer_id**: ID of the retailer
    """
    global last_successful_load
    
    # Create uploads directory if it doesn't exist
    os.makedirs("uploads", exist_ok=True)
    
    # Generate unique filenames with timestamps
    timestamp = int(datetime.now().timestamp())
    data_filename = f"data_{timestamp}.csv"
    master_filename = f"master_{timestamp}.xlsx" if master_file else None
    
    try:
        # Save the uploaded files
        data_filepath = os.path.join("uploads", data_filename)
        with open(data_filepath, "wb") as buffer:
            shutil.copyfileobj(data_file.file, buffer)
        
        if master_file:
            master_filepath = os.path.join("uploads", master_filename)
            with open(master_filepath, "wb") as buffer:
                shutil.copyfileobj(master_file.file, buffer)
        
        # Run the ETL pipeline
        result = run_etl_pipeline(
            data_file=data_filepath,
            master_file=master_filepath if master_file else None,
            retailer_id=retailer_id
        )
        
        # Update the last successful load time
        last_successful_load = datetime.now()
        
        # Log the successful load
        logger.info(f"Successfully processed data file: {data_filename}")
        if master_file:
            logger.info(f"Processed with master file: {master_filename}")
            
        response = {
            "status": "success", 
            "message": "ETL pipeline completed successfully", 
            "last_load_time": last_successful_load.isoformat(),
            "details": result,
            "master_file": master_filename if master_file else None,
            "retailer_id": retailer_id
        }
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in upload and process: {str(e)}\n{traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing files: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    
    # Configure logging for development
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Start the FastAPI application
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )