# Supermarket POS ETL Pipeline with FastAPI

This is an ETL pipeline for processing supermarket POS data with a FastAPI backend. The application includes monitoring that checks for new data and sends alerts if no data is received by Friday.

## Features

- File upload and processing via FastAPI endpoints
- Data validation and transformation
- PostgreSQL database integration
- Automated monitoring for data ingestion
- Deployment ready for Render

## Prerequisites

- Python 3.9+
- PostgreSQL
- Render account (for deployment)

## Local Development

1. Create and activate a virtual environment:

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: .\venv\Scripts\activate
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Set up environment variables:
   Create a `.env` file in the root directory with the following variable:

   ```
   # Database Configuration
   DATABASE_URL=postgresql://postgres:postgres@localhost:5435/etl_db
   ```

   Make sure to update the database URL with your actual database credentials if different from the default.

4. Run the application:

   ```bash
   uvicorn main:app --reload
   ```

5. Access the API documentation at: <http://localhost:8000/docs>

## Deployment to Render

1. Push your code to a GitHub repository

2. Go to [Render Dashboard](https://dashboard.render.com/)

3. Click "New" and select "Web Service"

4. Connect your GitHub repository

5. Configure the service:
   - Name: etl-monitor
   - Region: Choose the closest to your users
   - Branch: main (or your preferred branch)
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `gunicorn -w 4 -k uvicorn.workers.UvicornWorker main:app`

6. Add environment variables:
   - `PYTHONUNBUFFERED`: 1
   - `DATABASE_URL`: Will be provided by Render

7. Click "Create Web Service"

## Database Setup

1. In the Render Dashboard, create a new PostgreSQL database
2. Note the connection string provided by Render
3. The application will automatically create the necessary tables on first run

## Monitoring

The application includes a monitoring system that:

- Checks for new data every hour
- Sends an email alert if no new data is received by Friday
- Logs all monitoring activities

## API Endpoints

- `POST /upload-and-process/`: Upload and process data files
- `GET /docs`: Interactive API documentation

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `DATABASE_URL` | PostgreSQL connection URL | Yes |
