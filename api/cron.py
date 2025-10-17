"""
FastAPI function for scheduled TTB COLA scraping and sync.
Designed for Railway deployment with built-in cron scheduling.
"""
import json
import os
import sys
import time
import logging
import asyncio
from typing import Dict, Any
from contextlib import asynccontextmanager

# Add parent directory to path to import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from loguru import logger
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.loguru import LoguruIntegration
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from scraper import TTBScraper
from adapters import AirtableAdapter
from sync_strategy import IncrementalSyncStrategy, FullSyncStrategy


# Configure loguru for serverless environment
logger.remove()  # Remove default handler
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO"
)

# Initialize Sentry with Loguru integration
sentry_dsn = os.getenv("SENTRY_DSN")
if sentry_dsn:
    sentry_sdk.init(
        dsn=sentry_dsn,
        integrations=[
            FastApiIntegration(transaction_style="endpoint"),
            LoguruIntegration(
                level=logging.INFO,        # Capture INFO and above
                event_level=logging.ERROR  # Create Sentry events for ERROR and above
            ),
        ],
        enable_logs=True,
        traces_sample_rate=1.0,
        profiles_sample_rate=1.0,
        environment=os.getenv("SENTRY_ENVIRONMENT", "production"),
        # Capture breadcrumbs (context) for debugging
        max_breadcrumbs=50,
        # Add release tracking if available
        release=os.getenv("RAILWAY_GIT_COMMIT_SHA"),
        # Set profile_session_sample_rate to 1.0 to profile 100%
        # of profile sessions.
        profile_session_sample_rate=1.0,
        # Set profile_lifecycle to "trace" to automatically
        # run the profiler on when there is an active transaction
        profile_lifecycle="trace",
    )
    logger.info("Sentry initialized successfully")
else:
    logger.warning("SENTRY_DSN not set - error reporting disabled")


# Initialize scheduler
scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for FastAPI app.
    Starts the scheduler on startup and shuts it down on shutdown.
    """
    # Startup
    cron_schedule = os.getenv("CRON_SCHEDULE", "0 0 * * *")  # Default: daily at midnight
    enable_cron = os.getenv("ENABLE_CRON", "true").lower() == "true"

    if enable_cron:
        logger.info(f"Configuring cron scheduler with schedule: {cron_schedule}")

        # Parse cron expression (minute hour day month day_of_week)
        try:
            scheduler.add_job(
                run_sync_job,
                trigger=CronTrigger.from_crontab(cron_schedule),
                id="ttb_cola_sync",
                name="TTB COLA Sync Job",
                replace_existing=True,
                misfire_grace_time=3600  # Allow 1 hour grace time for missed jobs
            )
            scheduler.start()
            logger.success(f"Cron scheduler started with schedule: {cron_schedule}")
            logger.info("Next scheduled run: " + str(scheduler.get_job("ttb_cola_sync").next_run_time))
        except Exception as e:
            logger.error(f"Failed to configure cron scheduler: {e}")
            logger.warning("Cron scheduling disabled, manual trigger still available")
    else:
        logger.info("Cron scheduling disabled via ENABLE_CRON=false")

    yield  # App is running

    # Shutdown
    if scheduler.running:
        scheduler.shutdown()
        logger.info("Cron scheduler shut down")


app = FastAPI(
    title="TTB COLA Scraper API",
    description="Scheduled scraping and sync of TTB COLA data to Airtable with built-in cron",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/api/cron")
async def get_cron(background_tasks: BackgroundTasks):
    """Handle GET request (manual trigger). Returns immediately and runs sync in background."""
    logger.info("Manual trigger via GET /api/cron")
    return await start_background_sync(background_tasks)


@app.post("/api/cron")
async def post_cron(background_tasks: BackgroundTasks):
    """Handle POST request (cron trigger). Returns immediately and runs sync in background."""
    logger.info("Manual trigger via POST /api/cron")
    return await start_background_sync(background_tasks)


@app.get("/api/cron/status")
async def cron_status():
    """Get the status of the cron scheduler."""
    if not scheduler.running:
        return {
            "enabled": False,
            "message": "Cron scheduler is not running"
        }

    job = scheduler.get_job("ttb_cola_sync")
    if not job:
        return {
            "enabled": False,
            "message": "Cron job not configured"
        }

    return {
        "enabled": True,
        "schedule": os.getenv("CRON_SCHEDULE", "0 0 * * *"),
        "next_run": str(job.next_run_time),
        "job_id": job.id,
        "job_name": job.name
    }


@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    cron_info = {}
    if scheduler.running:
        job = scheduler.get_job("ttb_cola_sync")
        if job:
            cron_info = {
                "cron_enabled": True,
                "next_run": str(job.next_run_time),
                "schedule": os.getenv("CRON_SCHEDULE", "0 0 * * *")
            }
        else:
            cron_info = {"cron_enabled": False}
    else:
        cron_info = {"cron_enabled": False}

    return {
        "status": "healthy",
        "timestamp": time.time(),
        "cron": cron_info
    }


async def start_background_sync(background_tasks: BackgroundTasks) -> Dict[str, Any]:
    """Start the sync process in the background and return immediately."""
    try:
        logger.info("=" * 80)
        logger.info("Railway Cron Job: TTB COLA Scraper - Starting background job")
        logger.info("=" * 80)

        # Verify required environment variables
        required_vars = ["AIRTABLE_API_KEY", "AIRTABLE_BASE_ID"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
            logger.error(error_msg)
            sentry_sdk.capture_message(error_msg, level="error")
            raise HTTPException(status_code=500, detail=error_msg)

        # Add the sync job to background tasks
        background_tasks.add_task(run_sync_job)

        # Return immediately
        return JSONResponse(
            status_code=202,
            content={
                "status": "accepted",
                "message": "TTB COLA sync job started in background",
                "timestamp": time.time()
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Failed to start background sync: {e}")
        sentry_sdk.capture_exception(e)
        return JSONResponse(
            status_code=500,
            content={
                "error": str(e),
                "status": "failed",
                "timestamp": time.time()
            }
        )


def run_sync_job():
    """Execute the sync process in the background."""
    try:
        logger.info("Background sync job started")

        # Initialize scraper
        logger.info("Initializing TTB scraper...")
        scraper = TTBScraper(
            product_names=json.loads(os.getenv("TTB_PRODUCT_NAMES", '["Shottys"]')),
            delay_between_requests=float(os.getenv("TTB_DELAY", "1.0"))
        )

        # Initialize Airtable adapter
        logger.info("Initializing Airtable adapter...")
        airtable = AirtableAdapter(
            table_name=os.getenv("AIRTABLE_TABLE_NAME", "TTB COLAs"),
            fetch_documents=os.getenv("FETCH_DOCUMENTS", "false").lower() == "true",
            two_captcha_api_key=os.getenv("TWO_CAPTCHA_API_KEY")
        )

        # Determine sync strategy
        sync_mode = os.getenv("SYNC_STRATEGY", "incremental").lower()
        logger.info(f"Using sync strategy: {sync_mode}")

        if sync_mode == "full":
            strategy = FullSyncStrategy(scraper, airtable)
        else:
            # Default to incremental for cron jobs
            strategy = IncrementalSyncStrategy(scraper, airtable)

        # Execute sync
        stats = strategy.sync()

        logger.success("Background sync completed successfully!")
        logger.info(f"Stats: {stats}")

    except Exception as e:
        logger.exception(f"Background sync failed: {e}")
        sentry_sdk.capture_exception(e)
# For local testing
if __name__ == "__main__":
    import uvicorn

    print("Starting local test server on http://localhost:8000")
    print("Visit http://localhost:8000/api/cron to trigger the sync")
    print("Visit http://localhost:8000/docs for API documentation")

    uvicorn.run(app, host="0.0.0.0", port=8000)
