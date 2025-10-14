"""
FastAPI serverless function for scheduled TTB COLA scraping and sync.
"""
import os
import sys
import time
import logging
from typing import Dict, Any

# Add parent directory to path to import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from loguru import logger
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.loguru import LoguruIntegration

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
        release=os.getenv("VERCEL_GIT_COMMIT_SHA"),
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


app = FastAPI(
    title="TTB COLA Scraper API",
    description="Scheduled scraping and sync of TTB COLA data to Airtable",
    version="1.0.0"
)


@app.get("/api/cron")
async def get_cron():
    """Handle GET request (manual trigger)."""
    return await run_sync()


@app.post("/api/cron")
async def post_cron():
    """Handle POST request (cron trigger)."""
    return await run_sync()


@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": time.time()}


async def run_sync() -> Dict[str, Any]:
    """Execute the sync process."""
    try:
        logger.info("=" * 80)
        logger.info("Vercel Cron Job: TTB COLA Scraper")
        logger.info("=" * 80)

        # Verify required environment variables
        required_vars = ["AIRTABLE_API_KEY", "AIRTABLE_BASE_ID"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
            logger.error(error_msg)
            sentry_sdk.capture_message(error_msg, level="error")
            raise HTTPException(status_code=500, detail=error_msg)

        # Initialize scraper
        logger.info("Initializing TTB scraper...")
        scraper = TTBScraper(
            product_name=os.getenv("TTB_PRODUCT_NAME", "Shottys"),
            vendor_code=os.getenv("TTB_VENDOR_CODE", "23153"),
            delay_between_requests=float(os.getenv("TTB_DELAY", "1.0"))
        )

        # Initialize Airtable adapter
        logger.info("Initializing Airtable adapter...")
        airtable = AirtableAdapter(
            table_name=os.getenv("AIRTABLE_TABLE_NAME", "TTB COLAs")
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

        logger.success("Sync completed successfully!")
        logger.info(f"Stats: {stats}")

        # Send success response
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": "TTB COLA sync completed",
                "stats": stats,
                "timestamp": time.time()
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Sync failed: {e}")
        sentry_sdk.capture_exception(e)
        return JSONResponse(
            status_code=500,
            content={
                "error": str(e),
                "status": "failed",
                "timestamp": time.time()
            }
        )


# For Vercel serverless
from mangum import Mangum
handler = Mangum(app)


# For local testing
if __name__ == "__main__":
    import uvicorn

    print("Starting local test server on http://localhost:8000")
    print("Visit http://localhost:8000/api/cron to trigger the sync")
    print("Visit http://localhost:8000/docs for API documentation")

    uvicorn.run(app, host="0.0.0.0", port=8000)
