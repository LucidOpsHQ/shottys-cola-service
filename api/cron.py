"""
Vercel serverless function for scheduled TTB COLA scraping and sync.
"""
import os
import sys
from http.server import BaseHTTPRequestHandler
import json

# Add parent directory to path to import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
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


class handler(BaseHTTPRequestHandler):
    """Vercel serverless function handler."""

    def do_GET(self):
        """Handle GET request (manual trigger)."""
        self._run_sync()

    def do_POST(self):
        """Handle POST request (cron trigger)."""
        self._run_sync()

    def _run_sync(self):
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
                self._send_response(500, {
                    "error": error_msg,
                    "status": "failed"
                })
                return

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
            import time
            self._send_response(200, {
                "status": "success",
                "message": "TTB COLA sync completed",
                "stats": stats,
                "timestamp": time.time()
            })

        except Exception as e:
            logger.exception(f"Sync failed: {e}")
            self._send_response(500, {
                "error": str(e),
                "status": "failed"
            })

    def _send_response(self, status_code: int, data: dict):
        """Send JSON response."""
        self.send_response(status_code)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())


# For local testing
if __name__ == "__main__":
    from http.server import HTTPServer

    print("Starting local test server on http://localhost:8000")
    print("Visit http://localhost:8000/api/cron to trigger the sync")

    server = HTTPServer(("localhost", 8000), handler)
    server.serve_forever()
