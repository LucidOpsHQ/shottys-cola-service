"""
Main entrypoint for TTB COLA scraper and sync.
"""
import os
import sys
import json
from loguru import logger
from dotenv import load_dotenv

from scraper import TTBScraper
from adapters import AirtableAdapter
from sync_strategy import IncrementalSyncStrategy, FullSyncStrategy, ReplaceSyncStrategy


# Configure loguru logger
logger.remove()  # Remove default handler
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO"
)
logger.add(
    "scraper_{time:YYYY-MM-DD}.log",
    rotation="00:00",
    retention="30 days",
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
)


def main():
    """Main execution flow."""
    logger.info("=" * 80)
    logger.info("TTB COLA Scraper & Sync Tool")
    logger.info("=" * 80)
    load_dotenv()
    try:
        # Initialize scraper (data source)
        logger.info("Initializing TTB scraper...")
        scraper = TTBScraper(
            product_name=os.getenv("TTB_PRODUCT_NAME", "Shottys"),
            vendor_code=os.getenv("TTB_VENDOR_CODE", "23153"),
            delay_between_requests=float(os.getenv("TTB_DELAY", "1.0"))
        )

        # Check if Airtable credentials are available
        if not os.getenv("AIRTABLE_API_KEY") or not os.getenv("AIRTABLE_BASE_ID"):
            logger.warning("Airtable credentials not found!")
            logger.warning("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID environment variables to enable sync.")
            logger.info("\nRunning scraper in standalone mode (no sync)...\n")

            # Run scraper only
            items = scraper.scrape()

            # Save results as JSON
            output_file = "ttb_results.json"
            logger.info(f"Saving {len(items)} items to {output_file}")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump([item.model_dump() for item in items], f, indent=2)
            logger.success(f"Results saved to {output_file}")

            logger.info("=" * 80)
            logger.success("Scraping completed successfully!")
            logger.info("=" * 80)
            return

        # Check document fetching configuration
        fetch_documents = os.getenv("FETCH_DOCUMENTS", "false").lower() == "true"
        two_captcha_api_key = os.getenv("TWO_CAPTCHA_API_KEY")

        if fetch_documents:
            logger.info("Document fetching is ENABLED")
            if not two_captcha_api_key:
                logger.error("FETCH_DOCUMENTS=true but TWO_CAPTCHA_API_KEY is not set!")
                logger.error("Please set TWO_CAPTCHA_API_KEY in your .env file")
                return
            logger.info("2Captcha API key found - will fetch and upload PDFs")
        else:
            logger.info("Document fetching is DISABLED (set FETCH_DOCUMENTS=true to enable)")

        # Initialize storage adapter
        logger.info("Initializing Airtable adapter...")
        airtable = AirtableAdapter(
            table_name=os.getenv("AIRTABLE_TABLE_NAME", "TTB COLAs"),
            fetch_documents=fetch_documents,
            two_captcha_api_key=two_captcha_api_key
        )

        # Determine sync strategy from environment variable
        sync_mode = os.getenv("SYNC_STRATEGY", "incremental").lower()

        logger.info(f"Using sync strategy: {sync_mode}")

        if sync_mode == "incremental":
            strategy = IncrementalSyncStrategy(scraper, airtable)
        elif sync_mode == "full":
            strategy = FullSyncStrategy(scraper, airtable)
        elif sync_mode == "replace":
            logger.warning("REPLACE strategy selected - this will DELETE all existing data!")
            confirm = os.getenv("CONFIRM_REPLACE", "false").lower()
            if confirm != "true":
                logger.error("REPLACE strategy requires CONFIRM_REPLACE=true environment variable")
                logger.error("Aborting to prevent accidental data loss")
                return
            strategy = ReplaceSyncStrategy(scraper, airtable)
        else:
            logger.error(f"Unknown sync strategy: {sync_mode}")
            logger.error("Valid strategies: incremental, full, replace")
            return

        # Execute sync strategy
        stats = strategy.sync()

        # Save results as JSON backup
        logger.info("\nCreating JSON backup...")
        items = scraper.scrape() if sync_mode != "replace" else []
        if items or sync_mode == "replace":
            output_file = "ttb_results_backup.json"
            logger.info(f"Saving backup to {output_file}")
            # Re-scrape if we didn't just scrape
            if not items:
                items = scraper.scrape()
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump([item.model_dump() for item in items], f, indent=2)
            logger.success(f"Backup saved to {output_file}")

        logger.info("\n" + "=" * 80)
        logger.success("All operations completed successfully!")
        logger.info("=" * 80)

    except KeyboardInterrupt:
        logger.warning("\n\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
