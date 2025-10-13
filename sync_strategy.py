"""
Sync strategies for coordinating data retrieval and storage.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Protocol
from loguru import logger

from models import TTBItem


class DataSource(Protocol):
    """Protocol for data sources that can retrieve TTB items."""

    def scrape(self) -> List[TTBItem]:
        """Retrieve TTB items from the data source."""
        ...


class StorageTarget(Protocol):
    """Protocol for storage targets that can store TTB items."""

    def get_existing_ids(self) -> set:
        """Get set of existing TTB IDs."""
        ...

    def create_items(self, items: List[TTBItem]) -> int:
        """Create new items in storage."""
        ...

    def update_item(self, item: TTBItem) -> bool:
        """Update an existing item in storage."""
        ...

    def mark_as_deprecated(self, ttb_ids: List[str]) -> int:
        """Mark records as deprecated."""
        ...


class SyncStrategy(ABC):
    """Abstract base class for sync strategies."""

    def __init__(self, data_source: DataSource, storage_target: StorageTarget):
        """
        Initialize sync strategy.

        Args:
            data_source: Source to retrieve data from
            storage_target: Target to store data to
        """
        self.data_source = data_source
        self.storage_target = storage_target

    @abstractmethod
    def sync(self) -> Dict[str, int]:
        """
        Execute the sync strategy.

        Returns:
            Dictionary with sync statistics
        """
        pass


class IncrementalSyncStrategy(SyncStrategy):
    """
    Sync strategy that only creates new records, skipping existing ones.
    Also marks records as deprecated if they're not in the latest scrape.
    Most efficient for regular updates.
    """

    def sync(self) -> Dict[str, int]:
        """
        Sync data incrementally - only create new records and mark deprecated ones.

        Returns:
            Dictionary with sync statistics (total, new, skipped, deprecated)
        """
        logger.info("=" * 60)
        logger.info("Starting Incremental Sync Strategy")
        logger.info("=" * 60)

        # Step 1: Retrieve data from source
        logger.info("Step 1: Retrieving data from source...")
        items = self.data_source.scrape()
        logger.info(f"Retrieved {len(items)} items from source")

        if not items:
            logger.warning("No items to sync")
            return {"total": 0, "new": 0, "skipped": 0, "deprecated": 0}

        # Step 2: Get existing IDs from storage
        logger.info("Step 2: Checking for existing records in storage...")
        existing_ids = self.storage_target.get_existing_ids()
        logger.info(f"Found {len(existing_ids)} existing records in storage")

        # Step 3: Filter out existing items
        scraped_ids = {item.ttb_id for item in items}
        new_items = [item for item in items if item.ttb_id not in existing_ids]
        skipped_count = len(items) - len(new_items)

        logger.info(f"Step 3: Filtering results - {len(new_items)} new, {skipped_count} already exist")

        # Step 4: Create new items in storage
        if new_items:
            logger.info(f"Step 4: Creating {len(new_items)} new records in storage...")
            created_count = self.storage_target.create_items(new_items)
        else:
            logger.info("Step 4: No new records to create")
            created_count = 0

        # Step 5: Mark deprecated records (existing in storage but not in latest scrape)
        deprecated_ids = existing_ids - scraped_ids
        deprecated_count = 0

        if deprecated_ids:
            logger.info(f"Step 5: Marking {len(deprecated_ids)} records as deprecated...")
            deprecated_count = self.storage_target.mark_as_deprecated(list(deprecated_ids))
        else:
            logger.info("Step 5: No records to mark as deprecated")

        stats = {
            "total": len(items),
            "new": created_count,
            "skipped": skipped_count,
            "deprecated": deprecated_count
        }

        logger.success("=" * 60)
        logger.success(f"Incremental Sync Completed!")
        logger.success(f"Total: {stats['total']} | New: {stats['new']} | Skipped: {stats['skipped']} | Deprecated: {stats['deprecated']}")
        logger.success("=" * 60)

        return stats


class FullSyncStrategy(SyncStrategy):
    """
    Sync strategy that updates existing records and creates new ones.
    More thorough but slower.
    """

    def sync(self) -> Dict[str, int]:
        """
        Sync data fully - update existing records and create new ones.

        Returns:
            Dictionary with sync statistics (total, new, updated)
        """
        logger.info("=" * 60)
        logger.info("Starting Full Sync Strategy")
        logger.info("=" * 60)

        # Step 1: Retrieve data from source
        logger.info("Step 1: Retrieving data from source...")
        items = self.data_source.scrape()
        logger.info(f"Retrieved {len(items)} items from source")

        if not items:
            logger.warning("No items to sync")
            return {"total": 0, "new": 0, "updated": 0}

        # Step 2: Get existing IDs from storage
        logger.info("Step 2: Checking for existing records in storage...")
        existing_ids = self.storage_target.get_existing_ids()
        logger.info(f"Found {len(existing_ids)} existing records in storage")

        # Step 3: Separate new and existing items
        new_items = []
        existing_items = []

        for item in items:
            if item.ttb_id in existing_ids:
                existing_items.append(item)
            else:
                new_items.append(item)

        logger.info(f"Step 3: Categorizing - {len(new_items)} new, {len(existing_items)} to update")

        # Step 4: Create new items
        created_count = 0
        if new_items:
            logger.info(f"Step 4a: Creating {len(new_items)} new records...")
            created_count = self.storage_target.create_items(new_items)
        else:
            logger.info("Step 4a: No new records to create")

        # Step 5: Update existing items
        updated_count = 0
        if existing_items:
            logger.info(f"Step 4b: Updating {len(existing_items)} existing records...")
            for item in existing_items:
                if self.storage_target.update_item(item):
                    updated_count += 1
        else:
            logger.info("Step 4b: No records to update")

        stats = {
            "total": len(items),
            "new": created_count,
            "updated": updated_count
        }

        logger.success("=" * 60)
        logger.success(f"Full Sync Completed!")
        logger.success(f"Total: {stats['total']} | New: {stats['new']} | Updated: {stats['updated']}")
        logger.success("=" * 60)

        return stats


class ReplaceSyncStrategy(SyncStrategy):
    """
    Sync strategy that deletes all existing records and replaces with new data.
    WARNING: Destructive operation!
    """

    def sync(self) -> Dict[str, int]:
        """
        Sync data by replacing all existing records.
        WARNING: This deletes all existing data!

        Returns:
            Dictionary with sync statistics (deleted, created)
        """
        logger.warning("=" * 60)
        logger.warning("Starting Replace Sync Strategy (DESTRUCTIVE)")
        logger.warning("=" * 60)

        # Step 1: Delete all existing records
        logger.warning("Step 1: Deleting ALL existing records...")
        deleted_count = self.storage_target.delete_all()
        logger.info(f"Deleted {deleted_count} records")

        # Step 2: Retrieve data from source
        logger.info("Step 2: Retrieving data from source...")
        items = self.data_source.scrape()
        logger.info(f"Retrieved {len(items)} items from source")

        # Step 3: Create all items
        created_count = 0
        if items:
            logger.info(f"Step 3: Creating {len(items)} new records...")
            created_count = self.storage_target.create_items(items)
        else:
            logger.warning("Step 3: No items to create")

        stats = {
            "deleted": deleted_count,
            "created": created_count
        }

        logger.success("=" * 60)
        logger.success(f"Replace Sync Completed!")
        logger.success(f"Deleted: {stats['deleted']} | Created: {stats['created']}")
        logger.success("=" * 60)

        return stats
