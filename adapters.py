"""
Adapters for syncing TTB data to external services.
"""
import os
import io
import time
from abc import ABC, abstractmethod
from typing import List, Dict, Set, Optional
from pathlib import Path
from pyairtable import Api
from loguru import logger

from models import TTBItem


class StorageAdapter(ABC):
    """Abstract base class for storage adapters."""

    @abstractmethod
    def get_existing_ids(self) -> Set[str]:
        """Get set of existing TTB IDs from storage."""
        pass

    @abstractmethod
    def create_items(self, items: List[TTBItem]) -> int:
        """
        Create new items in storage.

        Args:
            items: List of TTBItem models to create

        Returns:
            Number of items created
        """
        pass

    @abstractmethod
    def update_item(self, item: TTBItem) -> bool:
        """
        Update an existing item in storage.

        Args:
            item: TTBItem model to update

        Returns:
            True if successful, False otherwise
        """
        pass

    @abstractmethod
    def mark_as_deprecated(self, ttb_ids: List[str]) -> int:
        """
        Mark records as deprecated.

        Args:
            ttb_ids: List of TTB IDs to mark as deprecated

        Returns:
            Number of records marked as deprecated
        """
        pass


class AirtableAdapter(StorageAdapter):
    """Adapter for syncing data to Airtable."""

    def __init__(
        self,
        api_key: str = None,
        base_id: str = None,
        table_name: str = "TTB COLAs",
        fetch_documents: bool = False,
        two_captcha_api_key: str = None
    ):
        """
        Initialize Airtable adapter.

        Args:
            api_key: Airtable API key (or set AIRTABLE_API_KEY env var)
            base_id: Airtable Base ID (or set AIRTABLE_BASE_ID env var)
            table_name: Name of the table to sync to
            fetch_documents: Whether to automatically fetch and upload HTML documents
            two_captcha_api_key: 2Captcha API key (required if fetch_documents=True)
        """
        self.api_key = api_key or os.getenv("AIRTABLE_API_KEY")
        self.base_id = base_id or os.getenv("AIRTABLE_BASE_ID")
        self.table_name = table_name

        self.fetch_documents = fetch_documents
        self.two_captcha_api_key = two_captcha_api_key or os.getenv("TWO_CAPTCHA_API_KEY")

        if not self.api_key:
            raise ValueError("Airtable API key not provided. Set AIRTABLE_API_KEY environment variable or pass api_key parameter.")

        if not self.base_id:
            raise ValueError("Airtable Base ID not provided. Set AIRTABLE_BASE_ID environment variable or pass base_id parameter.")

        if self.fetch_documents and not self.two_captcha_api_key:
            raise ValueError("TWO_CAPTCHA_API_KEY required when fetch_documents=True")

        self.api = Api(self.api_key)
        self.table = self.api.table(self.base_id, self.table_name)

        # Initialize document fetcher if enabled
        self.document_fetcher = None
        if self.fetch_documents:
            from cola_document_fetcher import ColaDocumentFetcher
            self.document_fetcher = ColaDocumentFetcher(self.two_captcha_api_key)

        logger.info(
            f"Initialized Airtable adapter for base {self.base_id}, "
            f"table '{self.table_name}' (fetch_documents={self.fetch_documents})"
        )

    def _item_to_record(self, item: TTBItem) -> Dict:
        """
        Convert TTBItem to Airtable record format.

        Args:
            item: TTBItem model

        Returns:
            Dictionary with Airtable field names and values
        """
        from datetime import datetime

        # Convert TTB ID to integer (remove leading zeros if any)
        try:
            ttb_id_number = int(item.ttb_id)
        except (ValueError, TypeError):
            ttb_id_number = None

        # Convert completed date to ISO format (YYYY-MM-DD) for Airtable Date field
        completed_date_iso = None
        if item.completed_date:
            try:
                # Parse MM/DD/YYYY format
                date_obj = datetime.strptime(item.completed_date, "%m/%d/%Y")
                completed_date_iso = date_obj.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                logger.warning(f"Could not parse completed date: {item.completed_date}")

        # Convert approval date to ISO format (YYYY-MM-DD) for Airtable Date field
        approval_date_iso = None
        if item.approval_date:
            try:
                # Parse MM/DD/YYYY format
                date_obj = datetime.strptime(item.approval_date, "%m/%d/%Y")
                approval_date_iso = date_obj.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                logger.warning(f"Could not parse approval date: {item.approval_date}")

        record = {
            # Basic fields from list page
            "TTB ID": ttb_id_number,
            "Permit No": item.permit_no,
            "Serial Number": item.serial_number,
            "Completed Date": completed_date_iso,
            "Fanciful Name": item.fanciful_name,
            "Brand Name": item.brand_name,
            "Origin Code": item.origin_code,
            "Origin Desc": item.origin_desc,
            "Class/Type": item.class_type,
            "Class/Type Desc": item.class_type_desc,
            "URL": item.url,
            "Deprecated": False,  # Mark as not deprecated when syncing

            # Detail page fields
            "Status": item.status,
            "Vendor Code": item.vendor_code,
            "Type of Application": item.type_of_application,
            "For Sale In": item.for_sale_in,
            "Total Bottle Capacity": item.total_bottle_capacity,
            "Grape Varietals": item.grape_varietals,
            "Wine Vintage": item.wine_vintage,
            "Formula": item.formula,
            "Lab No": item.lab_no,
            "Approval Date": approval_date_iso,  # Date field (YYYY-MM-DD)
            "Qualifications": item.qualifications,

            # Applicant information
            "Applicant Name": item.applicant_name,
            "Applicant Address": item.applicant_address,
            "Applicant City": item.applicant_city,
            "Applicant State": item.applicant_state,
            "Applicant ZIP": item.applicant_zip,

            # Contact information
            "Contact Name": item.contact_name,
            "Contact Phone": item.contact_phone,
            "Contact Email": item.contact_email,
        }
        return record

    def get_existing_ids(self) -> Set[str]:
        """
        Get set of existing TTB IDs from Airtable.

        Returns:
            Set of existing TTB IDs
        """
        logger.debug("Fetching existing TTB IDs from Airtable...")

        try:
            records = self.table.all(fields=["TTB ID"])
            existing_ids = {str(record["fields"].get("TTB ID")) for record in records if record["fields"].get("TTB ID")}
            logger.info(f"Found {len(existing_ids)} existing records in Airtable")
            return existing_ids
        except Exception as e:
            logger.error(f"Failed to fetch existing records: {e}")
            return set()

    def get_all_records(self) -> Dict[str, str]:
        """
        Get all existing records with their Airtable record IDs.

        Returns:
            Dictionary mapping TTB ID (as string) to Airtable record ID
        """
        logger.debug("Fetching all records from Airtable...")

        try:
            records = self.table.all(fields=["TTB ID"])
            record_map = {
                str(record["fields"].get("TTB ID")): record["id"]
                for record in records
                if record["fields"].get("TTB ID")
            }
            logger.info(f"Found {len(record_map)} existing records in Airtable")
            return record_map
        except Exception as e:
            logger.error(f"Failed to fetch all records: {e}")
            return {}

    def mark_as_deprecated(self, ttb_ids: List[str]) -> int:
        """
        Mark records as deprecated by TTB ID.

        Args:
            ttb_ids: List of TTB IDs to mark as deprecated

        Returns:
            Number of records marked as deprecated
        """
        if not ttb_ids:
            logger.info("No records to mark as deprecated")
            return 0

        logger.info(f"Marking {len(ttb_ids)} records as deprecated...")

        try:
            marked_count = 0

            # Process in batches
            for ttb_id in ttb_ids:
                try:
                    # Find the record by TTB ID
                    formula = f"{{TTB ID}} = {ttb_id}"
                    existing_records = self.table.all(formula=formula)

                    if existing_records:
                        record_id = existing_records[0]["id"]
                        self.table.update(record_id, {"Deprecated": True})
                        marked_count += 1
                        logger.debug(f"Marked as deprecated: {ttb_id}")
                    else:
                        logger.warning(f"Record not found: {ttb_id}")
                except Exception as e:
                    logger.error(f"Failed to mark {ttb_id} as deprecated: {e}")
                    continue

            logger.success(f"Successfully marked {marked_count} records as deprecated")
            return marked_count

        except Exception as e:
            logger.exception(f"Failed to mark records as deprecated: {e}")
            raise

    def _upload_html_to_record(self, ttb_id: str, record_id: str, html_content: str) -> bool:
        """
        Upload HTML content to Airtable record attachment field.

        Args:
            ttb_id: TTB ID (for logging and filename)
            record_id: Airtable record ID
            html_content: HTML content as string

        Returns:
            True if successful
        """
        try:
            # Convert HTML string to bytes
            html_bytes = html_content.encode('utf-8')

            # Upload attachment using pyairtable's upload method
            filename = f"COLA_{ttb_id}.html"

            # Create attachment using pyairtable Api's upload method
            attachment = self.table.upload_attachment(
                record_id,
                "COLA",  # Field name
                filename,
                html_bytes
            )

            logger.success(f"Uploaded HTML to record {record_id} for TTB ID {ttb_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to upload HTML for {ttb_id}: {e}")
            return False

    def _fetch_and_upload_document(self, ttb_id: str, record_id: str) -> bool:
        """
        Fetch document and upload HTML to Airtable record.

        Args:
            ttb_id: TTB ID to fetch document for
            record_id: Airtable record ID to update

        Returns:
            True if successful
        """
        if not self.document_fetcher:
            logger.warning("Document fetcher not initialized")
            return False

        try:
            logger.info(f"Fetching and uploading document for TTB ID: {ttb_id}")

            # Fetch HTML content
            html_content = self.document_fetcher.fetch_document_html(ttb_id)

            if not html_content:
                logger.error(f"Failed to fetch document for {ttb_id}")
                return False

            # Upload HTML to Airtable
            success = self._upload_html_to_record(ttb_id, record_id, html_content)

            if success:
                logger.success(f"Successfully uploaded HTML for {ttb_id}")

            return success

        except Exception as e:
            logger.error(f"Error fetching and uploading document for {ttb_id}: {e}")
            return False

    def create_items(self, items: List[TTBItem]) -> int:
        """
        Create new items in Airtable using batch operations.
        If fetch_documents is enabled, also fetches and uploads HTML documents for each item.

        Args:
            items: List of TTBItem models to create

        Returns:
            Number of items created
        """
        if not items:
            logger.info("No items to create")
            return 0

        logger.info(f"Creating {len(items)} new records in Airtable...")

        try:
            # Airtable batch API accepts max 10 records at a time
            batch_size = 10
            created_count = 0

            for i in range(0, len(items), batch_size):
                batch = items[i:i + batch_size]
                records_data = [self._item_to_record(item) for item in batch]

                created_records = self.table.batch_create(records_data)
                created_count += len(batch)
                logger.debug(f"Created batch {i // batch_size + 1}: {len(batch)} records")

                # Fetch and upload documents if enabled
                if self.fetch_documents and self.document_fetcher:
                    for idx, item in enumerate(batch):
                        record_id = created_records[idx]['id']
                        self._fetch_and_upload_document(item.ttb_id, record_id)
                        # Small delay to avoid rate limiting
                        time.sleep(0.5)

            logger.success(f"Successfully created {created_count} records in Airtable")
            return created_count

        except Exception as e:
            logger.exception(f"Failed to create items in Airtable: {e}")
            raise

    def update_item(self, item: TTBItem) -> bool:
        """
        Update an existing item in Airtable.
        If fetch_documents is enabled, also fetches and uploads HTML document for the item.

        Args:
            item: TTBItem model to update

        Returns:
            True if successful, False otherwise
        """
        try:
            # Find the record by TTB ID
            formula = f"{{TTB ID}} = '{item.ttb_id}'"
            existing_records = self.table.all(formula=formula)

            if not existing_records:
                logger.warning(f"Record not found for update: {item.ttb_id}")
                return False

            record_id = existing_records[0]["id"]
            record_data = self._item_to_record(item)

            self.table.update(record_id, record_data)
            logger.debug(f"Updated record: {item.ttb_id}")

            # Fetch and upload document if enabled
            if self.fetch_documents and self.document_fetcher:
                self._fetch_and_upload_document(item.ttb_id, record_id)
                time.sleep(0.5)  # Small delay to avoid rate limiting

            return True

        except Exception as e:
            logger.error(f"Failed to update item {item.ttb_id}: {e}")
            return False

    def delete_all(self) -> int:
        """
        Delete all records from the Airtable table.
        WARNING: This is destructive and cannot be undone!

        Returns:
            Number of records deleted
        """
        logger.warning("Deleting ALL records from Airtable table...")

        try:
            records = self.table.all()
            record_ids = [record["id"] for record in records]

            if not record_ids:
                logger.info("No records to delete")
                return 0

            # Airtable batch delete accepts max 10 records at a time
            batch_size = 10
            deleted_count = 0

            for i in range(0, len(record_ids), batch_size):
                batch = record_ids[i:i + batch_size]
                self.table.batch_delete(batch)
                deleted_count += len(batch)
                logger.debug(f"Deleted batch {i // batch_size + 1}: {len(batch)} records")

            logger.success(f"Deleted {deleted_count} records")
            return deleted_count

        except Exception as e:
            logger.exception(f"Delete all failed: {e}")
            raise
