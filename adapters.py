"""
Adapters for syncing TTB data to external services.
"""
import os
import io
import asyncio
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
        two_captcha_api_key: str = None,
        browserless_wss_endpoint: str = None
    ):
        """
        Initialize Airtable adapter.

        Args:
            api_key: Airtable API key (or set AIRTABLE_API_KEY env var)
            base_id: Airtable Base ID (or set AIRTABLE_BASE_ID env var)
            table_name: Name of the table to sync to
            fetch_documents: Whether to automatically fetch and upload PDF documents
            two_captcha_api_key: 2Captcha API key (required if fetch_documents=True)
            browserless_wss_endpoint: Browserless WSS endpoint (or set BROWSERLESS_WSS_ENDPOINT env var)
        """
        self.api_key = api_key or os.getenv("AIRTABLE_API_KEY")
        self.base_id = base_id or os.getenv("AIRTABLE_BASE_ID")
        self.table_name = table_name

        self.fetch_documents = fetch_documents
        self.two_captcha_api_key = two_captcha_api_key or os.getenv("TWO_CAPTCHA_API_KEY")
        self.browserless_wss_endpoint = browserless_wss_endpoint or os.getenv("BROWSERLESS_WSS_ENDPOINT")

        if not self.api_key:
            raise ValueError("Airtable API key not provided. Set AIRTABLE_API_KEY environment variable or pass api_key parameter.")

        if not self.base_id:
            raise ValueError("Airtable Base ID not provided. Set AIRTABLE_BASE_ID environment variable or pass base_id parameter.")

        if self.fetch_documents and not self.two_captcha_api_key:
            raise ValueError("TWO_CAPTCHA_API_KEY required when fetch_documents=True")

        if self.fetch_documents and not self.browserless_wss_endpoint:
            raise ValueError("BROWSERLESS_WSS_ENDPOINT required when fetch_documents=True")

        self.api = Api(self.api_key)
        self.table = self.api.table(self.base_id, self.table_name)

        # Initialize document fetcher if enabled
        self.document_fetcher = None
        if self.fetch_documents:
            from cola_document_fetcher import ColaDocumentFetcher
            self.document_fetcher = ColaDocumentFetcher(
                self.two_captcha_api_key,
                self.browserless_wss_endpoint
            )

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

    def _has_cola_documents(self, record_id: str) -> tuple[bool, bool]:
        """
        Check if COLA and Latest COLA fields have existing attachments.

        Args:
            record_id: Airtable record ID

        Returns:
            Tuple of (has_cola, has_latest_cola)
        """
        try:
            record = self.table.get(record_id)
            fields = record.get("fields", {})

            has_cola = bool(fields.get("COLA"))
            has_latest_cola = bool(fields.get("Latest COLA"))

            return has_cola, has_latest_cola
        except Exception as e:
            logger.error(f"Failed to check existing documents for {record_id}: {e}")
            return False, False

    def _has_fields_changed(self, ttb_id: str, new_item: TTBItem) -> bool:
        """
        Check if any fields have changed between existing record and new data.

        Args:
            ttb_id: TTB ID to check
            new_item: New TTBItem data

        Returns:
            True if any fields changed, False otherwise
        """
        try:
            # Find the existing record
            formula = f"{{TTB ID}} = {ttb_id}"
            existing_records = self.table.all(formula=formula)

            if not existing_records:
                # Record doesn't exist, so it's a "change"
                return True

            existing_fields = existing_records[0]["fields"]
            new_record = self._item_to_record(new_item)

            # Compare all fields except Deprecated (which we control)
            # and COLA/Latest COLA (which are attachment fields)
            fields_to_check = [
                "Permit No", "Serial Number", "Completed Date", "Fanciful Name",
                "Brand Name", "Origin Code", "Origin Desc", "Class/Type",
                "Class/Type Desc", "Status", "Vendor Code", "Type of Application",
                "For Sale In", "Total Bottle Capacity", "Grape Varietals",
                "Wine Vintage", "Formula", "Lab No", "Approval Date",
                "Qualifications", "Applicant Name", "Applicant Address",
                "Applicant City", "Applicant State", "Applicant ZIP",
                "Contact Name", "Contact Phone", "Contact Email"
            ]

            for field in fields_to_check:
                old_value = existing_fields.get(field)
                new_value = new_record.get(field)

                # Handle None/empty string equivalence
                if (old_value or new_value) and old_value != new_value:
                    logger.debug(f"Field '{field}' changed for {ttb_id}: {old_value} -> {new_value}")
                    return True

            logger.debug(f"No field changes detected for {ttb_id}")
            return False

        except Exception as e:
            logger.error(f"Failed to check field changes for {ttb_id}: {e}")
            # On error, assume changed to be safe
            return True

    def _upload_pdf_to_fields(self, ttb_id: str, record_id: str, pdf_bytes: bytes, upload_to_both: bool = False) -> bool:
        """
        Upload PDF content to Airtable record attachment fields.

        Args:
            ttb_id: TTB ID (for logging and filename)
            record_id: Airtable record ID
            pdf_bytes: PDF content as bytes
            upload_to_both: If True, uploads to both COLA and Latest COLA fields
                           (COLA adds to existing, Latest COLA replaces)

        Returns:
            True if successful
        """
        try:
            filename = f"COLA_{ttb_id}.pdf"

            # Upload to COLA field (always - adds to existing attachments)
            self.table.upload_attachment(
                record_id,
                "COLA",  # Field name
                filename,
                pdf_bytes
            )
            logger.debug(f"Uploaded PDF to COLA field for {ttb_id}")

            # Upload to Latest COLA field if requested (replaces existing)
            if upload_to_both:
                # First, clear the Latest COLA field to replace (not add to) existing attachments
                self.table.update(record_id, {"Latest COLA": []})
                logger.debug(f"Cleared Latest COLA field for {ttb_id}")

                # Then upload the new PDF
                self.table.upload_attachment(
                    record_id,
                    "Latest COLA",  # Field name
                    filename,
                    pdf_bytes
                )
                logger.debug(f"Uploaded PDF to Latest COLA field for {ttb_id}")

            logger.success(f"Uploaded PDF to record {record_id} for TTB ID {ttb_id} (both fields: {upload_to_both})")
            return True

        except Exception as e:
            logger.error(f"Failed to upload PDF for {ttb_id}: {e}")
            return False

    async def _fetch_and_upload_document(self, ttb_id: str, record_id: str, item: TTBItem = None, is_new: bool = False) -> bool:
        """
        Fetch document and upload PDF to Airtable record with smart upload logic.

        Smart upload logic:
        - For new records: always upload to both COLA and Latest COLA fields
        - For existing records:
          - If no documents exist: upload to both fields
          - If documents exist and fields changed: replace Latest COLA, add to COLA
          - If documents exist and no changes: skip upload

        Args:
            ttb_id: TTB ID to fetch document for
            record_id: Airtable record ID to update
            item: TTBItem data (required for change detection on updates)
            is_new: True if this is a new record (always upload to both fields)

        Returns:
            True if successful
        """
        if not self.document_fetcher:
            logger.warning("Document fetcher not initialized")
            return False

        try:
            # For new records, always upload to both fields
            if is_new:
                logger.info(f"Fetching and uploading document for new record: {ttb_id}")

                pdf_bytes = await self.document_fetcher.fetch_document_pdf(ttb_id)
                if not pdf_bytes:
                    logger.error(f"Failed to fetch document for {ttb_id}")
                    return False

                success = self._upload_pdf_to_fields(ttb_id, record_id, pdf_bytes, upload_to_both=True)
                if success:
                    logger.success(f"Successfully uploaded PDF to both fields for new record {ttb_id}")
                return success

            # For existing records, check conditions
            has_cola, has_latest_cola = self._has_cola_documents(record_id)

            # If no documents exist, upload to both fields
            if not has_cola or not has_latest_cola:
                logger.info(f"No existing documents for {ttb_id}, uploading to both fields")

                pdf_bytes = await self.document_fetcher.fetch_document_pdf(ttb_id)
                if not pdf_bytes:
                    logger.error(f"Failed to fetch document for {ttb_id}")
                    return False

                success = self._upload_pdf_to_fields(ttb_id, record_id, pdf_bytes, upload_to_both=True)
                if success:
                    logger.success(f"Successfully uploaded PDF to both fields for {ttb_id}")
                return success

            # Documents exist - check if fields changed
            if item:
                fields_changed = self._has_fields_changed(ttb_id, item)

                if not fields_changed:
                    logger.info(f"No fields changed and documents exist for {ttb_id}, skipping PDF upload")
                    return True  # Not an error, just skipping

                # Fields changed - replace Latest COLA and add to COLA
                logger.info(f"Fields changed for {ttb_id}, updating documents")

                pdf_bytes = await self.document_fetcher.fetch_document_pdf(ttb_id)
                if not pdf_bytes:
                    logger.error(f"Failed to fetch document for {ttb_id}")
                    return False

                success = self._upload_pdf_to_fields(ttb_id, record_id, pdf_bytes, upload_to_both=True)
                if success:
                    logger.success(f"Successfully updated PDF for changed record {ttb_id}")
                return success
            else:
                # No item provided for comparison, upload to be safe
                logger.warning(f"No item data for change detection on {ttb_id}, uploading to both fields")

                pdf_bytes = await self.document_fetcher.fetch_document_pdf(ttb_id)
                if not pdf_bytes:
                    logger.error(f"Failed to fetch document for {ttb_id}")
                    return False

                success = self._upload_pdf_to_fields(ttb_id, record_id, pdf_bytes, upload_to_both=True)
                if success:
                    logger.success(f"Successfully uploaded PDF for {ttb_id}")
                return success

        except Exception as e:
            logger.error(f"Error fetching and uploading document for {ttb_id}: {e}")
            return False

    def create_items(self, items: List[TTBItem]) -> int:
        """
        Create new items in Airtable using batch operations.
        If fetch_documents is enabled, also fetches and uploads PDF documents for each item
        using a persistent browser session.

        Args:
            items: List of TTBItem models to create

        Returns:
            Number of items created
        """
        if not items:
            logger.info("No items to create")
            return 0

        # If async operations needed, run them in event loop
        if self.fetch_documents and self.document_fetcher:
            return asyncio.run(self._create_items_async(items))
        else:
            return self._create_items_sync(items)

    def _create_items_sync(self, items: List[TTBItem]) -> int:
        """Synchronous version for when documents are not being fetched."""
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

            logger.success(f"Successfully created {created_count} records in Airtable")
            return created_count

        except Exception as e:
            logger.exception(f"Failed to create items in Airtable: {e}")
            raise

    async def _create_items_async(self, items: List[TTBItem]) -> int:
        """Async version for when documents are being fetched."""
        logger.info(f"Creating {len(items)} new records in Airtable...")

        try:
            # Airtable batch API accepts max 10 records at a time
            batch_size = 10
            created_count = 0

            # Use async context manager to manage browser session lifecycle
            async with self.document_fetcher:  # Opens browser connection
                logger.info("Browser session established for batch PDF generation")

                for i in range(0, len(items), batch_size):
                    batch = items[i:i + batch_size]
                    records_data = [self._item_to_record(item) for item in batch]

                    created_records = self.table.batch_create(records_data)
                    created_count += len(batch)
                    logger.debug(f"Created batch {i // batch_size + 1}: {len(batch)} records")

                    # Fetch and upload documents using persistent browser session
                    for idx, item in enumerate(batch):
                        record_id = created_records[idx]['id']
                        await self._fetch_and_upload_document(item.ttb_id, record_id, item=item, is_new=True)
                        # Small delay to avoid rate limiting
                        await asyncio.sleep(0.5)
            # Browser session automatically closed here

            logger.success(f"Successfully created {created_count} records in Airtable")
            return created_count

        except Exception as e:
            logger.exception(f"Failed to create items in Airtable: {e}")
            raise

    def update_item(self, item: TTBItem) -> bool:
        """
        Update an existing item in Airtable.
        If fetch_documents is enabled, also fetches and uploads PDF document for the item.

        Args:
            item: TTBItem model to update

        Returns:
            True if successful, False otherwise
        """
        # If async operations needed, run them in event loop
        if self.fetch_documents and self.document_fetcher:
            return asyncio.run(self._update_item_async(item))
        else:
            return self._update_item_sync(item)

    def _update_item_sync(self, item: TTBItem) -> bool:
        """Synchronous version for when documents are not being fetched."""
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

            return True

        except Exception as e:
            logger.error(f"Failed to update item {item.ttb_id}: {e}")
            return False

    async def _update_item_async(self, item: TTBItem) -> bool:
        """Async version for when documents are being fetched."""
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

            # Fetch and upload document if enabled (using smart upload logic)
            await self._fetch_and_upload_document(item.ttb_id, record_id, item=item, is_new=False)
            await asyncio.sleep(0.5)  # Small delay to avoid rate limiting

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
