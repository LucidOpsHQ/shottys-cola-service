"""
TTB COLA scraper - handles web scraping logic only.
"""
import httpx
from bs4 import BeautifulSoup
from typing import List, Generator, Optional
from contextlib import contextmanager
import time
import re
from datetime import datetime, timedelta
from loguru import logger

from models import TTBItem


# Global httpx client with persistent cookies
_global_client: httpx.Client | None = None


@contextmanager
def get_http_client() -> Generator[httpx.Client, None, None]:
    """
    Context manager that yields a global httpx client with persistent cookies.

    The client is reused across all scraper runs to maintain session state,
    including cookies. This allows the scraper to maintain authentication
    and session data between function invocations.

    Yields:
        httpx.Client with persistent cookies and connection pooling
    """
    global _global_client

    if _global_client is None:
        logger.info("Creating new global httpx client with persistent cookies")
        _global_client = httpx.Client(
            follow_redirects=True,
            timeout=30.0,
            verify=False,
        )
    else:
        logger.debug(f"Reusing global httpx client (cookies: {len(_global_client.cookies)} items)")

    try:
        yield _global_client
    except Exception:
        # If there's an error, don't close the client - keep it for next run
        raise


class TTBScraper:
    """Scraper for TTB COLAs Online database."""

    def __init__(self, product_name: str = "Shottys", delay_between_requests: float = 1.0):
        """
        Initialize the scraper.

        Args:
            product_name: Product or fanciful name to search for
            delay_between_requests: Delay in seconds between requests (be respectful)
        """
        self.product_name = product_name
        self.delay_between_requests = delay_between_requests

        self.base_url = "https://ttbonline.gov/colasonline/publicSearchColasAdvancedProcess.do"
        self.pagination_url = "https://ttbonline.gov/colasonline/publicPageAdvancedCola.do"

        # Calculate date range: today and 15 years ago + 1 day
        date_to_dt = datetime.now()
        date_from_dt = date_to_dt - timedelta(days=15*365-1)
        self.date_to = date_to_dt.strftime("%m/%d/%Y")
        self.date_from = date_from_dt.strftime("%m/%d/%Y")

        logger.info(f"Initialized scraper for product: {product_name}")
        logger.debug(f"Date range: {self.date_from} to {self.date_to}")

    def _get_headers(self) -> dict:
        """Get HTTP headers for requests."""
        return {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://ttbonline.gov',
            'Referer': 'https://ttbonline.gov/colasonline/publicSearchColasAdvanced.do',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }

    def _get_search_data(self) -> dict:
        """Get POST data for initial search."""
        return {
            'searchCriteria.dateCompletedFrom': self.date_from,
            'searchCriteria.dateCompletedTo': self.date_to,
            'searchCriteria.productOrFancifulName': self.product_name,
            'searchCriteria.productNameSearchType': 'B',
            'searchCriteria.classTypeDesired': 'desc',
            'searchCriteria.classTypeCode': '',
            'searchCriteria.ttbIdFrom': '',
            'searchCriteria.ttbIdTo': '',
            'searchCriteria.serialNumFrom': '',
            'searchCriteria.serialNumTo': '',
            'searchCriteria.permitId': '',
            'searchCriteria.vendorCode': '',
            'action': 'search'
        }

    def _extract_items_from_page(self, soup: BeautifulSoup) -> List[TTBItem]:
        """
        Extract TTB items from parsed HTML.

        Args:
            soup: BeautifulSoup object of the page

        Returns:
            List of TTBItem models
        """
        results = []

        # Look for the results table - it has width="785"
        tables = soup.find_all('table', width="785")

        for table in tables:
            rows = table.find_all('tr')

            # Skip first row if it's a header (contains <th> tags)
            for row in rows[1:]:
                cells = row.find_all('td')

                # Expected columns: TTB ID, Permit No, Serial Number, Completed Date,
                # Fanciful Name, Brand Name, Origin Code, Origin Desc, Class/Type Code, Class/Type Desc
                if len(cells) >= 10:
                    try:
                        # Extract TTB ID from the link in first cell
                        ttb_id_link = cells[0].find('a')
                        if not ttb_id_link:
                            continue

                        ttb_id = ttb_id_link.get_text(strip=True)

                        # Extract the href to get the proper URL
                        href = ttb_id_link.get('href', '')
                        if href:
                            url = f"https://ttbonline.gov/colasonline/{href}"
                        else:
                            url = f"https://ttbonline.gov/colasonline/viewColaDetails.do?action=publicDisplaySearchAdvanced&ttbid={ttb_id}"

                        permit_no = cells[1].get_text(strip=True) or None
                        serial_number = cells[2].get_text(strip=True) or None
                        completed_date = cells[3].get_text(strip=True) or None
                        fanciful_name = cells[4].get_text(strip=True) or None
                        brand_name = cells[5].get_text(strip=True) or None
                        origin_code = cells[6].get_text(strip=True) or None
                        origin_desc = cells[7].get_text(strip=True) or None
                        class_type = cells[8].get_text(strip=True) or None
                        class_type_desc = cells[9].get_text(strip=True) or None

                        # Create TTBItem model (basic fields only from list page)
                        item = TTBItem(
                            ttb_id=ttb_id,
                            permit_no=permit_no,
                            serial_number=serial_number,
                            completed_date=completed_date,
                            fanciful_name=fanciful_name,
                            brand_name=brand_name,
                            origin_code=origin_code,
                            origin_desc=origin_desc,
                            class_type=class_type,
                            class_type_desc=class_type_desc,
                            url=url
                        )
                        results.append(item)
                        logger.debug(f"Parsed TTB item: {item.ttb_id} - {item.brand_name}")
                    except Exception as e:
                        logger.warning(f"Failed to parse row: {e}")
                        continue

        logger.debug(f"Extracted {len(results)} items from page")
        return results

    def _fetch_detail_page(self, ttb_id: str, url: str) -> Optional[str]:
        """
        Fetch detail page HTML for a specific TTB ID.

        Args:
            ttb_id: TTB ID
            url: Detail page URL

        Returns:
            HTML content or None if failed
        """
        try:
            with get_http_client() as client:
                response = client.get(url)

                if response.status_code == 200:
                    logger.debug(f"Fetched detail page for {ttb_id}")
                    return response.text
                else:
                    logger.warning(f"Failed to fetch detail page for {ttb_id}: Status {response.status_code}")
                    return None
        except Exception as e:
            logger.error(f"Error fetching detail page for {ttb_id}: {e}")
            return None

    def _parse_detail_page(self, ttb_id: str, html: str) -> dict:
        """
        Parse detail page HTML and extract additional fields.

        Args:
            ttb_id: TTB ID (for logging)
            html: HTML content

        Returns:
            Dictionary of additional fields
        """
        details = {}

        try:
            soup = BeautifulSoup(html, 'html.parser')

            # Helper function to extract field value after a strong tag
            def get_field_value(label_text: str) -> Optional[str]:
                # Find the strong tag containing the label
                strong_tags = soup.find_all('strong')
                for strong in strong_tags:
                    if label_text in strong.get_text():
                        # Get the parent td
                        td = strong.find_parent('td')
                        if td:
                            # Get all text in the td, excluding the strong tag text
                            text = td.get_text(strip=True)
                            # Remove the label from the text
                            text = text.replace(strong.get_text(strip=True), '').strip()
                            return text if text else None
                return None

            # Extract fields from the detail page
            details['status'] = get_field_value('Status:')
            details['vendor_code'] = get_field_value('Vendor Code:')
            details['type_of_application'] = get_field_value('Type of Application:')
            details['for_sale_in'] = get_field_value('For Sale In:')
            details['total_bottle_capacity'] = get_field_value('Total Bottle Capacity:')
            details['grape_varietals'] = get_field_value('Grape Varietal(s):')
            details['wine_vintage'] = get_field_value('Wine Vintage:')
            details['formula'] = get_field_value('Formula')  # Note: no colon in the HTML for this one
            details['approval_date'] = get_field_value('Approval Date:')

            # Extract qualifications (longer text field)
            qualifications_strong = soup.find('strong', string=lambda t: t and 'Qualifications:' in t)
            if qualifications_strong:
                # Find the containing tr, then the next tr which has the qualifications text
                tr = qualifications_strong.find_parent('tr')
                if tr:
                    next_tr = tr.find_next_sibling('tr')
                    if next_tr:
                        td = next_tr.find('td')
                        if td:
                            details['qualifications'] = td.get_text(strip=True)

            # Extract applicant information (from the second box)
            # Look for the permit number section which contains address info
            permit_strong = soup.find('strong', string=lambda t: t and 'Plant Registry/Basic Permit/Brewers No (Principal Place of Business)' in t)
            if permit_strong:
                # Find the parent table
                table = permit_strong.find_parent('table')
                if table:
                    rows = table.find_all('tr')
                    # Parse address lines (skip first 2 rows which have the header and blank)
                    address_lines = []
                    for row in rows[2:]:
                        td = row.find('td')
                        if td:
                            text = td.get_text(strip=True)
                            if text and text != '&nbsp;':
                                address_lines.append(text)

                    # Parse address_lines
                    # Format is typically:
                    # BWN-FL-21062
                    # The Point Distillery, LLC
                    # 11807 LITTLE RD
                    # New Port Richey, FL 34654
                    if len(address_lines) >= 2:
                        details['applicant_name'] = address_lines[1] if len(address_lines) > 1 else None
                        details['applicant_address'] = address_lines[2] if len(address_lines) > 2 else None

                        # Parse city, state, zip from last line
                        if len(address_lines) > 3:
                            location = address_lines[3]
                            # Format: "New Port Richey, FL 34654"
                            if ',' in location:
                                city_part, state_zip = location.rsplit(',', 1)
                                details['applicant_city'] = city_part.strip()

                                state_zip = state_zip.strip()
                                parts = state_zip.split()
                                if len(parts) >= 2:
                                    details['applicant_state'] = parts[0]
                                    details['applicant_zip'] = parts[1]

            # Extract contact information
            contact_strong = soup.find('strong', string='Contact Information:')
            if contact_strong:
                tr = contact_strong.find_parent('tr')
                if tr:
                    # Next row has the contact name
                    next_tr = tr.find_next_sibling('tr')
                    if next_tr:
                        details['contact_name'] = next_tr.get_text(strip=True)

                        # Next row has phone number
                        phone_tr = next_tr.find_next_sibling('tr')
                        if phone_tr:
                            phone_text = phone_tr.get_text(strip=True)
                            if 'Phone Number:' in phone_text:
                                details['contact_phone'] = phone_text.replace('Phone Number:', '').strip()

            logger.debug(f"Parsed {len([v for v in details.values() if v])} detail fields for {ttb_id}")

        except Exception as e:
            logger.error(f"Error parsing detail page for {ttb_id}: {e}")

        return details

    def _enrich_item_with_details(self, item: TTBItem) -> TTBItem:
        """
        Fetch and parse detail page to enrich TTBItem with additional fields.

        Args:
            item: TTBItem with basic fields from list page

        Returns:
            Enriched TTBItem with detail fields
        """
        logger.debug(f"Enriching item {item.ttb_id} with detail page data")

        # Fetch detail page HTML
        html = self._fetch_detail_page(item.ttb_id, item.url)

        if not html:
            logger.warning(f"Could not fetch detail page for {item.ttb_id}, returning basic item")
            return item

        # Parse detail page
        details = self._parse_detail_page(item.ttb_id, html)

        # Create enriched item
        item_dict = item.model_dump()
        item_dict.update(details)

        enriched_item = TTBItem(**item_dict)
        logger.debug(f"Enriched item {item.ttb_id} with detail data")

        return enriched_item

    def _has_next_page(self, soup: BeautifulSoup) -> bool:
        """
        Check if there's a next page available.

        Args:
            soup: BeautifulSoup object of the page

        Returns:
            True if there's a next page, False otherwise
        """
        pagination_divs = soup.find_all('div', class_='pagination')

        for div in pagination_divs:
            text = div.get_text()

            # Parse the pattern "X to Y of Z"
            match = re.search(r'(\d+)\s+to\s+(\d+)\s+of\s+(\d+)', text)
            if match:
                end_range = int(match.group(2))  # Y (e.g., 52)
                total = int(match.group(3))       # Z (e.g., 52)

                # If we've reached the total, no next page
                if end_range >= total:
                    return False
                else:
                    return True

            # Also check for "Next" link that is not just text
            next_link = div.find('a', string=re.compile(r'Next', re.IGNORECASE))
            if next_link and next_link.get('href') and next_link.get('href') != '#':
                return True

        return False

    def scrape(self) -> List[TTBItem]:
        """
        Scrape TTB IDs from the TTB COLAs online database with pagination.

        Returns:
            List of TTBItem models containing scraped TTB data
        """
        all_results = []
        page = 1

        logger.info(f"Starting scrape for product: {self.product_name}")

        with get_http_client() as client:
            # Set headers for this scrape session
            client.headers.update(self._get_headers())
            while True:
                logger.info(f"Fetching page {page}...")

                # Make the POST request for first page, GET for subsequent pages
                if page == 1:
                    response = client.post(self.base_url, data=self._get_search_data())
                    logger.debug(f"POST request to {self.base_url}")
                    logger.debug(f"Response cookies: {dict(response.cookies)}")
                else:
                    pagination_params = {
                        'action': 'page',
                        'pgfcn': 'nextset'
                    }
                    response = client.get(self.pagination_url, params=pagination_params)
                    logger.debug(f"GET request to {self.pagination_url} with params {pagination_params}")
                    logger.debug(f"Response cookies: {dict(response.cookies)}")

                if response.status_code != 200:
                    logger.error(f"Received status code {response.status_code}")
                    break

                # Parse the HTML
                soup = BeautifulSoup(response.text, 'html.parser')

                # Extract TTB IDs and related information
                page_results = self._extract_items_from_page(soup)

                if not page_results:
                    logger.warning("No results found on this page")
                    break

                # Check for duplicate results - if all results are duplicates, we've looped
                duplicates = 0
                new_items = []
                for item in page_results:
                    if any(existing.ttb_id == item.ttb_id for existing in all_results):
                        duplicates += 1
                        logger.debug(f"Duplicate TTB ID found: {item.ttb_id}")
                    else:
                        new_items.append(item)

                # Enrich new items with detail page data
                for item in new_items:
                    enriched_item = self._enrich_item_with_details(item)
                    all_results.append(enriched_item)
                    # Small delay between detail page fetches
                    time.sleep(self.delay_between_requests)

                logger.info(f"Found {len(page_results)} results on page {page} ({len(new_items)} new, {duplicates} duplicates)")

                # If all results are duplicates, we're looping - stop here
                if duplicates == len(page_results):
                    logger.warning("All results are duplicates - stopping pagination")
                    break

                # Check if there's a next page
                if not self._has_next_page(soup):
                    logger.info("Reached last page")
                    break

                page += 1
                logger.debug(f"Waiting {self.delay_between_requests}s before next request")
                time.sleep(self.delay_between_requests)

        logger.success(f"Scraping completed! Total results: {len(all_results)}")
        return all_results
