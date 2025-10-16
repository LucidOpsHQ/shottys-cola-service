"""
COLA Document Fetcher - handles fetching PDF documents from TTB COLA detail pages.

This module handles:
1. Detecting if a captcha is present
2. Solving captcha using 2Captcha service
3. Fetching the document page
4. Converting the HTML page to PDF using playwright
"""
import os
import httpx
import base64
import time
import asyncio
from bs4 import BeautifulSoup
from pathlib import Path
from typing import Optional, Tuple
from loguru import logger
from playwright.async_api import Page, Browser
import playwright_aws_lambda

# Import the global httpx client context manager
from scraper import get_http_client


class TwoCaptchaSolver:
    """Handler for 2Captcha API integration."""

    def __init__(self, api_key: str):
        """
        Initialize 2Captcha solver.

        Args:
            api_key: 2Captcha API key
        """
        self.api_key = api_key
        self.base_url = "http://2captcha.com"
        # Use a separate client for 2Captcha API calls (not the TTB session)
        self.client = httpx.Client(timeout=180.0)  # 3 minute timeout for captcha solving
        logger.info("Initialized 2Captcha solver")

    def solve_image_captcha(self, image_base64: str) -> Optional[str]:
        """
        Solve an image captcha using 2Captcha.

        Args:
            image_base64: Base64 encoded image data (with or without data:image prefix)

        Returns:
            Solved captcha text or None if failed
        """
        # Remove data URI prefix if present
        if image_base64.startswith('data:image'):
            image_base64 = image_base64.split(',', 1)[1]

        logger.info("Submitting captcha to 2Captcha...")

        # Submit captcha
        try:
            response = self.client.post(
                f"{self.base_url}/in.php",
                data={
                    'key': self.api_key,
                    'method': 'base64',
                    'body': image_base64,
                    'json': 1
                }
            )
            result = response.json()

            if result.get('status') != 1:
                error_msg = result.get('request', 'Unknown error')
                logger.error(f"Failed to submit captcha: {error_msg}")
                return None

            captcha_id = result.get('request')
            logger.info(f"Captcha submitted, ID: {captcha_id}")

            # Poll for result
            max_attempts = 60  # 2 minutes max
            for attempt in range(max_attempts):
                time.sleep(2)  # Wait 2 seconds between checks

                response = self.client.get(
                    f"{self.base_url}/res.php",
                    params={
                        'key': self.api_key,
                        'action': 'get',
                        'id': captcha_id,
                        'json': 1
                    }
                )
                result = response.json()

                if result.get('status') == 1:
                    captcha_text = result.get('request')
                    logger.success(f"Captcha solved: {captcha_text}")
                    return captcha_text
                elif result.get('request') == 'CAPCHA_NOT_READY':
                    logger.debug(f"Captcha not ready yet (attempt {attempt + 1}/{max_attempts})")
                    continue
                else:
                    error_msg = result.get('request', 'Unknown error')
                    logger.error(f"Failed to get captcha result: {error_msg}")
                    return None

            logger.error("Captcha solving timed out")
            return None

        except Exception as e:
            logger.error(f"Error solving captcha: {e}")
            return None

    def __del__(self):
        """Cleanup httpx client."""
        try:
            self.client.close()
        except:
            pass


class ColaDocumentFetcher:
    """Fetches COLA documents from TTB, handling captcha and generating PDFs."""

    def __init__(self, two_captcha_api_key: str):
        """
        Initialize the document fetcher.

        Args:
            two_captcha_api_key: 2Captcha API key
            output_dir: Directory to save PDFs
        """
        self.captcha_solver = TwoCaptchaSolver(two_captcha_api_key)

        logger.info(f"Initialized COLA document fetcher")

    def _get_headers(self) -> dict:
        """Get HTTP headers for requests."""
        return {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
        }

    def _check_for_captcha(self, html: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Check if the HTML contains a captcha.

        Args:
            html: HTML content to check

        Returns:
            Tuple of (has_captcha, image_base64, support_id)
        """
        soup = BeautifulSoup(html, 'html.parser')

        # Look for captcha indicators from example-captha.html
        captcha_text = soup.find(string=lambda text: text and "What code is in the image?" in text)
        if not captcha_text:
            captcha_text = soup.find(string=lambda text: text and "testing whether you are a human" in text)

        if captcha_text:
            logger.info("Captcha detected on page")

            # Find the captcha image
            img = soup.find('img', alt='bottle')
            if not img:
                img = soup.find('img', src=lambda src: src and src.startswith('data:image'))

            if img and img.get('src'):
                image_data = img.get('src')
                logger.debug(f"Found captcha image (length: {len(image_data)})")

                # Find support ID if present
                support_id = None
                support_text = soup.find(string=lambda text: text and "Your support ID is:" in text)
                if support_text:
                    # Extract ID from text like "Your support ID is: 185792478901906347."
                    import re
                    match = re.search(r'support ID is:\s*(\d+)', support_text)
                    if match:
                        support_id = match.group(1)
                        logger.debug(f"Found support ID: {support_id}")

                return True, image_data, support_id

        return False, None, None

    def _is_document_page(self, html: str) -> bool:
        """
        Check if the HTML is a COLA document page (not captcha).

        Args:
            html: HTML content to check

        Returns:
            True if it's a document page
        """
        soup = BeautifulSoup(html, 'html.parser')

        # Check for document page indicators from example-cola-doc.html
        # Look for the form with COLA details
        form = soup.find('form', {'name': 'colaApplicationForm'})
        if form:
            logger.debug("Detected COLA document page")
            return True

        # Also check for TTB ID in the page
        ttb_id_div = soup.find('div', class_='data', string=lambda text: text and text.strip().isdigit() and len(text.strip()) == 14)
        if ttb_id_div:
            logger.debug("Detected TTB ID on page - likely a document page")
            return True

        return False

    def fetch_document_page(self, ttb_id: str, max_retries: int = 3) -> Optional[str]:
        """
        Fetch the COLA document page HTML, handling captcha if present.

        Args:
            ttb_id: TTB ID to fetch
            max_retries: Maximum number of retry attempts

        Returns:
            HTML content of the document page or None if failed
        """
        url = f"https://ttbonline.gov/colasonline/viewColaDetails.do?action=publicFormDisplay&ttbid={ttb_id}"

        logger.info(f"Fetching COLA document for TTB ID: {ttb_id}")

        # Use the global httpx client from scraper.py
        with get_http_client() as client:
            for attempt in range(max_retries):
                try:
                    # Fetch the page
                    response = client.get(url, headers=self._get_headers())

                    if response.status_code != 200:
                        logger.error(f"Received status code {response.status_code}")
                        continue

                    html = response.text

                    # Check if it's a document page (success!)
                    if self._is_document_page(html):
                        logger.success(f"Successfully fetched document page for {ttb_id}")
                        return html

                    # Check if there's a captcha
                    has_captcha, image_data, support_id = self._check_for_captcha(html)

                    if has_captcha:
                        logger.info(f"Captcha detected (attempt {attempt + 1}/{max_retries})")

                        # Solve the captcha
                        captcha_solution = self.captcha_solver.solve_image_captcha(image_data)

                        if not captcha_solution:
                            logger.error("Failed to solve captcha")
                            continue

                        # Submit the captcha solution
                        # Note: The exact submission mechanism depends on the actual captcha form
                        # This is a placeholder - you'll need to inspect the actual form submission
                        soup = BeautifulSoup(html, 'html.parser')

                        # Find the form and submit button
                        answer_input = soup.find('input', {'id': 'ans'}) or soup.find('input', {'name': 'answer'})
                        submit_button = soup.find('button', {'id': 'jar'})

                        if answer_input and submit_button:
                            # Submit the captcha
                            # The actual URL and method may vary - this is based on example-captha.html
                            logger.info(f"Submitting captcha solution: {captcha_solution}")

                            # Try to post back to the same URL or find the form action
                            submit_response = client.post(
                                url,
                                data={'answer': captcha_solution},
                                headers=self._get_headers()
                            )

                            if submit_response.status_code == 200:
                                html = submit_response.text

                                # Check if we got the document page now
                                if self._is_document_page(html):
                                    logger.success(f"Captcha solved! Got document page for {ttb_id}")
                                    return html
                                else:
                                    logger.warning("Captcha submitted but didn't get document page")
                            else:
                                logger.error(f"Captcha submission failed with status {submit_response.status_code}")
                        else:
                            logger.error("Could not find captcha form elements")
                    else:
                        logger.warning(f"Page is neither document nor captcha (attempt {attempt + 1}/{max_retries})")

                except Exception as e:
                    logger.error(f"Error fetching document (attempt {attempt + 1}/{max_retries}): {e}")

                # Wait before retry
                if attempt < max_retries - 1:
                    time.sleep(2)

            logger.error(f"Failed to fetch document for {ttb_id} after {max_retries} attempts")
            return None

    async def _convert_html_to_pdf_bytes_async(
        self,
        html: str,
        browser: Browser
    ) -> Optional[bytes]:
        """
        Convert HTML to PDF bytes using playwright (async version).

        Args:
            html: HTML content
            browser: Playwright browser instance

        Returns:
            PDF content as bytes or None if failed
        """
        try:
            # Create a new page
            page = await browser.new_page()

            # Set the HTML content
            await page.set_content(html, wait_until='networkidle')

            # Generate PDF with print settings (returns bytes)
            pdf_bytes = await page.pdf(
                format='Letter',
                print_background=True,
                margin={
                    'top': '0.5in',
                    'right': '0.5in',
                    'bottom': '0.5in',
                    'left': '0.5in'
                }
            )

            await page.close()

            logger.success(f"PDF generated: {len(pdf_bytes)} bytes")
            return pdf_bytes

        except Exception as e:
            logger.error(f"Error converting HTML to PDF: {e}")
            return None

    def convert_html_to_pdf_bytes(self, html: str, ttb_id: str) -> Optional[bytes]:
        """
        Convert HTML document to PDF bytes using playwright.

        Args:
            html: HTML content
            ttb_id: TTB ID (for logging)

        Returns:
            PDF content as bytes or None if failed
        """
        logger.info(f"Converting HTML to PDF bytes for {ttb_id}")

        async def run_conversion():
            # Launch browser using playwright-aws-lambda (optimized for AWS Lambda/Vercel)
            browser = await playwright_aws_lambda.launch()
            pdf_bytes = await self._convert_html_to_pdf_bytes_async(html, browser)
            await browser.close()
            return pdf_bytes

        # Check if there's already a running event loop
        try:
            loop = asyncio.get_running_loop()
            # If we're here, there's already a loop running
            # We need to use a different approach - run in a thread
            import nest_asyncio
            nest_asyncio.apply()
            pdf_bytes = asyncio.run(run_conversion())
        except RuntimeError:
            # No running loop, safe to use asyncio.run()
            pdf_bytes = asyncio.run(run_conversion())

        return pdf_bytes

    def fetch_and_generate_pdf_bytes(self, ttb_id: str) -> Optional[bytes]:
        """
        Complete workflow: fetch document page and generate PDF as bytes.

        Args:
            ttb_id: TTB ID to fetch and convert

        Returns:
            PDF content as bytes or None if failed
        """
        # Fetch the document page HTML
        html = self.fetch_document_page(ttb_id)

        if not html:
            logger.error(f"Failed to fetch document page for {ttb_id}")
            return None

        # Convert to PDF bytes
        pdf_bytes = self.convert_html_to_pdf_bytes(html, ttb_id)

        if pdf_bytes:
            logger.success(f"Successfully generated PDF for {ttb_id}: {len(pdf_bytes)} bytes")
        else:
            logger.error(f"Failed to generate PDF for {ttb_id}")

        return pdf_bytes


# Example usage
if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv

    load_dotenv()

    api_key = os.getenv("TWO_CAPTCHA_API_KEY")
    if not api_key:
        logger.error("TWO_CAPTCHA_API_KEY not set in environment")
        sys.exit(1)

    fetcher = ColaDocumentFetcher(api_key)

    # Test with the example TTB ID from example-cola-doc.html
    ttb_id = "23079001000657"
    pdf_bytes = fetcher.fetch_and_generate_pdf_bytes(ttb_id)

    if pdf_bytes:
        logger.success(f"PDF generated successfully: {len(pdf_bytes)} bytes")
        # Optionally save for testing
        test_output = Path("./test_output.pdf")
        test_output.write_bytes(pdf_bytes)
        logger.info(f"Saved test PDF to {test_output}")
    else:
        logger.error("Failed to generate PDF")
