"""
COLA Document Fetcher - handles fetching PDF documents from TTB COLA detail pages using Playwright.

This module handles:
1. Connecting to Browserless via WSS endpoint
2. Detecting if a captcha is present
3. Solving captcha using 2Captcha service
4. Generating PDF using browser-native print functionality
"""
import os
import asyncio
from typing import Optional, Tuple
from loguru import logger
from playwright.async_api import async_playwright, Page, Browser, TimeoutError as PlaywrightTimeoutError
import httpx


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
        self.client = httpx.AsyncClient(timeout=180.0)  # 3 minute timeout for captcha solving
        logger.info("Initialized 2Captcha solver")

    async def solve_image_captcha(self, image_base64: str) -> Optional[str]:
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
            response = await self.client.post(
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
                await asyncio.sleep(2)  # Wait 2 seconds between checks

                response = await self.client.get(
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

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - cleanup httpx client."""
        try:
            await self.client.aclose()
        except:
            pass
        return False


class ColaDocumentFetcher:
    """Fetches COLA documents from TTB as PDFs using Playwright and Browserless."""

    def __init__(self, two_captcha_api_key: str, browserless_wss_endpoint: str = None):
        """
        Initialize the document fetcher.

        Args:
            two_captcha_api_key: 2Captcha API key
            browserless_wss_endpoint: Browserless WSS endpoint URL (or set BROWSERLESS_WSS_ENDPOINT env var)
        """
        self.captcha_solver = TwoCaptchaSolver(two_captcha_api_key)
        self.browserless_wss_endpoint = browserless_wss_endpoint or os.getenv("BROWSERLESS_WSS_ENDPOINT")

        if not self.browserless_wss_endpoint:
            raise ValueError("Browserless WSS endpoint not provided. Set BROWSERLESS_WSS_ENDPOINT environment variable or pass browserless_wss_endpoint parameter.")

        # Browser session management
        self._playwright = None
        self._browser = None
        self._context = None

        logger.info(f"Initialized COLA document fetcher with Browserless")

    async def connect_browser(self, max_retries: int = 5, initial_timeout: int = 60000):
        """
        Connect to Browserless browser with retry logic for serverless cold starts.

        Args:
            max_retries: Maximum number of connection attempts (default: 5)
            initial_timeout: Initial timeout in milliseconds (default: 60000ms = 60s)
        """
        if self._browser is not None:
            logger.debug("Browser already connected")
            return

        logger.info(f"Connecting to Browserless at {self.browserless_wss_endpoint}")

        # Use the helper method for connection with retry
        self._playwright, self._browser, self._context = await self._connect_to_browserless_with_retry(
            max_retries=max_retries,
            initial_timeout=initial_timeout
        )
        logger.success("Persistent browser session established and ready for reuse")

    async def disconnect_browser(self):
        """
        Disconnect from browser and cleanup resources. Call this after all documents are fetched.
        """
        if self._context:
            try:
                await self._context.close()
                logger.debug("Browser context closed")
            except Exception as e:
                logger.warning(f"Error closing context: {e}")
            self._context = None

        if self._browser:
            try:
                await self._browser.close()
                logger.debug("Browser connection closed")
            except Exception as e:
                logger.warning(f"Error closing browser: {e}")
            self._browser = None

        if self._playwright:
            try:
                await self._playwright.stop()
                logger.debug("Playwright stopped")
            except Exception as e:
                logger.warning(f"Error stopping Playwright: {e}")
            self._playwright = None

        logger.info("Browser session cleaned up")

    async def __aenter__(self):
        """Async context manager entry - connects browser."""
        await self.connect_browser()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - disconnects browser."""
        await self.disconnect_browser()
        return False

    async def _connect_to_browserless_with_retry(self, max_retries: int = 5, initial_timeout: int = 60000) -> Tuple:
        """
        Helper method to connect to Browserless with retry logic.

        Args:
            max_retries: Maximum number of connection attempts
            initial_timeout: Initial timeout in milliseconds

        Returns:
            Tuple of (playwright, browser, context)
        """
        last_error = None
        for attempt in range(1, max_retries + 1):
            playwright = None
            try:
                # Increase timeout with each retry attempt
                timeout = initial_timeout + (attempt - 1) * 30000
                logger.info(f"Browserless connection attempt {attempt}/{max_retries} (timeout: {timeout}ms)...")

                playwright = await async_playwright().start()
                browser = await playwright.chromium.connect_over_cdp(
                    self.browserless_wss_endpoint,
                    timeout=timeout
                )
                context = browser.contexts[0] if browser.contexts else await browser.new_context()
                logger.success(f"Browserless connection established on attempt {attempt}")
                return playwright, browser, context

            except Exception as e:
                last_error = e
                logger.warning(f"Browserless connection attempt {attempt}/{max_retries} failed: {e}")

                # Clean up any partial connections
                if playwright:
                    try:
                        await playwright.stop()
                    except:
                        pass

                if attempt < max_retries:
                    # Exponential backoff with cap
                    wait_time = min(2 ** attempt, 30)
                    logger.info(f"Waiting {wait_time}s before retry...")
                    await asyncio.sleep(wait_time)

        # All retries failed
        error_msg = f"Failed to connect to Browserless after {max_retries} attempts. Last error: {last_error}"
        logger.error(error_msg)
        raise ConnectionError(error_msg)

    async def _check_for_captcha_on_page(self, page: Page) -> Tuple[bool, Optional[str]]:
        """
        Check if the page contains a captcha.

        Args:
            page: Playwright Page object

        Returns:
            Tuple of (has_captcha, image_base64)
        """
        try:
            # Look for captcha text indicator: "What code is in the image?"
            captcha_text = page.locator("text=What code is in the image?").first

            if await captcha_text.is_visible(timeout=2000):
                logger.info("Captcha detected on page")

                # Find the captcha image with alt="bottle"
                img = page.locator('img[alt="bottle"]').first

                if await img.is_visible(timeout=1000):
                    image_data = await img.get_attribute('src')
                    if image_data and image_data.startswith('data:image'):
                        logger.debug(f"Found captcha image (length: {len(image_data)})")
                        return True, image_data

        except Exception as e:
            logger.debug(f"No captcha found: {e}")
            return False, None

        return False, None

    async def _is_document_page(self, page: Page) -> bool:
        """
        Check if the page is a COLA document page (not captcha).

        Args:
            page: Playwright Page object

        Returns:
            True if it's a document page
        """
        try:
            # Check if captcha is present - if so, not a document page
            captcha_text = page.locator("text=What code is in the image?").first
            if await captcha_text.is_visible(timeout=1000):
                logger.debug("Captcha detected, not a document page")
                return False

            # Primary check: Look for the COLA application form
            form = page.locator('form[name="colaApplicationForm"]').first
            if await form.is_visible(timeout=2000):
                logger.debug("Detected COLA document page (colaApplicationForm found)")
                return True

            # Secondary check: Look for TTB ID label in the document structure
            # The actual document has: <div class="label">TTB ID</div>
            ttb_id_label = page.locator('div.label:has-text("TTB ID")').first
            if await ttb_id_label.is_visible(timeout=1000):
                logger.debug("Detected document page with TTB ID label")
                return True

            # Tertiary check: Look for "PART I - APPLICATION" section header
            part_one_header = page.locator('div.sectionhead:has-text("PART I - APPLICATION")').first
            if await part_one_header.is_visible(timeout=1000):
                logger.debug("Detected document page with PART I header")
                return True

        except Exception as e:
            logger.debug(f"Not a document page: {e}")
            return False

        return False

    async def _handle_captcha(self, page: Page, max_retries: int = 3) -> bool:
        """
        Handle captcha if present on the page.

        Args:
            page: Playwright Page object
            max_retries: Maximum number of retry attempts

        Returns:
            True if captcha was solved and page is now document page, False otherwise
        """
        for attempt in range(max_retries):
            has_captcha, image_data = await self._check_for_captcha_on_page(page)

            if not has_captcha:
                logger.info("No captcha present")
                return await self._is_document_page(page)

            logger.info(f"Solving captcha (attempt {attempt + 1}/{max_retries})")

            # Solve the captcha
            captcha_solution = await self.captcha_solver.solve_image_captcha(image_data)

            if not captcha_solution:
                logger.error("Failed to solve captcha")
                continue

            try:
                # Find the answer input field with id="ans"
                answer_input = page.locator('input#ans').first

                # Find the submit button with id="jar"
                submit_button = page.locator('button#jar').first

                if await answer_input.is_visible(timeout=2000) and await submit_button.is_visible(timeout=2000):
                    logger.info(f"Submitting captcha solution: {captcha_solution}")

                    # Fill in the answer
                    await answer_input.fill(captcha_solution)

                    # Click submit and wait for navigation
                    await submit_button.click()
                    await page.wait_for_load_state('networkidle', timeout=15000)

                    # Wait a bit for the page to fully load after captcha
                    await page.wait_for_timeout(1000)

                    # Check if we got the document page now
                    if await self._is_document_page(page):
                        logger.success("Captcha solved! Got document page")
                        return True
                    else:
                        logger.warning("Captcha submitted but didn't get document page")
                else:
                    logger.error("Could not find captcha form elements (input#ans or button#jar)")

            except Exception as e:
                logger.error(f"Error submitting captcha: {e}")

            # Wait before retry
            if attempt < max_retries - 1:
                await asyncio.sleep(2)

        logger.error("Failed to solve captcha after all retries")
        return False

    async def fetch_document_pdf(self, ttb_id: str, max_retries: int = 3) -> Optional[bytes]:
        """
        Fetch the COLA document as a PDF, handling captcha if present.

        Uses persistent browser session if connected via connect_browser(),
        otherwise creates a temporary session.

        Args:
            ttb_id: TTB ID to fetch
            max_retries: Maximum number of retry attempts

        Returns:
            PDF content as bytes or None if failed
        """
        url = f"https://ttbonline.gov/colasonline/viewColaDetails.do?action=publicFormDisplay&ttbid={ttb_id}"

        logger.info(f"Fetching COLA document PDF for TTB ID: {ttb_id}")

        # Check if we have a persistent browser session
        use_persistent_session = self._browser is not None

        if not use_persistent_session:
            logger.debug("No persistent browser session, creating temporary session")

        try:
            # Use persistent session if available, otherwise create temporary one
            if use_persistent_session:
                context = self._context
                page = await context.new_page()
            else:
                # Temporary session for standalone usage with retry logic
                logger.debug("Creating temporary Browserless session with retry logic")
                playwright, browser, context = await self._connect_to_browserless_with_retry()
                page = await context.new_page()

            try:
                # Navigate to the URL
                logger.info(f"Navigating to {url}")
                await page.goto(url, wait_until='networkidle', timeout=30000)

                # Handle captcha if present
                if not await self._handle_captcha(page, max_retries):
                    logger.error("Failed to handle captcha or reach document page")
                    return None

                # Wait for all images to load (label images, signature, etc.)
                # The document includes images like:
                # - /colasonline/publicViewSignature.do?ttbid=...
                # - /colasonline/publicViewAttachment.do?filename=...
                logger.info("Waiting for all images to load...")
                await page.wait_for_load_state('networkidle', timeout=10000)
                await page.wait_for_timeout(2000)  # Additional wait for dynamic content

                # Generate PDF using browser-native print
                logger.info("Generating PDF...")
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

                logger.success(f"Successfully generated PDF for {ttb_id}: {len(pdf_bytes)} bytes")
                return pdf_bytes

            finally:
                # Clean up page (but not context/browser if using persistent session)
                await page.close()

                if not use_persistent_session:
                    # Clean up temporary session
                    await context.close()
                    await browser.close()
                    await playwright.stop()

        except Exception as e:
            logger.error(f"Error fetching document PDF for {ttb_id}: {e}")
            return None


# Example usage
if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv

    load_dotenv()

    two_captcha_key = os.getenv("TWO_CAPTCHA_API_KEY")
    browserless_wss = os.getenv("BROWSERLESS_WSS_ENDPOINT")

    if not two_captcha_key:
        logger.error("TWO_CAPTCHA_API_KEY not set in environment")
        sys.exit(1)

    if not browserless_wss:
        logger.error("BROWSERLESS_WSS_ENDPOINT not set in environment")
        sys.exit(1)

    async def main():
        fetcher = ColaDocumentFetcher(two_captcha_key, browserless_wss)

        # Test with an example TTB ID
        ttb_id = "23079001000657"
        pdf_bytes = await fetcher.fetch_document_pdf(ttb_id)

        if pdf_bytes:
            logger.success(f"PDF fetched successfully: {len(pdf_bytes)} bytes")
            # Optionally save for testing
            from pathlib import Path
            test_output = Path("./test_output.pdf")
            test_output.write_bytes(pdf_bytes)
            logger.info(f"Saved test PDF to {test_output}")
        else:
            logger.error("Failed to fetch PDF")

    asyncio.run(main())
