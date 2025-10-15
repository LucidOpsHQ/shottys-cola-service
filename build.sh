#!/bin/bash
# Install Python dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium

# Install dependencies for Playwright (required for headless browsers)
playwright install-deps chromium
