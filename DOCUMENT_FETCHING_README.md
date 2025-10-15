# COLA Document Fetching & PDF Upload

This document explains the new document fetching functionality that automatically downloads COLA documents from TTB and uploads them as PDFs to Airtable.

## Overview

The system now supports automatic fetching and uploading of COLA documents (TTB Form 5100.31) as PDF files directly to your Airtable records. When enabled, the system will:

1. **Fetch the COLA document page** from TTB for each record
2. **Handle captchas automatically** using 2Captcha service
3. **Convert HTML to PDF** using Playwright (headless browser)
4. **Upload PDF directly to Airtable** attachment field named "COLA"

## Architecture

### Key Components

- **`cola_document_fetcher.py`**: Handles document fetching, captcha solving, and PDF generation
  - `TwoCaptchaSolver`: Integrates with 2Captcha API for automatic captcha solving
  - `ColaDocumentFetcher`: Main class that orchestrates the workflow
  - Uses the global httpx client from `scraper.py` for session management

- **`adapters.py`**: Extended with PDF upload functionality
  - `_fetch_and_upload_document()`: Fetches and uploads PDFs for individual records
  - `_upload_pdf_to_record()`: Uploads PDF bytes to Airtable attachment field
  - Integrated into `create_items()` and `update_item()` methods

### Workflow

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Scrape COLA records from TTB                            │
│    (existing functionality)                                  │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Create/Update records in Airtable                        │
│    (existing functionality)                                  │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. For each record (if FETCH_DOCUMENTS=true):              │
│    a. Fetch document page from TTB                          │
│    b. Detect if captcha is present                          │
│    c. If captcha: solve using 2Captcha → submit solution    │
│    d. Convert HTML to PDF using Playwright                  │
│    e. Upload PDF bytes to Airtable "COLA" field            │
└─────────────────────────────────────────────────────────────┘
```

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
playwright install chromium  # Install browser for PDF generation
```

### 2. Get 2Captcha API Key

1. Sign up at https://2captcha.com/
2. Add funds to your account (captcha solving costs ~$1-3 per 1000 captchas)
3. Get your API key from the dashboard

### 3. Configure Environment Variables

Edit your `.env` file:

```bash
# Enable document fetching
FETCH_DOCUMENTS=true

# Add your 2Captcha API key
TWO_CAPTCHA_API_KEY=your_api_key_here
```

### 4. Ensure Airtable Field Exists

Make sure your Airtable table has an **Attachment** field named **"COLA"**. This is where the PDF documents will be uploaded.

## Usage

### Run with Document Fetching

```bash
# Make sure FETCH_DOCUMENTS=true in .env
python main.py
```

### Run without Document Fetching

```bash
# Set FETCH_DOCUMENTS=false in .env (or remove it)
python main.py
```

### Test Document Fetcher Standalone

```bash
# Test with a specific TTB ID
python cola_document_fetcher.py
```

This will fetch the example document (TTB ID: 23079001000657) and save it as `test_output.pdf`.

## Configuration Options

| Environment Variable | Required | Default | Description |
|---------------------|----------|---------|-------------|
| `FETCH_DOCUMENTS` | No | `false` | Enable/disable automatic document fetching |
| `TWO_CAPTCHA_API_KEY` | Yes (if fetching) | - | Your 2Captcha API key |
| `AIRTABLE_API_KEY` | Yes | - | Airtable API key |
| `AIRTABLE_BASE_ID` | Yes | - | Airtable base ID |
| `AIRTABLE_TABLE_NAME` | No | `TTB COLAs` | Airtable table name |

## How Captcha Handling Works

### Example Scenarios

#### Scenario 1: No Captcha (Direct Access)
```
Request → Document Page (200 OK) → Convert to PDF → Upload to Airtable
```

#### Scenario 2: Captcha Present
```
Request → Captcha Page
  → Extract captcha image (base64)
  → Send to 2Captcha API
  → Wait for solution (30-60 seconds)
  → Submit solution
  → Get Document Page
  → Convert to PDF
  → Upload to Airtable
```

### Captcha Detection

The system detects captchas by looking for:
- Text: "What code is in the image?" or "testing whether you are a human"
- Image element with captcha
- Input field for captcha answer

### Captcha Submission

Once solved, the system submits the captcha solution back to TTB and retrieves the actual document page.

## PDF Generation

PDFs are generated using **Playwright** (headless Chromium browser) with the following settings:

- Format: Letter (8.5" x 11")
- Margins: 0.5" on all sides
- Print background: Yes (includes background colors/images)
- Output: Direct to bytes (no filesystem storage)

## Cost Considerations

### 2Captcha Pricing
- Image captchas: ~$1-3 per 1000 captchas
- Average solve time: 10-60 seconds
- Success rate: ~95%

### Rate Limiting
The system includes automatic delays:
- 0.5 second delay between document fetches
- Respects TTB's rate limits
- Uses persistent HTTP session (cookies maintained)

## Troubleshooting

### "Failed to solve captcha"
- Check 2Captcha account balance
- Verify API key is correct
- Check 2Captcha service status

### "Failed to fetch document page"
- TTB website may be down or blocking requests
- Try increasing retry count
- Check network connectivity

### "Failed to upload PDF"
- Verify Airtable "COLA" field exists and is type "Attachment"
- Check Airtable API key permissions
- Ensure sufficient Airtable storage space

### "Playwright browser not found"
Run: `playwright install chromium`

## Technical Details

### Session Management
- Reuses global httpx client from `scraper.py`
- Maintains cookies across requests
- Shares session with TTB scraper

### PDF Storage
- **No local filesystem storage** - PDFs are generated in memory
- PDFs are uploaded directly to Airtable as bytes
- Filename format: `COLA_{ttb_id}.pdf`

### Error Handling
- Graceful degradation: If document fetch fails, the record is still created
- Detailed logging for debugging
- Retry logic for transient failures

## Example Output

```
2025-10-15 10:00:00 | INFO     | Creating 10 new records in Airtable...
2025-10-15 10:00:01 | INFO     | Created batch 1: 10 records
2025-10-15 10:00:01 | INFO     | Fetching and uploading document for TTB ID: 23079001000657
2025-10-15 10:00:02 | INFO     | Fetching COLA document for TTB ID: 23079001000657
2025-10-15 10:00:03 | INFO     | Captcha detected (attempt 1/3)
2025-10-15 10:00:03 | INFO     | Submitting captcha to 2Captcha...
2025-10-15 10:00:03 | INFO     | Captcha submitted, ID: 12345678
2025-10-15 10:00:35 | SUCCESS  | Captcha solved: ABC123
2025-10-15 10:00:35 | INFO     | Submitting captcha solution: ABC123
2025-10-15 10:00:36 | SUCCESS  | Captcha solved! Got document page for 23079001000657
2025-10-15 10:00:36 | SUCCESS  | Successfully fetched document page for 23079001000657
2025-10-15 10:00:36 | INFO     | Converting HTML to PDF bytes for 23079001000657
2025-10-15 10:00:38 | SUCCESS  | PDF generated: 245678 bytes
2025-10-15 10:00:38 | SUCCESS  | Successfully generated PDF for 23079001000657: 245678 bytes
2025-10-15 10:00:39 | SUCCESS  | Uploaded PDF to record rec123456 for TTB ID 23079001000657
2025-10-15 10:00:39 | SUCCESS  | Successfully uploaded PDF for 23079001000657
```

## Security Notes

- 2Captcha API key should be kept secret
- Never commit `.env` file to version control
- Use `.env.example` as template only
- PDFs are transmitted securely to Airtable via HTTPS

## Performance

### Timing Estimates (per record)
- No captcha: ~3-5 seconds
- With captcha: ~40-70 seconds (depends on 2Captcha queue)
- PDF generation: ~2-3 seconds
- Airtable upload: ~1-2 seconds

### Batch Processing
- 10 records without captchas: ~5 minutes
- 10 records with captchas: ~10-15 minutes

## Future Enhancements

Potential improvements:
- Parallel document fetching
- Caching of already-fetched documents
- Alternative captcha services
- PDF quality/size optimization
- Automatic retry on captcha failure
