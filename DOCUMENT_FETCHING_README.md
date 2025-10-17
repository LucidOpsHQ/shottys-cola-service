# COLA Document Fetching & PDF Upload

This document explains the document fetching functionality that automatically downloads COLA documents from TTB and uploads them as PDF files to Airtable.

## Overview

The system now supports automatic fetching and uploading of COLA documents (TTB Form 5100.31) as PDF files directly to your Airtable records. When enabled, the system will:

1. **Connect to Browserless** via WebSocket (WSS) for remote browser automation
2. **Fetch the COLA document page** from TTB using Playwright
3. **Handle captchas automatically** using 2Captcha service
4. **Generate PDF** using browser-native print functionality
5. **Upload PDF directly to Airtable** attachment field named "COLA"

## Architecture

### Key Components

- **`cola_document_fetcher.py`**: Handles document fetching using Playwright
  - `TwoCaptchaSolver`: Integrates with 2Captcha API for automatic captcha solving
  - `ColaDocumentFetcher`: Main class that orchestrates the workflow
  - Uses Playwright to connect to Browserless via WSS endpoint
  - Generates PDFs using browser-native `page.pdf()` method

- **`adapters.py`**: Extended with PDF upload functionality
  - `_fetch_and_upload_document()`: Fetches and uploads PDF documents for individual records
  - `_upload_pdf_to_record()`: Uploads PDF content to Airtable attachment field
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
│    a. Connect to Browserless browser via WSS                │
│    b. Navigate to TTB document page with Playwright         │
│    c. Detect if captcha is present                          │
│    d. If captcha: solve using 2Captcha → submit solution    │
│    e. Wait for all images to load (label, signature, etc.)  │
│    f. Generate PDF using browser-native print               │
│    g. Upload PDF bytes to Airtable "COLA" field            │
└─────────────────────────────────────────────────────────────┘
```

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Get 2Captcha API Key

1. Sign up at https://2captcha.com/
2. Add funds to your account (captcha solving costs ~$1-3 per 1000 captchas)
3. Get your API key from the dashboard

### 3. Get Browserless Account

1. Sign up at https://www.browserless.io/
2. Choose a plan (free tier available)
3. Get your WebSocket endpoint with token
   - Format: `wss://chrome.browserless.io?token=YOUR_TOKEN`

### 4. Configure Environment Variables

Edit your `.env` file:

```bash
# Enable document fetching
FETCH_DOCUMENTS=true

# Add your 2Captcha API key
TWO_CAPTCHA_API_KEY=your_api_key_here

# Add your Browserless WSS endpoint
BROWSERLESS_WSS_ENDPOINT=wss://chrome.browserless.io?token=YOUR_TOKEN
```

### 5. Ensure Airtable Field Exists

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
| `BROWSERLESS_WSS_ENDPOINT` | Yes (if fetching) | - | Browserless WebSocket endpoint with token |
| `AIRTABLE_API_KEY` | Yes | - | Airtable API key |
| `AIRTABLE_BASE_ID` | Yes | - | Airtable base ID |
| `AIRTABLE_TABLE_NAME` | No | `TTB COLAs` | Airtable table name |

## How It Works

### Browser Automation with Playwright

The system uses Playwright to automate a real Chrome browser running on Browserless:

1. **Connect via WSS**: Establishes WebSocket connection to remote browser
2. **Navigate**: Opens TTB COLA document page
3. **Detect Elements**: Uses Playwright locators to find captcha or document content
4. **Generate PDF**: Uses `page.pdf()` for high-quality, browser-native PDF generation

### Captcha Handling

#### Example Scenarios

##### Scenario 1: No Captcha (Direct Access)
```
Connect → Navigate → Document Page → Generate PDF → Upload to Airtable
```

##### Scenario 2: Captcha Present
```
Connect → Navigate → Captcha Page
  → Extract captcha image (base64 from img[alt="bottle"])
  → Send to 2Captcha API
  → Wait for solution (30-60 seconds)
  → Fill input#ans with solution
  → Click button#jar
  → Wait for navigation
  → Get Document Page
  → Generate PDF
  → Upload to Airtable
```

### Captcha Detection

The system detects captchas by looking for:
- **Text**: "What code is in the image?" (exact match from `example-captcha.html`)
- **Image element**: `<img alt="bottle">` containing base64 captcha
- **Input field**: `<input id="ans">` for captcha answer
- **Submit button**: `<button id="jar">` to submit solution

### Document Page Detection

The system verifies document pages by checking for:
- **Primary**: `<form name="colaApplicationForm">` (main document form)
- **Secondary**: `<div class="label">TTB ID</div>` (TTB ID field)
- **Tertiary**: `<div class="sectionhead">PART I - APPLICATION</div>` (section header)

### PDF Generation

Benefits of browser-native PDF generation:
- **High Quality**: True browser rendering with all styles
- **Complete Content**: Includes all images (labels, signatures, etc.)
- **No Dependencies**: No need for external PDF libraries
- **Consistent**: Same rendering as viewing in browser

PDF settings:
- **Format**: Letter (8.5" x 11")
- **Margins**: 0.5 inches on all sides
- **Background**: Enabled (includes all background colors/images)

## PDF Storage

PDF documents are generated and uploaded directly:

- **No local filesystem storage** - PDF is kept in memory as bytes
- PDF bytes are uploaded directly to Airtable
- Filename format: `COLA_{ttb_id}.pdf`
- Typical size: 100-500 KB per document

## Cost Considerations

### 2Captcha Pricing
- Image captchas: ~$1-3 per 1000 captchas
- Average solve time: 10-60 seconds
- Success rate: ~95%

### Browserless Pricing
- **Free Tier**: 6 hours/month
- **Starter**: $29/month - 60 hours
- **Professional**: $99/month - 240 hours
- **Enterprise**: Custom pricing

### Rate Limiting
The system includes automatic delays:
- 0.5 second delay between document fetches
- Respects TTB's rate limits
- Each browser session is properly cleaned up

## Troubleshooting

### "Failed to solve captcha"
- Check 2Captcha account balance
- Verify API key is correct
- Check 2Captcha service status

### "Failed to connect to Browserless"
- Verify WSS endpoint is correct (includes token)
- Check Browserless account status and usage limits
- Ensure WebSocket connections are not blocked by firewall

### "Failed to fetch document page"
- TTB website may be down or blocking requests
- Try increasing retry count in code
- Check Browserless browser logs

### "Could not find captcha form elements"
- Captcha page structure may have changed
- Check example-captcha.html against actual captcha page
- Update selectors if needed

### "Failed to upload PDF"
- Verify Airtable "COLA" field exists and is type "Attachment"
- Check Airtable API key permissions
- Ensure sufficient Airtable storage space

## Technical Details

### Browser Automation
- Uses Playwright's Chrome DevTools Protocol (CDP) over WebSocket
- Browser runs remotely on Browserless infrastructure
- Proper cleanup: closes page, context, and browser connection

### Image Loading
The system explicitly waits for all images to load before generating PDF:
- Label images: `/colasonline/publicViewAttachment.do?filename=...`
- Signature images: `/colasonline/publicViewSignature.do?ttbid=...`
- Wait strategy: `networkidle` + 2-second buffer

### Error Handling
- Graceful degradation: If document fetch fails, the record is still created
- Detailed logging for debugging
- Retry logic for transient failures (configurable, default 3 retries)
- All errors sent to Sentry for monitoring

## Example Output

```
2025-10-17 10:00:00 | INFO     | Creating 10 new records in Airtable...
2025-10-17 10:00:01 | INFO     | Created batch 1: 10 records
2025-10-17 10:00:01 | INFO     | Fetching and uploading document for TTB ID: 23079001000657
2025-10-17 10:00:02 | INFO     | Fetching COLA document PDF for TTB ID: 23079001000657
2025-10-17 10:00:02 | INFO     | Connecting to Browserless at wss://chrome.browserless.io?token=...
2025-10-17 10:00:03 | INFO     | Navigating to https://ttbonline.gov/colasonline/viewColaDetails.do?...
2025-10-17 10:00:04 | INFO     | Captcha detected on page
2025-10-17 10:00:04 | INFO     | Solving captcha (attempt 1/3)
2025-10-17 10:00:04 | INFO     | Submitting captcha to 2Captcha...
2025-10-17 10:00:04 | INFO     | Captcha submitted, ID: 12345678
2025-10-17 10:00:35 | SUCCESS  | Captcha solved: ABC123
2025-10-17 10:00:35 | INFO     | Submitting captcha solution: ABC123
2025-10-17 10:00:36 | SUCCESS  | Captcha solved! Got document page
2025-10-17 10:00:36 | INFO     | Waiting for all images to load...
2025-10-17 10:00:38 | INFO     | Generating PDF...
2025-10-17 10:00:39 | SUCCESS  | Successfully generated PDF for 23079001000657: 245678 bytes
2025-10-17 10:00:40 | SUCCESS  | Uploaded PDF to record rec123456 for TTB ID 23079001000657
2025-10-17 10:00:40 | SUCCESS  | Successfully uploaded PDF for 23079001000657
```

## Security Notes

- 2Captcha API key should be kept secret
- Browserless WSS endpoint contains your token - keep it secure
- Never commit `.env` file to version control
- Use `.env.example` as template only
- PDF documents are transmitted securely to Airtable via HTTPS
- Browserless connections are encrypted via WSS (WebSocket Secure)

## Performance

### Timing Estimates (per record)
- No captcha: ~5-8 seconds (browser startup + navigation + PDF generation)
- With captcha: ~40-70 seconds (includes 30-60 second 2Captcha solve time)
- Airtable upload: ~1-2 seconds

### Batch Processing
- 10 records without captchas: ~5-8 minutes
- 10 records with captchas: ~10-15 minutes

### Optimization Tips
- Use incremental sync to avoid re-fetching existing documents
- Consider running during off-peak hours
- Monitor Browserless usage to stay within limits

## Railway Deployment Notes

When deploying to Railway with document fetching enabled:

1. The async job architecture handles long-running PDF generation
2. Railway cron receives immediate HTTP 202 response
3. Background job continues fetching and uploading PDFs
4. All progress logged to Railway logs and Sentry
5. No timeout issues since endpoint returns immediately

## Future Enhancements

Potential improvements:
- Parallel document fetching with multiple browser sessions
- Caching of already-fetched documents
- Alternative captcha services as fallback
- Configurable PDF quality/size settings
- Support for batch PDF generation
- Webhook notifications when batch completes
