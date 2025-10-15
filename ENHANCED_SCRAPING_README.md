# Enhanced COLA Data Scraping

## Overview

The scraper has been enhanced to fetch **detailed information** from each COLA record's detail page, in addition to the basic fields from the search results list. This provides a much richer dataset with complete COLA application details.

## What Changed

### Before (Basic Scraping)
The scraper only collected data from the search results table:
- TTB ID
- Permit No
- Serial Number
- Completed Date
- Fanciful Name
- Brand Name
- Origin Code/Description
- Class/Type Code/Description
- URL

### After (Enhanced Scraping with Details)
The scraper now fetches **additional fields** from each record's detail page:

#### Application Details
- **Status** - COLA status (APPROVED, etc.)
- **Vendor Code** - Vendor identification code
- **Type of Application** - Type (LABEL APPROVAL, EXEMPTION, etc.)
- **For Sale In** - State restriction (if any)
- **Total Bottle Capacity** - Bottle capacity for distinctive bottles
- **Grape Varietals** - Wine grape varieties
- **Wine Vintage** - Vintage year
- **Formula** - TTB formula number
- **Lab No** - Lab identification number
- **Approval Date** - Date the COLA was approved
- **Qualifications** - Additional qualifications text

#### Applicant Information
- **Applicant Name** - Company/business name
- **Applicant Address** - Street address
- **Applicant City** - City
- **Applicant State** - State
- **Applicant ZIP** - ZIP code

#### Contact Information
- **Contact Name** - Contact person name
- **Contact Phone** - Phone number
- **Contact Email** - Email address

## How It Works

### Workflow

```
1. Fetch search results page (paginated list of COLAs)
   ↓
2. Extract basic information from each row in the table
   ↓
3. For each record:
   a. Fetch detail page: viewColaDetails.do?action=publicDisplaySearchAdvanced&ttbid={TTB_ID}
   b. Parse HTML to extract additional fields
   c. Merge basic + detail fields into enriched TTBItem
   ↓
4. Continue to next page and repeat
   ↓
5. Sync all enriched records to Airtable with complete data
```

### Code Structure

#### `models.py`
- Extended `TTBItem` model with 18 new optional fields
- All new fields are properly typed and documented
- Backward compatible (all new fields are Optional)

#### `scraper.py`
New methods added:
- `_fetch_detail_page()` - Fetches detail page HTML for a TTB ID
- `_parse_detail_page()` - Parses HTML and extracts all detail fields
- `_enrich_item_with_details()` - Combines basic + detail data
- Modified `scrape()` - Now enriches each item with detail page data

#### `adapters.py`
- Updated `_item_to_record()` to map all 18 new fields to Airtable
- All fields are synced automatically when records are created/updated

## Performance Considerations

### Additional Requests
The enhanced scraper makes **1 additional HTTP request per COLA record** to fetch the detail page.

**Example:**
- 50 COLA records = 50 additional detail page requests
- With 1 second delay: ~50 additional seconds total
- Total time: List page time + Detail page time

### Rate Limiting
- Uses same delay (`TTB_DELAY` env var) between detail page requests
- Reuses global httpx client for session persistence
- Respects TTB server rate limits

### Timing Estimates
| Records | Basic Scraping | Enhanced Scraping (1s delay) |
|---------|----------------|------------------------------|
| 10      | ~15 seconds    | ~25 seconds                  |
| 50      | ~1 minute      | ~1.5-2 minutes               |
| 100     | ~2 minutes     | ~4-5 minutes                 |

## Airtable Field Mapping

Make sure your Airtable table has these fields (create them if they don't exist):

### Required Fields (existing)
- TTB ID (Number)
- Permit No (Text)
- Serial Number (Text)
- Completed Date (Date)
- Fanciful Name (Text)
- Brand Name (Text)
- Origin Code (Text)
- Origin Desc (Text)
- Class/Type (Text)
- Class/Type Desc (Text)
- URL (URL)
- Deprecated (Checkbox)

### New Fields (from detail page)
- Status (Text)
- Vendor Code (Text)
- Type of Application (Text)
- For Sale In (Text)
- Total Bottle Capacity (Text)
- Grape Varietals (Text)
- Wine Vintage (Text)
- Formula (Text)
- Lab No (Text)
- Approval Date (Text)
- Qualifications (Long Text)
- Applicant Name (Text)
- Applicant Address (Text)
- Applicant City (Text)
- Applicant State (Text)
- Applicant ZIP (Text)
- Contact Name (Text)
- Contact Phone (Text)
- Contact Email (Email)

**Note:** The scraper will still work if fields don't exist in Airtable, but those fields will be silently ignored. For best results, create all fields.

## Usage

### No Configuration Changes Required!

The enhanced scraping is **enabled by default**. Just run:

```bash
python main.py
```

### Disabling Detail Scraping (if needed)

If you want to go back to basic scraping (faster, less data), you can modify `scraper.py`:

```python
# In the scrape() method, comment out the enrichment section:
# for item in new_items:
#     enriched_item = self._enrich_item_with_details(item)
#     all_results.append(enriched_item)
#     time.sleep(self.delay_between_requests)

# And replace with:
for item in new_items:
    all_results.append(item)
```

## Error Handling

The scraper is resilient to detail page fetch failures:

- **If detail page fails to load**: Basic fields are still saved
- **If parsing fails**: Error is logged, basic fields are used
- **If field is missing**: Field value is `None` (not an error)

This ensures that even if TTB's detail page structure changes or has issues, the core scraping functionality continues to work.

## Example Data

### Before (Basic)
```json
{
  "ttb_id": "21227001000056",
  "brand_name": "SHOTTYS",
  "fanciful_name": "SPICED APPLE CIDER FLAVOR",
  "serial_number": "21S015",
  "completed_date": "08/24/2021"
}
```

### After (Enhanced)
```json
{
  "ttb_id": "21227001000056",
  "brand_name": "SHOTTYS",
  "fanciful_name": "SPICED APPLE CIDER FLAVOR",
  "serial_number": "21S015",
  "completed_date": "08/24/2021",
  "status": "APPROVED",
  "vendor_code": "43024",
  "type_of_application": "LABEL APPROVAL",
  "formula": "1407005",
  "approval_date": "08/24/2021",
  "applicant_name": "The Point Distillery, LLC",
  "applicant_address": "11807 LITTLE RD",
  "applicant_city": "New Port Richey",
  "applicant_state": "FL",
  "applicant_zip": "34654",
  "contact_name": "Jacob Polukoff",
  "contact_phone": "(727) 269-5588",
  "qualifications": "TTB has not reviewed this label..."
}
```

## Logging

Enhanced scraping produces additional log output:

```
2025-10-15 10:00:01 | INFO     | Found 10 results on page 1 (10 new, 0 duplicates)
2025-10-15 10:00:01 | DEBUG    | Enriching item 21227001000056 with detail page data
2025-10-15 10:00:02 | DEBUG    | Fetched detail page for 21227001000056
2025-10-15 10:00:02 | DEBUG    | Parsed 15 detail fields for 21227001000056
2025-10-15 10:00:02 | DEBUG    | Enriched item 21227001000056 with detail data
```

## Testing

To test the enhanced scraper with a single record:

```python
from scraper import TTBScraper

scraper = TTBScraper(product_names=["Shottys"])

# Create a basic item
from models import TTBItem
basic_item = TTBItem(
    ttb_id="21227001000056",
    url="https://ttbonline.gov/colasonline/viewColaDetails.do?action=publicDisplaySearchAdvanced&ttbid=21227001000056",
    brand_name="SHOTTYS"
)

# Enrich it
enriched = scraper._enrich_item_with_details(basic_item)

print(f"Status: {enriched.status}")
print(f"Applicant: {enriched.applicant_name}")
print(f"Contact: {enriched.contact_name}")
```

## Benefits

### For Data Analysis
- Complete COLA application information in one place
- Can filter/search by approval date, vendor, applicant, etc.
- Better understanding of product details (formula, varietals, etc.)

### For Business Intelligence
- Track which companies/applicants are most active
- Analyze approval patterns and timelines
- Monitor specific states or regions
- Contact information for outreach

### For Compliance
- Full qualifications and restrictions visible
- Easy to verify approval status
- Complete audit trail with all COLA details

## Future Enhancements

Potential improvements:
- Parallel detail page fetching (faster)
- Cache detail pages to avoid re-fetching
- Extract label images from detail pages
- Parse qualifications into structured fields
- Add support for "Other Permits" from detail page
