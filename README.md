# TTB COLA Scraper & Sync Service

A modular Python service for scraping TTB (Alcohol and Tobacco Tax and Trade Bureau) COLA (Certificate of Label Approval) data and syncing it to Airtable.

## Architecture

The service follows a clean, modular architecture with clear separation of concerns:

```
┌─────────────┐
│   main.py   │  ← Entrypoint
└──────┬──────┘
       │
       ├──────────────┬──────────────┐
       │              │              │
       ▼              ▼              ▼
┌────────────┐  ┌──────────┐  ┌──────────┐
│ scraper.py │  │adapters.py│  │sync_     │
│            │  │           │  │strategy  │
│ TTBScraper │  │ Airtable  │  │          │
│            │  │ Adapter   │  │ Strategies│
└────────────┘  └──────────┘  └──────────┘
       │              │              │
       └──────────────┴──────────────┘
                      │
                      ▼
              ┌──────────────┐
              │  models.py   │
              │              │
              │  TTBItem     │
              └──────────────┘
```

### Components

#### **models.py**
- Contains the `TTBItem` Pydantic model
- Defines the data structure for TTB COLA records
- Fields: TTB ID, Permit No, Serial Number, Completed Date, Names, Origin, Classification, URL

#### **scraper.py**
- `TTBScraper` class handles web scraping logic
- Implements the `DataSource` protocol
- Fetches data from TTB's public COLA registry
- Handles pagination, session management, and duplicate detection
- Returns structured `TTBItem` objects

#### **adapters.py**
- Abstract `StorageAdapter` base class defines storage interface
- `AirtableAdapter` implements Airtable integration
- Follows the Adapter pattern for storage abstraction
- Methods: `get_existing_ids()`, `create_items()`, `update_item()`, `delete_all()`
- Handles batch operations for efficiency

#### **sync_strategy.py**
- Coordinates data flow between source and storage
- Three strategies available:
  - **IncrementalSyncStrategy**: Only create new records (fastest, recommended)
  - **FullSyncStrategy**: Update existing + create new records
  - **ReplaceSyncStrategy**: Delete all + recreate (destructive)

#### **main.py**
- Application entrypoint
- Configures logging with loguru
- Reads environment configuration
- Instantiates scraper, adapter, and strategy
- Orchestrates the sync process

## Installation

1. **Clone the repository and navigate to the project:**
   ```bash
   cd cola-service
   ```

2. **Create and activate a virtual environment:**
   ```bash
   python -m venv venv
   venv\Scripts\activate  # Windows
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables:**
   ```bash
   copy .env.example .env
   # Edit .env with your credentials
   ```

## Configuration

Create a `.env` file with the following variables:

```bash
# Airtable Configuration (required for sync)
AIRTABLE_API_KEY=your_api_key_here
AIRTABLE_BASE_ID=your_base_id_here
AIRTABLE_TABLE_NAME=TTB COLAs

# TTB Scraper Configuration
TTB_PRODUCT_NAMES=["Shottys"]
TTB_DELAY=1.0

# Sync Strategy
SYNC_STRATEGY=incremental  # Options: incremental, full, replace
```

### Airtable Setup

Create a table in Airtable with these fields:
- **TTB ID** (Single line text) - Primary key
- **Permit No** (Single line text)
- **Serial Number** (Single line text)
- **Completed Date** (Single line text or Date)
- **Fanciful Name** (Single line text)
- **Brand Name** (Single line text)
- **Origin Code** (Single line text)
- **Origin Desc** (Single line text)
- **Class/Type** (Single line text)
- **Class/Type Desc** (Single line text)
- **URL** (URL field)

## Usage

### Run with Airtable sync:
```bash
python main.py
```

### Run scraper only (no Airtable):
```bash
# Don't set AIRTABLE_API_KEY and AIRTABLE_BASE_ID
python main.py
# Saves results to ttb_results.json
```

### Change sync strategy:
```bash
# Incremental (default) - only new records
set SYNC_STRATEGY=incremental
python main.py

# Full - update existing + create new
set SYNC_STRATEGY=full
python main.py

# Replace - DELETE ALL and recreate (requires confirmation)
set SYNC_STRATEGY=replace
set CONFIRM_REPLACE=true
python main.py
```

## Logging

The service uses **loguru** for structured logging:
- **Console**: Colored INFO-level logs to stderr
- **File**: DEBUG-level logs saved to `scraper_YYYY-MM-DD.log`
- **Rotation**: Daily log rotation with 30-day retention

## Design Patterns

### Strategy Pattern
Three sync strategies implement different synchronization behaviors:
- `IncrementalSyncStrategy`: Efficient for regular updates
- `FullSyncStrategy`: Complete synchronization with updates
- `ReplaceSyncStrategy`: Fresh start (destructive)

### Adapter Pattern
`AirtableAdapter` adapts the TTB data to Airtable's API, making it easy to add support for other storage backends (PostgreSQL, MongoDB, etc.) by implementing the `StorageAdapter` interface.

### Protocol Pattern
Uses Python protocols (`DataSource`, `StorageTarget`) for type-safe dependency injection and loose coupling between components.

## Extending the Service

### Add a New Storage Backend

1. Create a new adapter in `adapters.py`:
```python
class PostgresAdapter(StorageAdapter):
    def get_existing_ids(self) -> Set[str]:
        # Implementation
        pass

    def create_items(self, items: List[TTBItem]) -> int:
        # Implementation
        pass

    def update_item(self, item: TTBItem) -> bool:
        # Implementation
        pass
```

2. Use it in `main.py`:
```python
storage = PostgresAdapter(connection_string=os.getenv("DATABASE_URL"))
strategy = IncrementalSyncStrategy(scraper, storage)
```

### Add a New Data Source

1. Create a scraper that implements the `DataSource` protocol:
```python
class AlternativeDataSource:
    def scrape(self) -> List[TTBItem]:
        # Implementation
        pass
```

2. Use it with existing strategies:
```python
source = AlternativeDataSource()
strategy = IncrementalSyncStrategy(source, airtable)
```

## Development

### Project Structure
```
cola-service/
├── main.py                 # Entrypoint
├── models.py               # Data models
├── scraper.py              # Web scraping logic
├── adapters.py             # Storage adapters
├── sync_strategy.py        # Sync coordination
├── requirements.txt        # Dependencies
├── .env.example           # Configuration template
├── README.md              # Documentation
└── scraper_*.log          # Log files
```

### Dependencies
- **httpx**: HTTP client for web scraping
- **beautifulsoup4**: HTML parsing
- **pydantic**: Data validation and modeling
- **loguru**: Structured logging
- **pyairtable**: Airtable API client

## Railway Deployment

This service is configured for deployment on Railway with automatic cron job scheduling.

### Setup

1. **Install Railway CLI:**
   ```bash
   npm install -g @railway/cli
   ```

2. **Login to Railway:**
   ```bash
   railway login
   ```

3. **Configure Environment Variables:**

   In your Railway project settings, add the following environment variables:

   ```
   AIRTABLE_API_KEY=your_api_key
   AIRTABLE_BASE_ID=your_base_id
   AIRTABLE_TABLE_NAME=TTB COLAs
   TTB_PRODUCT_NAMES=["Shottys"]
   TTB_DELAY=1.0
   SYNC_STRATEGY=incremental
   ```

   Or use Railway CLI:
   ```bash
   railway variables set AIRTABLE_API_KEY=your_api_key
   railway variables set AIRTABLE_BASE_ID=your_base_id
   ```

4. **Deploy:**
   ```bash
   railway up
   ```

### Cron Configuration

The cron job is configured in `railway.toml`:
- **Schedule**: `0 0 * * *` (Daily at midnight UTC)
- **Endpoint**: `/api/cron`
- **Strategy**: Incremental sync (only new records)

To change the schedule, edit the `[cron]` section in `railway.toml`:
```toml
[cron]
schedule = "0 */6 * * *"  # Every 6 hours
```

**Cron Schedule Examples:**
- `0 0 * * *` - Daily at midnight
- `0 */6 * * *` - Every 6 hours
- `0 9 * * 1` - Every Monday at 9 AM
- `0 0 1 * *` - First day of every month

### Manual Trigger

You can manually trigger the sync by visiting:
```
https://your-project.up.railway.app/api/cron
```

Or using curl:
```bash
curl https://your-project.up.railway.app/api/cron
```

### Local Testing

Test the serverless function locally:
```bash
python api/cron.py
# Visit http://localhost:8000/api/cron
```

### Monitoring

- **Logs**: View logs in Railway Dashboard → Your Project → Deployments
- **Metrics**: Check in Railway Dashboard → Your Project → Metrics
- **Response Format**:
  ```json
  {
    "status": "success",
    "message": "TTB COLA sync completed",
    "stats": {
      "total": 52,
      "new": 3,
      "skipped": 49
    },
    "timestamp": 1234567890.123
  }
  ```

### Railway Limits

- **Free Plan**:
  - 500 hours per month
  - 512MB RAM
  - 1GB disk

- **Pro Plan**:
  - Unlimited hours
  - Configurable resources
  - Priority support

If scraping takes longer than expected, consider:
1. Optimizing the scraper (reduce delay, fewer pages)
2. Upgrading to Pro plan for more resources
3. Splitting into multiple smaller jobs

## License

MIT License
