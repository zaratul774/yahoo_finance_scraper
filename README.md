# Yahoo Finance News Scraper

A Python web scraper that collects financial news headlines from Yahoo Finance using Playwright. Results are stored in a local SQLite database with automatic deduplication.

## Features

- Scrapes news headlines, sources, URLs, and ISO 8601 timestamps for any stock ticker
- Headless browser scraping with Playwright (handles JavaScript-rendered content)
- **RSS fallback** — automatically switches to Yahoo Finance's RSS feed if the browser scrape fails, making the scraper resilient against anti-scraper measures
- SQLite storage with deduplication — re-running never creates duplicate entries
- **CLI interface** — pass tickers, query stored results, and check stats from the terminal without editing source code
- Proper `logging` module — structured log output instead of raw print statements
- Test suite with 20+ unit tests covering all core logic

## Tech Stack

- **Playwright** — headless browser automation
- **httpx** — HTTP client for RSS fallback
- **SQLite3** — lightweight local database (no setup required)
- **asyncio** — async scraping for clean, non-blocking code
- **pytest** — unit tests with mocked network calls

## Installation

**1. Clone the repo**
```bash
git clone https://github.com/zaratul774/yahoo-finance-scraper
cd yahoo-finance-scraper
```

**2. Create a virtual environment**
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
```

**3. Install dependencies**
```bash
pip install -r requirements.txt
playwright install chromium
```

## Usage

**Scrape news for one or more tickers**
```bash
python scraper.py --tickers AAPL TSLA MSFT
```

**Limit articles per ticker**
```bash
python scraper.py --tickers NVDA --max 50
```

**Query stored articles for a ticker**
```bash
python scraper.py --query AAPL --limit 10
```

**Check database statistics**
```bash
python scraper.py --stats
```

**Use as a module in your own script**
```python
from scraper import init_db, get_latest, get_all_tickers, get_stats

conn = init_db()

articles = get_latest(conn, "AAPL", limit=10)
for a in articles:
    print(a["headline"], "|", a["source"], "|", a["published"])

print(get_all_tickers(conn))
print(get_stats(conn))

conn.close()
```

## Running Tests

```bash
pytest tests/ -v
```

## Database Schema

```
articles
├── id          INTEGER  Primary key
├── hash        TEXT     MD5 hash for deduplication (ticker + headline)
├── ticker      TEXT     Stock ticker symbol e.g. AAPL
├── headline    TEXT     Article headline
├── source      TEXT     Publisher name e.g. Reuters
├── url         TEXT     Full article URL
├── published   TEXT     ISO 8601 timestamp
└── scraped_at  TEXT     UTC timestamp of when the record was inserted
```

## Project Structure

```
yahoo-finance-scraper/
├── scraper.py           # Scraper, database logic, CLI, and query helpers
├── requirements.txt     # Dependencies
├── tests/
│   └── test_scraper.py  # Unit tests (no live network calls)
├── data/
│   └── finance_news.db  # SQLite database (auto-created on first run)
└── README.md
```

## How the Fallback Works

Yahoo Finance actively fights scrapers. If the Playwright browser scrape returns no results, the scraper automatically falls back to Yahoo Finance's RSS feed which is a structured public data endpoint. This means the scraper keeps working even when Yahoo updates their page layout.

## License

MIT