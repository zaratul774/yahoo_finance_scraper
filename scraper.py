"""
Yahoo Finance News Scraper
Scrapes financial news headlines, sources, and timestamps using Playwright.
Stores results in a SQLite database with deduplication.

Falls back to Yahoo Finance RSS feed if the main page scrape returns no results,
making the scraper more resilient against anti-scraper measures.

Usage:
    python scraper.py --tickers AAPL TSLA MSFT
    python scraper.py --tickers NVDA --max 50
    python scraper.py --query AAPL --limit 10
    python scraper.py --stats
"""

import asyncio
import sqlite3
import hashlib
import logging
import argparse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx
from playwright.async_api import async_playwright


# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Database ──────────────────────────────────────────────────────────────────

DB_PATH = Path(__file__).parent / "data" / "finance_news.db"


def init_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Create the database and articles table if they don't exist."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            hash        TEXT    UNIQUE NOT NULL,
            ticker      TEXT    NOT NULL,
            headline    TEXT    NOT NULL,
            source      TEXT,
            url         TEXT,
            published   TEXT,
            scraped_at  TEXT    NOT NULL
        )
    """)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_ticker    ON articles(ticker)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_published ON articles(published)")
    conn.commit()
    return conn


def make_hash(ticker: str, headline: str) -> str:
    """Generate a unique MD5 hash for deduplication."""
    return hashlib.md5(f"{ticker}:{headline}".encode()).hexdigest()


def save_articles(conn: sqlite3.Connection, ticker: str, articles: list[dict]) -> int:
    """
    Insert articles into the database, skipping duplicates.
    Returns the number of new articles inserted.
    """
    inserted = 0
    now = datetime.now(timezone.utc).isoformat()

    for article in articles:
        h = make_hash(ticker, article["headline"])
        try:
            conn.execute("""
                INSERT INTO articles (hash, ticker, headline, source, url, published, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                h,
                ticker,
                article["headline"],
                article.get("source"),
                article.get("url"),
                article.get("published"),
                now,
            ))
            inserted += 1
        except sqlite3.IntegrityError:
            pass  # Duplicate — skip silently

    conn.commit()
    return inserted


# ── RSS fallback ──────────────────────────────────────────────────────────────

def scrape_rss(ticker: str, max_articles: int = 30) -> list[dict]:
    """
    Fallback: fetch news from Yahoo Finance RSS feed.
    Returns parsed articles with proper ISO 8601 timestamps.

    This is more reliable than browser scraping since RSS is a
    structured data feed that Yahoo Finance exposes publicly.
    """
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
    articles = []

    try:
        logger.info(f"[{ticker}] Trying RSS fallback ...")
        response = httpx.get(url, timeout=15, follow_redirects=True)
        response.raise_for_status()

        root = ET.fromstring(response.text)
        channel = root.find("channel")
        if channel is None:
            return []

        for item in channel.findall("item")[:max_articles]:
            headline = item.findtext("title", "").strip()
            if not headline:
                continue

            # Parse RFC 2822 date to ISO 8601
            pub_raw = item.findtext("pubDate", "")
            try:
                published = parsedate_to_datetime(pub_raw).isoformat()
            except Exception:
                published = None

            articles.append({
                "headline": headline,
                "source":   item.findtext("source", "Yahoo Finance"),
                "url":      item.findtext("link", ""),
                "published": published,
            })

        logger.info(f"[{ticker}] RSS returned {len(articles)} articles.")

    except Exception as e:
        logger.error(f"[{ticker}] RSS fallback failed: {e}")

    return articles


# ── Playwright scraper ────────────────────────────────────────────────────────

async def scrape_playwright(ticker: str, max_articles: int = 30) -> list[dict]:
    """
    Scrape Yahoo Finance news page using a headless Playwright browser.
    Returns a list of article dicts.
    """
    url = f"https://finance.yahoo.com/quote/{ticker}/news/"
    articles = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()

        logger.info(f"[{ticker}] Navigating to {url} ...")
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)

        # Dismiss cookie consent if present
        try:
            await page.click('button:has-text("Accept")', timeout=3000)
        except Exception:
            pass

        # Wait for news items
        try:
            await page.wait_for_selector("li.stream-item", timeout=15000)
        except Exception:
            logger.warning(f"[{ticker}] No stream items found — page may have changed.")
            await browser.close()
            return []

        # Scroll to load more
        for _ in range(3):
            await page.evaluate("window.scrollBy(0, 800)")
            await page.wait_for_timeout(1000)

        items = await page.query_selector_all("li.stream-item")
        logger.info(f"[{ticker}] Found {len(items)} raw items on page.")

        for item in items[:max_articles]:
            try:
                headline_el = await item.query_selector("h3")
                headline = await headline_el.inner_text() if headline_el else None
                if not headline:
                    continue

                link_el = await item.query_selector("a")
                href = await link_el.get_attribute("href") if link_el else None
                if href and href.startswith("/"):
                    href = f"https://finance.yahoo.com{href}"

                # FIX: Only extract source name, drop the relative time string
                # ("2 hours ago") entirely — we store our own UTC timestamp instead
                # which is actually useful for analysis.
                source_el = await item.query_selector("div.publishing")
                source_text = await source_el.inner_text() if source_el else ""
                parts = source_text.split("·")
                source = parts[0].strip() if parts else None

                # Store UTC time of scrape as the published timestamp
                published = datetime.now(timezone.utc).isoformat()

                articles.append({
                    "headline": headline.strip(),
                    "source":   source,
                    "url":      href,
                    "published": published,
                })

            except Exception as e:
                logger.warning(f"[{ticker}] Could not parse article: {e}")
                continue

        await browser.close()

    return articles


# ── Main scrape logic (with fallback) ─────────────────────────────────────────

async def scrape_ticker(ticker: str, max_articles: int = 30) -> list[dict]:
    """
    Scrape news for a ticker. Tries Playwright first, falls back to RSS
    if the browser scrape returns no results.
    """
    articles = await scrape_playwright(ticker, max_articles)

    if not articles:
        logger.warning(f"[{ticker}] Playwright returned nothing — using RSS fallback.")
        articles = scrape_rss(ticker, max_articles)

    return articles


# ── Query helpers ─────────────────────────────────────────────────────────────

def get_latest(conn: sqlite3.Connection, ticker: str, limit: int = 10) -> list[dict]:
    """Fetch the most recent articles for a ticker."""
    rows = conn.execute("""
        SELECT headline, source, url, published, scraped_at
        FROM   articles
        WHERE  ticker = ?
        ORDER  BY scraped_at DESC
        LIMIT  ?
    """, (ticker.upper(), limit)).fetchall()
    return [dict(r) for r in rows]


def get_all_tickers(conn: sqlite3.Connection) -> list[str]:
    """Return all tickers that have been scraped."""
    rows = conn.execute(
        "SELECT DISTINCT ticker FROM articles ORDER BY ticker"
    ).fetchall()
    return [r["ticker"] for r in rows]


def get_stats(conn: sqlite3.Connection) -> dict:
    """Return summary statistics for the database."""
    total   = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    tickers = conn.execute("SELECT COUNT(DISTINCT ticker) FROM articles").fetchone()[0]
    return {"total_articles": total, "tickers_tracked": tickers}


# ── CLI ───────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Yahoo Finance News Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scraper.py --tickers AAPL TSLA MSFT
  python scraper.py --tickers NVDA --max 50
  python scraper.py --query AAPL --limit 10
  python scraper.py --stats
        """
    )

    parser.add_argument(
        "--tickers", nargs="+", metavar="TICKER",
        help="One or more stock tickers to scrape e.g. AAPL TSLA"
    )
    parser.add_argument(
        "--max", type=int, default=30, metavar="N",
        help="Max articles to scrape per ticker (default: 30)"
    )
    parser.add_argument(
        "--query", metavar="TICKER",
        help="Print stored articles for a ticker"
    )
    parser.add_argument(
        "--limit", type=int, default=10, metavar="N",
        help="Number of articles to show with --query (default: 10)"
    )
    parser.add_argument(
        "--stats", action="store_true",
        help="Print database statistics"
    )

    return parser


async def main():
    parser = build_parser()
    args = parser.parse_args()

    conn = init_db()

    if args.stats:
        stats = get_stats(conn)
        print(f"\nTotal articles : {stats['total_articles']}")
        print(f"Tickers tracked: {stats['tickers_tracked']}")
        tickers = get_all_tickers(conn)
        if tickers:
            print(f"Tickers        : {', '.join(tickers)}")

    elif args.query:
        ticker = args.query.upper()
        articles = get_latest(conn, ticker, args.limit)
        if not articles:
            print(f"No articles found for {ticker}.")
        else:
            print(f"\n--- Latest {len(articles)} articles for {ticker} ---\n")
            for a in articles:
                # FIX: display source and scraped_at only — no messy relative
                # time string mixed in
                print(f"  {a['headline']}")
                print(f"  {a['source']} | scraped: {a['scraped_at']}")
                print(f"  {a['url']}\n")

    elif args.tickers:
        for ticker in args.tickers:
            ticker = ticker.upper()
            logger.info(f"Scraping {ticker} ...")
            try:
                articles = await scrape_ticker(ticker, args.max)
                inserted = save_articles(conn, ticker, articles)
                logger.info(
                    f"[{ticker}] {len(articles)} fetched — {inserted} new saved to DB."
                )
            except Exception as e:
                logger.error(f"[{ticker}] Failed: {e}")

        stats = get_stats(conn)
        logger.info(
            f"Done. Database now has {stats['total_articles']} articles "
            f"across {stats['tickers_tracked']} tickers."
        )

    else:
        parser.print_help()

    conn.close()


if __name__ == "__main__":
    asyncio.run(main())