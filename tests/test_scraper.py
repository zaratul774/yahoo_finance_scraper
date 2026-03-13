"""
Tests for the Yahoo Finance News Scraper.
Covers database logic, deduplication, RSS parsing, and query helpers.
Does not make live network calls — all external I/O is mocked.
"""

import sqlite3
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

# Import everything we want to test
from scraper import (
    init_db,
    make_hash,
    save_articles,
    get_latest,
    get_all_tickers,
    get_stats,
    scrape_rss,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def db(tmp_path):
    """Provide a fresh in-memory-style database for each test."""
    conn = init_db(tmp_path / "test.db")
    yield conn
    conn.close()


SAMPLE_ARTICLES = [
    {
        "headline": "Apple hits record high",
        "source": "Reuters",
        "url": "https://finance.yahoo.com/news/apple-record-high",
        "published": "2024-01-15T10:00:00+00:00",
    },
    {
        "headline": "Apple warns on supply chain",
        "source": "Bloomberg",
        "url": "https://finance.yahoo.com/news/apple-supply-chain",
        "published": "2024-01-15T08:00:00+00:00",
    },
]


# ── Database tests ────────────────────────────────────────────────────────────

class TestInitDb:
    def test_creates_articles_table(self, db):
        tables = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = [t["name"] for t in tables]
        assert "articles" in table_names

    def test_creates_indexes(self, db):
        indexes = db.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()
        index_names = [i["name"] for i in indexes]
        assert "idx_ticker" in index_names
        assert "idx_published" in index_names


class TestMakeHash:
    def test_same_inputs_same_hash(self):
        assert make_hash("AAPL", "Apple hits record high") == \
               make_hash("AAPL", "Apple hits record high")

    def test_different_ticker_different_hash(self):
        assert make_hash("AAPL", "headline") != make_hash("TSLA", "headline")

    def test_different_headline_different_hash(self):
        assert make_hash("AAPL", "headline one") != make_hash("AAPL", "headline two")

    def test_returns_string(self):
        assert isinstance(make_hash("AAPL", "test"), str)


class TestSaveArticles:
    def test_inserts_new_articles(self, db):
        inserted = save_articles(db, "AAPL", SAMPLE_ARTICLES)
        assert inserted == 2

    def test_skips_duplicates(self, db):
        save_articles(db, "AAPL", SAMPLE_ARTICLES)
        inserted_again = save_articles(db, "AAPL", SAMPLE_ARTICLES)
        assert inserted_again == 0

    def test_partial_duplicates(self, db):
        save_articles(db, "AAPL", SAMPLE_ARTICLES[:1])
        inserted = save_articles(db, "AAPL", SAMPLE_ARTICLES)
        assert inserted == 1  # Only the second article is new

    def test_stores_correct_ticker(self, db):
        save_articles(db, "AAPL", SAMPLE_ARTICLES)
        rows = db.execute("SELECT DISTINCT ticker FROM articles").fetchall()
        assert rows[0]["ticker"] == "AAPL"

    def test_empty_list_inserts_nothing(self, db):
        inserted = save_articles(db, "AAPL", [])
        assert inserted == 0

    def test_scraped_at_is_utc_iso(self, db):
        save_articles(db, "AAPL", SAMPLE_ARTICLES[:1])
        row = db.execute("SELECT scraped_at FROM articles").fetchone()
        # Should parse without error
        dt = datetime.fromisoformat(row["scraped_at"])
        assert dt.tzinfo is not None


# ── Query helper tests ────────────────────────────────────────────────────────

class TestGetLatest:
    def test_returns_articles_for_ticker(self, db):
        save_articles(db, "AAPL", SAMPLE_ARTICLES)
        results = get_latest(db, "AAPL", limit=10)
        assert len(results) == 2

    def test_respects_limit(self, db):
        save_articles(db, "AAPL", SAMPLE_ARTICLES)
        results = get_latest(db, "AAPL", limit=1)
        assert len(results) == 1

    def test_returns_empty_for_unknown_ticker(self, db):
        results = get_latest(db, "UNKNOWN", limit=10)
        assert results == []

    def test_case_insensitive_ticker(self, db):
        save_articles(db, "AAPL", SAMPLE_ARTICLES)
        results = get_latest(db, "aapl", limit=10)
        assert len(results) == 2


class TestGetAllTickers:
    def test_returns_all_tickers(self, db):
        save_articles(db, "AAPL", SAMPLE_ARTICLES)
        save_articles(db, "TSLA", SAMPLE_ARTICLES)
        tickers = get_all_tickers(db)
        assert set(tickers) == {"AAPL", "TSLA"}

    def test_returns_empty_when_no_data(self, db):
        assert get_all_tickers(db) == []

    def test_no_duplicates(self, db):
        save_articles(db, "AAPL", SAMPLE_ARTICLES)
        save_articles(db, "AAPL", [{"headline": "Another headline", "source": "WSJ", "url": "", "published": None}])
        tickers = get_all_tickers(db)
        assert tickers.count("AAPL") == 1


class TestGetStats:
    def test_counts_articles(self, db):
        save_articles(db, "AAPL", SAMPLE_ARTICLES)
        stats = get_stats(db)
        assert stats["total_articles"] == 2

    def test_counts_tickers(self, db):
        save_articles(db, "AAPL", SAMPLE_ARTICLES)
        save_articles(db, "TSLA", SAMPLE_ARTICLES)
        stats = get_stats(db)
        assert stats["tickers_tracked"] == 2

    def test_empty_database(self, db):
        stats = get_stats(db)
        assert stats["total_articles"] == 0
        assert stats["tickers_tracked"] == 0


# ── RSS fallback tests ────────────────────────────────────────────────────────

RSS_SAMPLE = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Yahoo Finance</title>
    <item>
      <title>Apple Q1 earnings beat expectations</title>
      <link>https://finance.yahoo.com/news/apple-q1</link>
      <source>Reuters</source>
      <pubDate>Mon, 15 Jan 2024 10:00:00 +0000</pubDate>
    </item>
    <item>
      <title>Apple announces new product line</title>
      <link>https://finance.yahoo.com/news/apple-products</link>
      <source>Bloomberg</source>
      <pubDate>Mon, 15 Jan 2024 08:00:00 +0000</pubDate>
    </item>
  </channel>
</rss>"""


class TestScrapeRss:
    def test_parses_headlines(self):
        mock_response = MagicMock()
        mock_response.text = RSS_SAMPLE
        mock_response.raise_for_status = MagicMock()

        with patch("scraper.httpx.get", return_value=mock_response):
            articles = scrape_rss("AAPL")

        assert len(articles) == 2
        assert articles[0]["headline"] == "Apple Q1 earnings beat expectations"

    def test_parses_iso_timestamps(self):
        mock_response = MagicMock()
        mock_response.text = RSS_SAMPLE
        mock_response.raise_for_status = MagicMock()

        with patch("scraper.httpx.get", return_value=mock_response):
            articles = scrape_rss("AAPL")

        # Should be a valid ISO 8601 string, not a relative string
        dt = datetime.fromisoformat(articles[0]["published"])
        assert dt.year == 2024

    def test_returns_empty_on_network_error(self):
        with patch("scraper.httpx.get", side_effect=Exception("Network error")):
            articles = scrape_rss("AAPL")
        assert articles == []

    def test_respects_max_articles(self):
        mock_response = MagicMock()
        mock_response.text = RSS_SAMPLE
        mock_response.raise_for_status = MagicMock()

        with patch("scraper.httpx.get", return_value=mock_response):
            articles = scrape_rss("AAPL", max_articles=1)

        assert len(articles) == 1