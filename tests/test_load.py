"""
test_load.py
------------
Unit tests for the load module.

All database calls are mocked using unittest.mock so tests
run without a real PostgreSQL connection.

Run with:
    pytest tests/test_load.py -v
"""

import pytest
from unittest.mock import patch, MagicMock, call
from datetime import datetime, timezone
from etl.load import insert_crypto_records, get_db_connection


# ── Fixtures ──────────────────────────────────────────────────

@pytest.fixture
def sample_records():
    """Well-formed transformed records ready for DB insertion."""
    return [
        {
            "coin_id":                    "bitcoin",
            "coin_name":                  "Bitcoin",
            "symbol":                     "BTC",
            "current_price_usd":          65000.0,
            "market_cap":                 1280000000000.0,
            "total_volume":               35000000000.0,
            "price_change_24h":           1200.0,
            "price_change_percentage_24h": 1.85,
            "high_24h":                   66000.0,
            "low_24h":                    63500.0,
            "circulating_supply":         19700000.0,
            "fetched_at":                 datetime.now(timezone.utc),
        },
        {
            "coin_id":                    "ethereum",
            "coin_name":                  "Ethereum",
            "symbol":                     "ETH",
            "current_price_usd":          3500.0,
            "market_cap":                 420000000000.0,
            "total_volume":               18000000000.0,
            "price_change_24h":           -50.0,
            "price_change_percentage_24h": -1.41,
            "high_24h":                   3600.0,
            "low_24h":                    3450.0,
            "circulating_supply":         120000000.0,
            "fetched_at":                 datetime.now(timezone.utc),
        }
    ]


@pytest.fixture
def single_record():
    """A single transformed record."""
    return [{
        "coin_id":                    "solana",
        "coin_name":                  "Solana",
        "symbol":                     "SOL",
        "current_price_usd":          150.0,
        "market_cap":                 65000000000.0,
        "total_volume":               3000000000.0,
        "price_change_24h":           5.0,
        "price_change_percentage_24h": 3.45,
        "high_24h":                   155.0,
        "low_24h":                    145.0,
        "circulating_supply":         440000000.0,
        "fetched_at":                 datetime.now(timezone.utc),
    }]


# ── Tests: get_db_connection ──────────────────────────────────

class TestGetDbConnection:

    @patch("etl.load.psycopg2.connect")
    def test_returns_connection_on_success(self, mock_connect):
        """get_db_connection should return a connection object."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        result = get_db_connection()

        assert result == mock_conn
        mock_connect.assert_called_once()

    @patch("etl.load.psycopg2.connect")
    def test_raises_on_connection_failure(self, mock_connect):
        """get_db_connection should raise OperationalError on DB failure."""
        import psycopg2
        mock_connect.side_effect = psycopg2.OperationalError("Connection refused")

        with pytest.raises(psycopg2.OperationalError):
            get_db_connection()

    @patch("etl.load.os.getenv")
    @patch("etl.load.psycopg2.connect")
    def test_uses_environment_variables(self, mock_connect, mock_getenv):
        """get_db_connection should read credentials from environment variables."""
        mock_getenv.side_effect = lambda key, default=None: {
            "DB_HOST":     "testhost",
            "DB_PORT":     "5432",
            "DB_NAME":     "testdb",
            "DB_USER":     "testuser",
            "DB_PASSWORD": "testpass",
        }.get(key, default)

        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        get_db_connection()

        mock_connect.assert_called_once_with(
            host="testhost",
            port="5432",
            dbname="testdb",
            user="testuser",
            password="testpass"
        )


# ── Tests: insert_crypto_records ─────────────────────────────

class TestInsertCryptoRecords:

    @patch("etl.load.get_db_connection")
    def test_returns_correct_count(self, mock_get_conn, sample_records):
        """insert_crypto_records should return number of rows inserted."""
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        result = insert_crypto_records(sample_records)

        assert result == 2

    @patch("etl.load.get_db_connection")
    def test_single_record_returns_one(self, mock_get_conn, single_record):
        """insert_crypto_records should return 1 for a single record."""
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        result = insert_crypto_records(single_record)

        assert result == 1

    @patch("etl.load.get_db_connection")
    def test_empty_records_returns_zero(self, mock_get_conn):
        """insert_crypto_records with empty list should return 0."""
        result = insert_crypto_records([])
        assert result == 0
        mock_get_conn.assert_not_called()

    @patch("etl.load.get_db_connection")
    def test_connection_is_closed_after_insert(self, mock_get_conn, sample_records):
        """Database connection should always be closed after insertion."""
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        insert_crypto_records(sample_records)

        mock_conn.close.assert_called_once()

    @patch("etl.load.get_db_connection")
    def test_connection_closed_even_on_error(self, mock_get_conn, sample_records):
        """Connection should close even if insertion raises an error."""
        import psycopg2
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__.return_value.executemany = MagicMock(
            side_effect=psycopg2.Error("Insert failed")
        )

        with pytest.raises(Exception):
            insert_crypto_records(sample_records)

        mock_conn.close.assert_called_once()

    @patch("etl.load.get_db_connection")
    def test_db_not_called_for_empty_list(self, mock_get_conn):
        """No DB connection should be created for empty record list."""
        insert_crypto_records([])
        mock_get_conn.assert_not_called()

    def test_records_have_required_keys(self, sample_records):
        """All records in fixture should have required keys for insertion."""
        required = {
            "coin_id", "coin_name", "symbol", "current_price_usd",
            "market_cap", "total_volume", "price_change_24h",
            "price_change_percentage_24h", "high_24h", "low_24h",
            "circulating_supply", "fetched_at"
        }
        for record in sample_records:
            assert required.issubset(record.keys())

    def test_fetched_at_is_datetime(self, sample_records):
        """fetched_at in each record should be a datetime object."""
        for record in sample_records:
            assert isinstance(record["fetched_at"], datetime)

    def test_prices_are_positive(self, sample_records):
        """current_price_usd should be positive for all records."""
        for record in sample_records:
            assert record["current_price_usd"] > 0

    def test_symbols_are_uppercase(self, sample_records):
        """All symbols should be uppercase strings."""
        for record in sample_records:
            assert record["symbol"] == record["symbol"].upper()
      
