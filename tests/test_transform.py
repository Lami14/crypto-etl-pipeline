"""
test_transform.py
-----------------
Unit tests for the transform module.

Tests cover:
    - Normal transformation of valid data
    - Handling of missing/null fields
    - Handling of invalid types
    - Summary statistics generation

Run with:
    pytest tests/test_transform.py -v
"""

import pytest
from datetime import datetime, timezone
from etl.transform import transform_crypto_data, get_summary_stats


# ── Fixtures ──────────────────────────────────────────────────

@pytest.fixture
def valid_raw_data():
    """Well-formed raw API response for two coins."""
    return [
        {
            "id":                            "bitcoin",
            "name":                          "Bitcoin",
            "symbol":                        "btc",
            "current_price":                 65000.0,
            "market_cap":                    1280000000000,
            "total_volume":                  35000000000,
            "price_change_24h":              1200.0,
            "price_change_percentage_24h":   1.85,
            "high_24h":                      66000.0,
            "low_24h":                       63500.0,
            "circulating_supply":            19700000,
        },
        {
            "id":                            "ethereum",
            "name":                          "Ethereum",
            "symbol":                        "eth",
            "current_price":                 3500.0,
            "market_cap":                    420000000000,
            "total_volume":                  18000000000,
            "price_change_24h":              -50.0,
            "price_change_percentage_24h":   -1.41,
            "high_24h":                      3600.0,
            "low_24h":                       3450.0,
            "circulating_supply":            120000000,
        }
    ]


@pytest.fixture
def raw_data_missing_optional():
    """Raw data with optional fields missing — should still transform."""
    return [{
        "id":            "solana",
        "name":          "Solana",
        "symbol":        "sol",
        "current_price": 150.0,
        "market_cap":    None,
        "total_volume":  None,
        "price_change_24h": None,
        "price_change_percentage_24h": None,
        "high_24h":      None,
        "low_24h":       None,
        "circulating_supply": None,
    }]


@pytest.fixture
def raw_data_missing_critical():
    """Raw data missing required fields — should be skipped."""
    return [
        {"id": "badcoin", "name": "Bad Coin"},          # Missing current_price
        {"current_price": 100.0},                        # Missing id
        {},                                              # Completely empty
    ]


@pytest.fixture
def raw_data_invalid_types():
    """Raw data with wrong types — should be handled gracefully."""
    return [{
        "id":            "brokencoin",
        "name":          "Broken Coin",
        "symbol":        "brk",
        "current_price": "not_a_number",   # Invalid type
        "market_cap":    "also_invalid",
    }]


# ── Tests: transform_crypto_data ─────────────────────────────

class TestTransformCryptoData:

    def test_returns_list(self, valid_raw_data):
        """transform_crypto_data should return a list."""
        result = transform_crypto_data(valid_raw_data)
        assert isinstance(result, list)

    def test_correct_number_of_records(self, valid_raw_data):
        """Should return one record per valid input coin."""
        result = transform_crypto_data(valid_raw_data)
        assert len(result) == 2

    def test_output_contains_required_keys(self, valid_raw_data):
        """Each transformed record must contain all required keys."""
        required_keys = {
            "coin_id", "coin_name", "symbol",
            "current_price_usd", "market_cap", "total_volume",
            "price_change_24h", "price_change_percentage_24h",
            "high_24h", "low_24h", "circulating_supply", "fetched_at"
        }
        result = transform_crypto_data(valid_raw_data)
        for record in result:
            assert required_keys.issubset(record.keys()), (
                f"Missing keys: {required_keys - record.keys()}"
            )

    def test_coin_id_is_correct(self, valid_raw_data):
        """coin_id should match the input id field."""
        result = transform_crypto_data(valid_raw_data)
        ids = [r["coin_id"] for r in result]
        assert "bitcoin"  in ids
        assert "ethereum" in ids

    def test_symbol_is_uppercase(self, valid_raw_data):
        """Symbol should be converted to uppercase."""
        result = transform_crypto_data(valid_raw_data)
        for record in result:
            assert record["symbol"] == record["symbol"].upper()

    def test_price_is_float(self, valid_raw_data):
        """current_price_usd should be a float."""
        result = transform_crypto_data(valid_raw_data)
        for record in result:
            assert isinstance(record["current_price_usd"], float)

    def test_fetched_at_is_datetime(self, valid_raw_data):
        """fetched_at should be a datetime object."""
        result = transform_crypto_data(valid_raw_data)
        for record in result:
            assert isinstance(record["fetched_at"], datetime)

    def test_missing_optional_fields_default_to_none(self, raw_data_missing_optional):
        """Optional null fields should not cause errors — default to None or 0."""
        result = transform_crypto_data(raw_data_missing_optional)
        assert len(result) == 1
        record = result[0]
        assert record["market_cap"]       is None
        assert record["total_volume"]     is None
        assert record["high_24h"]         is None
        assert record["low_24h"]          is None
        assert record["circulating_supply"] is None

    def test_missing_price_change_defaults_to_zero(self, raw_data_missing_optional):
        """Missing price_change_24h should default to 0.0, not raise."""
        result = transform_crypto_data(raw_data_missing_optional)
        assert result[0]["price_change_24h"] == 0.0

    def test_skips_records_missing_critical_fields(self, raw_data_missing_critical):
        """Records missing id or current_price should be skipped silently."""
        result = transform_crypto_data(raw_data_missing_critical)
        assert len(result) == 0

    def test_skips_records_with_invalid_types(self, raw_data_invalid_types):
        """Records with invalid field types should be skipped, not raise."""
        result = transform_crypto_data(raw_data_invalid_types)
        assert len(result) == 0

    def test_empty_input_returns_empty_list(self):
        """Empty input should return empty list without error."""
        result = transform_crypto_data([])
        assert result == []

    def test_bitcoin_price_correct(self, valid_raw_data):
        """Bitcoin price should be correctly mapped to current_price_usd."""
        result = transform_crypto_data(valid_raw_data)
        btc = next(r for r in result if r["coin_id"] == "bitcoin")
        assert btc["current_price_usd"] == 65000.0

    def test_negative_price_change_preserved(self, valid_raw_data):
        """Negative price changes should be preserved correctly."""
        result = transform_crypto_data(valid_raw_data)
        eth = next(r for r in result if r["coin_id"] == "ethereum")
        assert eth["price_change_24h"] < 0


# ── Tests: get_summary_stats ──────────────────────────────────

class TestGetSummaryStats:

    def test_returns_dict(self, valid_raw_data):
        """get_summary_stats should return a dictionary."""
        records = transform_crypto_data(valid_raw_data)
        result  = get_summary_stats(records)
        assert isinstance(result, dict)

    def test_total_coins_correct(self, valid_raw_data):
        """total_coins should match number of records."""
        records = transform_crypto_data(valid_raw_data)
        result  = get_summary_stats(records)
        assert result["total_coins"] == 2

    def test_top_gainer_is_string(self, valid_raw_data):
        """top_gainer should be a coin name string."""
        records = transform_crypto_data(valid_raw_data)
        result  = get_summary_stats(records)
        assert isinstance(result["top_gainer"], str)

    def test_top_loser_is_string(self, valid_raw_data):
        """top_loser should be a coin name string."""
        records = transform_crypto_data(valid_raw_data)
        result  = get_summary_stats(records)
        assert isinstance(result["top_loser"], str)

    def test_top_gainer_not_same_as_loser(self, valid_raw_data):
        """top_gainer and top_loser should be different coins when changes differ."""
        records = transform_crypto_data(valid_raw_data)
        result  = get_summary_stats(records)
        assert result["top_gainer"] != result["top_loser"]

    def test_avg_price_is_numeric(self, valid_raw_data):
        """avg_price_usd should be a positive number."""
        records = transform_crypto_data(valid_raw_data)
        result  = get_summary_stats(records)
        assert isinstance(result["avg_price_usd"], float)
        assert result["avg_price_usd"] > 0

    def test_avg_price_correct_value(self, valid_raw_data):
        """avg_price_usd should be the mean of Bitcoin and Ethereum prices."""
        records = transform_crypto_data(valid_raw_data)
        result  = get_summary_stats(records)
        expected = round((65000.0 + 3500.0) / 2, 4)
        assert result["avg_price_usd"] == expected

    def test_empty_records_returns_empty_dict(self):
        """get_summary_stats with empty list should return empty dict."""
        result = get_summary_stats([])
        assert result == {}

    def test_batch_time_is_string(self, valid_raw_data):
        """batch_time should be an ISO format string."""
        records = transform_crypto_data(valid_raw_data)
        result  = get_summary_stats(records)
        assert isinstance(result["batch_time"], str)
        assert "T" in result["batch_time"]  # ISO format contains T
