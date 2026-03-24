"""
test_extract.py
---------------
Unit tests for the extract module.

Tests use unittest.mock to simulate CoinGecko API responses
so tests run without a real internet connection or API key.

Run with:
    pytest tests/test_extract.py -v
"""

import pytest
from unittest.mock import patch, MagicMock
from etl.extract import fetch_crypto_data, fetch_multiple_stocks, COINS_TO_TRACK


# ── Fixtures ──────────────────────────────────────────────────

@pytest.fixture
def mock_api_response():
    """Sample CoinGecko API response for two coins."""
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
def empty_api_response():
    """Empty API response simulating no data returned."""
    return []


# ── Tests: fetch_crypto_data ───────────────────────────────────

class TestFetchCryptoData:

    @patch("etl.extract.requests.get")
    def test_returns_list_on_success(self, mock_get, mock_api_response):
        """fetch_crypto_data should return a list when API call succeeds."""
        mock_response = MagicMock()
        mock_response.json.return_value = mock_api_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = fetch_crypto_data(["bitcoin", "ethereum"])

        assert isinstance(result, list)
        assert len(result) == 2

    @patch("etl.extract.requests.get")
    def test_returns_correct_coin_ids(self, mock_get, mock_api_response):
        """fetch_crypto_data should return data with correct coin IDs."""
        mock_response = MagicMock()
        mock_response.json.return_value = mock_api_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = fetch_crypto_data(["bitcoin", "ethereum"])
        ids = [coin["id"] for coin in result]

        assert "bitcoin"  in ids
        assert "ethereum" in ids

    @patch("etl.extract.requests.get")
    def test_returns_empty_list_when_no_data(self, mock_get, empty_api_response):
        """fetch_crypto_data should return empty list when API returns nothing."""
        mock_response = MagicMock()
        mock_response.json.return_value = empty_api_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = fetch_crypto_data(["bitcoin"])

        assert result == []

    @patch("etl.extract.requests.get")
    def test_raises_on_http_error(self, mock_get):
        """fetch_crypto_data should raise an exception on HTTP errors."""
        import requests
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
        mock_get.return_value = mock_response

        with pytest.raises(requests.exceptions.HTTPError):
            fetch_crypto_data(["bitcoin"])

    @patch("etl.extract.requests.get")
    def test_raises_on_connection_error(self, mock_get):
        """fetch_crypto_data should raise on network connection failure."""
        import requests
        mock_get.side_effect = requests.exceptions.ConnectionError("No internet")

        with pytest.raises(requests.exceptions.ConnectionError):
            fetch_crypto_data(["bitcoin"])

    @patch("etl.extract.requests.get")
    def test_raises_on_timeout(self, mock_get):
        """fetch_crypto_data should raise on request timeout."""
        import requests
        mock_get.side_effect = requests.exceptions.Timeout("Request timed out")

        with pytest.raises(requests.exceptions.Timeout):
            fetch_crypto_data(["bitcoin"])

    @patch("etl.extract.requests.get")
    def test_correct_params_sent_to_api(self, mock_get, mock_api_response):
        """fetch_crypto_data should pass correct params to the CoinGecko API."""
        mock_response = MagicMock()
        mock_response.json.return_value = mock_api_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        fetch_crypto_data(["bitcoin", "ethereum"], currency="usd")

        call_kwargs = mock_get.call_args[1]
        assert call_kwargs["params"]["vs_currency"] == "usd"
        assert "bitcoin"  in call_kwargs["params"]["ids"]
        assert "ethereum" in call_kwargs["params"]["ids"]

    def test_default_coins_list_not_empty(self):
        """COINS_TO_TRACK should be a non-empty list of strings."""
        assert isinstance(COINS_TO_TRACK, list)
        assert len(COINS_TO_TRACK) > 0
        assert all(isinstance(c, str) for c in COINS_TO_TRACK)


# ── Tests: fetch_multiple_stocks ──────────────────────────────

class TestFetchMultipleStocks:

    @patch("etl.extract.fetch_crypto_data")
    def test_returns_dict_with_ticker_keys(self, mock_fetch, mock_api_response):
        """fetch_multiple_stocks should return a dict keyed by ticker."""
        mock_fetch.return_value = mock_api_response

        result = fetch_multiple_stocks(["bitcoin", "ethereum"])

        assert isinstance(result, dict)

    @patch("etl.extract.fetch_crypto_data")
    def test_excludes_failed_tickers(self, mock_fetch):
        """fetch_multiple_stocks should skip tickers that return empty data."""
        mock_fetch.return_value = []

        result = fetch_multiple_stocks(["invalid_ticker"])

        assert result == {}

    @patch("etl.extract.fetch_crypto_data")
    def test_handles_empty_tickers_list(self, mock_fetch):
        """fetch_multiple_stocks with empty list should return empty dict."""
        result = fetch_multiple_stocks([])
        assert result == {}
        mock_fetch.assert_not_called()
