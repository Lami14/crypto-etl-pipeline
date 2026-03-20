"""
extract.py
----------
Fetches cryptocurrency market data from the CoinGecko public API.
No API key required for the free tier.
"""

import requests
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"

# Top coins to track - easily extendable
COINS_TO_TRACK = [
    "bitcoin", "ethereum", "solana", "cardano",
    "ripple", "dogecoin", "polkadot", "chainlink"
]

def fetch_crypto_data(coins: list = COINS_TO_TRACK, currency: str = "usd") -> list[dict]:
    """
    Fetch market data for a list of coins from CoinGecko.

    Args:
        coins: List of coin IDs to fetch.
        currency: Target currency for prices (default: usd).

    Returns:
        List of raw coin data dictionaries.
    
    Raises:
        Exception: If the API request fails.
    """
    params = {
        "vs_currency": currency,
        "ids": ",".join(coins),
        "order": "market_cap_desc",
        "per_page": len(coins),
        "page": 1,
        "sparkline": False,
        "price_change_percentage": "24h"
    }

    logger.info(f"[EXTRACT] Fetching data for {len(coins)} coins at {datetime.now()}")

    try:
        response = requests.get(COINGECKO_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info(f"[EXTRACT] Successfully fetched {len(data)} records.")
        return data

    except requests.exceptions.HTTPError as e:
        logger.error(f"[EXTRACT] HTTP error: {e}")
        raise
    except requests.exceptions.ConnectionError:
        logger.error("[EXTRACT] Connection error — check your internet connection.")
        raise
    except requests.exceptions.Timeout:
        logger.error("[EXTRACT] Request timed out.")
        raise


if __name__ == "__main__":
    data = fetch_crypto_data()
    for coin in data:
        print(f"{coin['name']}: ${coin['current_price']}")
      
