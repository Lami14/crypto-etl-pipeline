"""
transform.py
------------
Cleans, validates, and shapes raw CoinGecko API data
into records ready for database insertion.
"""

import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transform_crypto_data(raw_data: list[dict]) -> list[dict]:
    """
    Transform raw CoinGecko API response into clean DB-ready records.

    Steps:
        - Extract only required fields
        - Handle missing/null values with safe defaults
        - Add a fetched_at timestamp
        - Validate that critical fields are present

    Args:
        raw_data: List of raw coin dicts from the CoinGecko API.

    Returns:
        List of cleaned, validated dictionaries ready for DB insertion.
    """
    transformed = []
    skipped = 0

    for coin in raw_data:
        try:
            # Validate critical fields exist
            if not coin.get("id") or not coin.get("current_price"):
                logger.warning(f"[TRANSFORM] Skipping coin with missing critical fields: {coin.get('id', 'UNKNOWN')}")
                skipped += 1
                continue

            record = {
                "coin_id": coin["id"],
                "coin_name": coin.get("name", "Unknown"),
                "symbol": coin.get("symbol", "???").upper(),
                "current_price_usd": float(coin["current_price"]),
                "market_cap": float(coin["market_cap"]) if coin.get("market_cap") else None,
                "total_volume": float(coin["total_volume"]) if coin.get("total_volume") else None,
                "price_change_24h": float(coin["price_change_24h"]) if coin.get("price_change_24h") else 0.0,
                "price_change_percentage_24h": float(coin.get("price_change_percentage_24h") or 0.0),
                "high_24h": float(coin["high_24h"]) if coin.get("high_24h") else None,
                "low_24h": float(coin["low_24h"]) if coin.get("low_24h") else None,
                "circulating_supply": float(coin["circulating_supply"]) if coin.get("circulating_supply") else None,
                "fetched_at": datetime.now(timezone.utc)
            }

            transformed.append(record)

        except (ValueError, TypeError) as e:
            logger.error(f"[TRANSFORM] Error processing coin '{coin.get('id')}': {e}")
            skipped += 1
            continue

    logger.info(f"[TRANSFORM] {len(transformed)} records transformed. {skipped} skipped.")
    return transformed


def get_summary_stats(records: list[dict]) -> dict:
    """
    Generate a quick summary of the transformed batch.
    Useful for logging and monitoring.
    """
    if not records:
        return {}

    prices = [r["current_price_usd"] for r in records]
    changes = [r["price_change_percentage_24h"] for r in records]

    return {
        "total_coins": len(records),
        "avg_price_usd": round(sum(prices) / len(prices), 4),
        "top_gainer": max(records, key=lambda x: x["price_change_percentage_24h"])["coin_name"],
        "top_loser": min(records, key=lambda x: x["price_change_percentage_24h"])["coin_name"],
        "batch_time": datetime.now(timezone.utc).isoformat()
    }


if __name__ == "__main__":
    # Quick test with mock data
    mock = [{
        "id": "bitcoin", "name": "Bitcoin", "symbol": "btc",
        "current_price": 65000, "market_cap": 1280000000000,
        "total_volume": 35000000000, "price_change_24h": 1200,
        "price_change_percentage_24h": 1.85, "high_24h": 66000,
        "low_24h": 63500, "circulating_supply": 19700000
    }]
    result = transform_crypto_data(mock)
    print(result)
    print(get_summary_stats(result))
  
