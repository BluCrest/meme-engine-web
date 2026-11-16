"""Strict DexScreener token-age gate.

A buy is allowed only when pairCreatedAt is present and the oldest known pair is
younger than max_age_seconds. Any fetch/parse failure is a rejection.
"""
import logging
import time
from typing import Any, Dict

import aiohttp

logger = logging.getLogger(__name__)

DEXSCREENER_TOKENS_URL = "https://api.dexscreener.com/tokens/v1/solana/{mint}"
DEFAULT_MAX_TOKEN_AGE_SECONDS = 1800


async def check_token_age(
    mint: str,
    max_age_seconds: int = DEFAULT_MAX_TOKEN_AGE_SECONDS,
    timeout_seconds: int = 8,
) -> Dict[str, Any]:
    if not mint:
        return {
            "ok": False,
            "reason": "missing_mint",
            "pair_created_at": None,
            "age_seconds": None,
        }

    url = DEXSCREENER_TOKENS_URL.format(mint=mint)
    try:
        timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return {
                        "ok": False,
                        "reason": f"dexscreener_http_{resp.status}",
                        "pair_created_at": None,
                        "age_seconds": None,
                    }
                data = await resp.json()
    except Exception as exc:
        logger.warning("DexScreener token-age fetch failed for %s: %s", mint[:8], exc)
        return {
            "ok": False,
            "reason": "dexscreener_fetch_failed",
            "pair_created_at": None,
            "age_seconds": None,
            "error": str(exc),
        }

    if not isinstance(data, list) or not data:
        return {
            "ok": False,
            "reason": "no_pairs",
            "pair_created_at": None,
            "age_seconds": None,
        }

    created_values = []
    for pair in data:
        if not isinstance(pair, dict):
            continue
        value = pair.get("pairCreatedAt")
        if value is None:
            continue
        try:
            created = int(value)
        except (TypeError, ValueError):
            continue
        if created > 0:
            created_values.append(created)

    if not created_values:
        return {
            "ok": False,
            "reason": "missing_pair_created_at",
            "pair_created_at": None,
            "age_seconds": None,
        }

    pair_created_at = min(created_values)
    age_seconds = (time.time() * 1000 - pair_created_at) / 1000.0
    if age_seconds > max_age_seconds:
        return {
            "ok": False,
            "reason": "token_too_old",
            "pair_created_at": pair_created_at,
            "age_seconds": age_seconds,
        }

    return {
        "ok": True,
        "reason": "ok",
        "pair_created_at": pair_created_at,
        "age_seconds": age_seconds,
    }
