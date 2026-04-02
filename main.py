import os
import requests
from typing import Any, Dict, List, Optional

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

DEX_TOP_BOOSTS_URL = "https://api.dexscreener.com/token-boosts/top/v1"
DEX_TOKEN_PAIRS_URL = "https://api.dexscreener.com/token-pairs/v1/{chain_id}/{token_address}"


def send_telegram_message(text: str) -> None:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        raise ValueError("TG_BOT_TOKEN 또는 TG_CHAT_ID가 설정되지 않았습니다.")

    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text
    }
    response = requests.post(url, json=payload, timeout=20)
    response.raise_for_status()


def get_top_boosted_tokens(limit: int = 3) -> List[Dict[str, Any]]:
    response = requests.get(DEX_TOP_BOOSTS_URL, timeout=20)
    response.raise_for_status()
    data = response.json()

    if not isinstance(data, list):
        raise ValueError("Dexscreener top boosts 응답 형식이 예상과 다릅니다.")

    return data[:limit]


def get_token_pairs(chain_id: str, token_address: str) -> List[Dict[str, Any]]:
    url = DEX_TOKEN_PAIRS_URL.format(chain_id=chain_id, token_address=token_address)
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    data = response.json()

    if not isinstance(data, list):
        raise ValueError("Dexscreener token pairs 응답 형식이 예상과 다릅니다.")

    return data


def choose_best_pair(pairs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not pairs:
        return None

    def liquidity_score(pair: Dict[str, Any]) -> float:
        liquidity = pair.get("liquidity") or {}
        usd = liquidity.get("usd")
        try:
            return float(usd) if usd is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    return max(pairs, key=liquidity_score)


def fmt_num(value: Any) -> str:
    if value is None:
        return "-"
    try:
        num = float(value)
    except (TypeError, ValueError):
        return str(value)

    if num >= 1_000_000_000:
        return f"{num/1_000_000_000:.2f}B"
    if num >= 1_000_000:
        return f"{num/1_000_000:.2f}M"
    if num >= 1_000:
        return f"{num/1_000:.2f}K"
    if num >= 1:
        return f"{num:.2f}"
    return f"{num:.6f}"


def build_message() -> str:
    boosted_tokens = get_top_boosted_tokens(limit=3)

    lines: List[str] = []
    lines.append("[오늘의 Dexscreener 상위 Boost 토큰 3개]")
    lines.append("")

    for idx, token in enumerate(boosted_tokens, start=1):
        chain_id = token.get("chainId", "-")
        token_address = token.get("tokenAddress", "-")
        boost_amount = token.get("amount", "-")
        boost_total = token.get("totalAmount", "-")
        token_url = token.get("url", "-")
        description = token.get("description") or "-"

        pairs = get_token_pairs(chain_id, token_address)
        best_pair = choose_best_pair(pairs)

        if best_pair:
            base_token = best_pair.get("baseToken") or {}
            quote_token = best_pair.get("quoteToken") or {}
            liquidity = best_pair.get("liquidity") or {}
            volume = best_pair.get("volume") or {}

            name = base_token.get("name") or "-"
            symbol = base_token.get("symbol") or "-"
            price_usd = best_pair.get("priceUsd")
            liquidity_usd = liquidity.get("usd")
            volume_24h = volume.get("h24")
            fdv = best_pair.get("fdv")
            market_cap = best_pair.get("marketCap")
            dex_id = best_pair.get("dexId", "-")
            pair_url = best_pair.get("url", "-")
            quote_symbol = quote_token.get("symbol") or "-"
        else:
            name = "-"
            symbol = "-"
            price_usd = None
            liquidity_usd = None
            volume_24h = None
            fdv = None
            market_cap = None
            dex_id = "-"
            pair_url = "-"
            quote_symbol = "-"

        lines.append(f"{idx}) {name} ({symbol})")
        lines.append(f"- 체인: {chain_id}")
        lines.append(f"- 주소: {token_address}")
        lines.append(f"- 가격(USD): {fmt_num(price_usd)}")
        lines.append(f"- 유동성: {fmt_num(liquidity_usd)}")
        lines.append(f"- 24h 거래량: {fmt_num(volume_24h)}")
        lines.append(f"- FDV: {fmt_num(fdv)}")
        lines.append(f"- 시가총액: {fmt_num(market_cap)}")
        lines.append(f"- DEX: {dex_id}")
        lines.append(f"- 상대 토큰: {quote_symbol}")
        lines.append(f"- 현재 Boost: {fmt_num(boost_amount)} / 총 Boost: {fmt_num(boost_total)}")
        lines.append(f"- 토큰 링크: {token_url}")
        lines.append(f"- 페어 링크: {pair_url}")
        lines.append(f"- 설명: {description}")
        lines.append("")

    lines.append("주의: Boost가 높다고 좋은 프로젝트라는 뜻은 아닙니다.")
    return "\n".join(lines)


def main() -> None:
    message = build_message()
    send_telegram_message(message)


if __name__ == "__main__":
    main()
