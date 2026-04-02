import os
import requests
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

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


def get_top_boosted_tokens(limit: int = 15) -> List[Dict[str, Any]]:
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


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


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


def get_age_hours(pair_created_at: Any) -> float:
    created_ms = to_float(pair_created_at, 0.0)
    if created_ms <= 0:
        return 0.0
    created_dt = datetime.fromtimestamp(created_ms / 1000, tz=timezone.utc)
    now_dt = datetime.now(timezone.utc)
    delta = now_dt - created_dt
    return max(delta.total_seconds() / 3600, 0.0)


def basic_filter(pair: Dict[str, Any]) -> bool:
    liquidity_usd = to_float((pair.get("liquidity") or {}).get("usd"))
    volume_24h = to_float((pair.get("volume") or {}).get("h24"))
    fdv = to_float(pair.get("fdv"))
    age_hours = get_age_hours(pair.get("pairCreatedAt"))

    if liquidity_usd < 100_000:
        return False

    if volume_24h < 50_000:
        return False

    if age_hours < 24:
        return False

    if liquidity_usd > 0 and fdv > 0 and (fdv / liquidity_usd) > 30:
        return False

    return True


def analyze_pair(pair: Dict[str, Any], boost_info: Dict[str, Any]) -> Dict[str, Any]:
    base_token = pair.get("baseToken") or {}
    quote_token = pair.get("quoteToken") or {}
    liquidity = pair.get("liquidity") or {}
    volume = pair.get("volume") or {}

    name = base_token.get("name") or "-"
    symbol = base_token.get("symbol") or "-"
    chain_id = pair.get("chainId") or boost_info.get("chainId") or "-"
    token_address = base_token.get("address") or boost_info.get("tokenAddress") or "-"
    price_usd = to_float(pair.get("priceUsd"), None)
    liquidity_usd = to_float(liquidity.get("usd"))
    volume_24h = to_float(volume.get("h24"))
    fdv = to_float(pair.get("fdv"))
    market_cap = to_float(pair.get("marketCap"))
    age_hours = get_age_hours(pair.get("pairCreatedAt"))
    dex_id = pair.get("dexId") or "-"
    pair_url = pair.get("url") or "-"
    token_url = boost_info.get("url") or "-"
    description = boost_info.get("description") or "-"
    boost_amount = to_float(boost_info.get("amount"))
    boost_total = to_float(boost_info.get("totalAmount"))
    quote_symbol = quote_token.get("symbol") or "-"

    fdv_liquidity_ratio = (fdv / liquidity_usd) if liquidity_usd > 0 and fdv > 0 else 0.0
    volume_liquidity_ratio = (volume_24h / liquidity_usd) if liquidity_usd > 0 else 0.0

    score = 100
    reasons: List[str] = []

    # 유동성 평가
    if liquidity_usd < 150_000:
        score -= 20
        reasons.append("유동성이 낮음")
    elif liquidity_usd < 300_000:
        score -= 10
        reasons.append("유동성이 보통 이하")
    else:
        reasons.append("유동성이 양호")

    # 거래량 평가
    if volume_24h < 100_000:
        score -= 15
        reasons.append("24h 거래량이 낮음")
    elif volume_24h < 300_000:
        score -= 8
        reasons.append("24h 거래량이 보통")
    else:
        reasons.append("거래량이 양호")

    # FDV / 유동성 비율
    if fdv_liquidity_ratio > 20:
        score -= 25
        reasons.append("FDV 대비 유동성 과열")
    elif fdv_liquidity_ratio > 10:
        score -= 12
        reasons.append("FDV 대비 유동성 다소 부담")
    else:
        reasons.append("FDV/유동성 구조 무난")

    # 거래량 / 유동성 비율
    if volume_liquidity_ratio < 0.2:
        score -= 12
        reasons.append("거래 회전이 약함")
    elif volume_liquidity_ratio > 3.0:
        score -= 8
        reasons.append("과열 거래 가능성")
    else:
        reasons.append("거래 회전이 무난")

    # 신생 위험
    if age_hours < 72:
        score -= 18
        reasons.append("생성된지 얼마 안 된 신생 페어")
    elif age_hours < 168:
        score -= 8
        reasons.append("상대적으로 신생 프로젝트")
    else:
        reasons.append("최소 생존 기간 통과")

    # 시총/FDV 비교
    if fdv > 0 and market_cap > 0:
        if market_cap < fdv * 0.3:
            score -= 10
            reasons.append("유통 시총이 FDV 대비 낮음")

    # 광고성 boost 참고
    if boost_total > 0:
        reasons.append("Boost 노출 토큰")

    if score >= 80:
        grade = "관심"
        verdict = "구조상 상대적으로 양호"
    elif score >= 60:
        grade = "관찰"
        verdict = "추가 확인 필요"
    else:
        grade = "위험"
        verdict = "구조상 위험 신호가 큼"

    return {
        "name": name,
        "symbol": symbol,
        "chain_id": chain_id,
        "token_address": token_address,
        "price_usd": price_usd,
        "liquidity_usd": liquidity_usd,
        "volume_24h": volume_24h,
        "fdv": fdv,
        "market_cap": market_cap,
        "age_hours": age_hours,
        "dex_id": dex_id,
        "pair_url": pair_url,
        "token_url": token_url,
        "description": description,
        "boost_amount": boost_amount,
        "boost_total": boost_total,
        "quote_symbol": quote_symbol,
        "fdv_liquidity_ratio": fdv_liquidity_ratio,
        "volume_liquidity_ratio": volume_liquidity_ratio,
        "score": max(score, 0),
        "grade": grade,
        "verdict": verdict,
        "reasons": reasons[:5],
    }


def build_message() -> str:
    boosted_tokens = get_top_boosted_tokens(limit=15)
    analyzed_projects: List[Dict[str, Any]] = []

    for token in boosted_tokens:
        chain_id = token.get("chainId")
        token_address = token.get("tokenAddress")

        if not chain_id or not token_address:
            continue

        pairs = get_token_pairs(chain_id, token_address)
        best_pair = choose_best_pair(pairs)

        if not best_pair:
            continue

        if not basic_filter(best_pair):
            continue

        analyzed = analyze_pair(best_pair, token)
        analyzed_projects.append(analyzed)

    analyzed_projects.sort(key=lambda x: x["score"], reverse=True)
    final_projects = analyzed_projects[:3]

    if not final_projects:
        return (
            "[오늘의 디파이 분석 결과]\n\n"
            "조건을 통과한 프로젝트가 없습니다.\n"
            "- 유동성 부족\n"
            "- 거래량 부족\n"
            "- 너무 신생\n"
            "- FDV 과열\n"
            "중 하나로 제외되었을 가능성이 큽니다."
        )

    lines: List[str] = []
    lines.append("[오늘의 투자용 디파이 분석 봇 결과]")
    lines.append("")

    for idx, item in enumerate(final_projects, start=1):
        lines.append(f"{idx}) {item['name']} ({item['symbol']})")
        lines.append(f"- 판정: {item['grade']} / 점수: {item['score']}")
        lines.append(f"- 결론: {item['verdict']}")
        lines.append(f"- 체인: {item['chain_id']}")
        lines.append(f"- 가격: {fmt_num(item['price_usd'])} USD")
        lines.append(f"- 유동성: {fmt_num(item['liquidity_usd'])}")
        lines.append(f"- 24h 거래량: {fmt_num(item['volume_24h'])}")
        lines.append(f"- FDV: {fmt_num(item['fdv'])}")
        lines.append(f"- 시총: {fmt_num(item['market_cap'])}")
        lines.append(f"- FDV/유동성: {item['fdv_liquidity_ratio']:.2f}")
        lines.append(f"- 거래량/유동성: {item['volume_liquidity_ratio']:.2f}")
        lines.append(f"- 생성 후 경과: {item['age_hours']:.1f}시간")
        lines.append(f"- DEX: {item['dex_id']} / 상대토큰: {item['quote_symbol']}")
        lines.append(f"- 핵심 이유: {', '.join(item['reasons'])}")
        lines.append(f"- 페어 링크: {item['pair_url']}")
        lines.append("")

    lines.append("주의: 이 버전은 Dexscreener 기반 시장구조 분석이며,")
    lines.append("TVL / Revenue / 락업 / 팀 검증은 아직 포함되지 않았습니다.")
    return "\n".join(lines)


def main() -> None:
    message = build_message()
    send_telegram_message(message)


if __name__ == "__main__":
    main()
