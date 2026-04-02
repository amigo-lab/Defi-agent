import os
import requests
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

DEX_TOP_BOOSTS_URL = "https://api.dexscreener.com/token-boosts/top/v1"
DEX_TOKEN_PAIRS_URL = "https://api.dexscreener.com/token-pairs/v1/{chain_id}/{token_address}"
LLAMA_PROTOCOLS_URL = "https://api.llama.fi/protocols"

# 진짜 디파이 투자용 체인만 우선 허용
ALLOWED_CHAINS = {
    "ethereum",
    "arbitrum",
    "base",
    "polygon",
    "bsc",
    "optimism",
    "avalanche",
}

# DefiLlama에 없는 토큰 장난감 이름을 어느 정도 거르기 위한 키워드
LOW_QUALITY_KEYWORDS = {
    "inu", "pepe", "doge", "cat", "pump", "moon", "elon", "baby"
}


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


def get_top_boosted_tokens(limit: int = 20) -> List[Dict[str, Any]]:
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


def get_llama_protocols() -> List[Dict[str, Any]]:
    response = requests.get(LLAMA_PROTOCOLS_URL, timeout=30)
    response.raise_for_status()
    data = response.json()

    if not isinstance(data, list):
        raise ValueError("DefiLlama protocols 응답 형식이 예상과 다릅니다.")

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


def normalize_name(text: str) -> str:
    return "".join(ch.lower() for ch in text if ch.isalnum())


def contains_low_quality_keyword(name: str, symbol: str) -> bool:
    combined = f"{name} {symbol}".lower()
    return any(keyword in combined for keyword in LOW_QUALITY_KEYWORDS)


def basic_filter(pair: Dict[str, Any]) -> bool:
    chain_id = (pair.get("chainId") or "").lower()
    liquidity_usd = to_float((pair.get("liquidity") or {}).get("usd"))
    volume_24h = to_float((pair.get("volume") or {}).get("h24"))
    fdv = to_float(pair.get("fdv"))
    age_hours = get_age_hours(pair.get("pairCreatedAt"))

    base_token = pair.get("baseToken") or {}
    name = base_token.get("name") or ""
    symbol = base_token.get("symbol") or ""

    if chain_id not in ALLOWED_CHAINS:
        return False

    if contains_low_quality_keyword(name, symbol):
        return False

    if liquidity_usd < 150_000:
        return False

    if volume_24h < 80_000:
        return False

    if age_hours < 72:
        return False

    if liquidity_usd > 0 and fdv > 0 and (fdv / liquidity_usd) > 25:
        return False

    return True


def match_llama_protocol(
    token_name: str,
    token_symbol: str,
    chain_id: str,
    protocols: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    target_names = {
        normalize_name(token_name),
        normalize_name(token_symbol),
        normalize_name(f"{token_name}{token_symbol}")
    }

    best_match = None
    best_score = -1

    for protocol in protocols:
        pname = protocol.get("name") or ""
        pslug = protocol.get("slug") or ""
        pchains = [c.lower() for c in (protocol.get("chains") or [])]

        norm_name = normalize_name(pname)
        norm_slug = normalize_name(pslug)

        score = 0

        if chain_id in pchains:
            score += 2

        if norm_name in target_names or norm_slug in target_names:
            score += 10

        if any(t and t in norm_name for t in target_names):
            score += 4

        if any(t and t in norm_slug for t in target_names):
            score += 3

        if score > best_score:
            best_score = score
            best_match = protocol

    if best_score < 5:
        return None

    return best_match


def analyze_pair(
    pair: Dict[str, Any],
    boost_info: Dict[str, Any],
    llama_protocol: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    base_token = pair.get("baseToken") or {}
    quote_token = pair.get("quoteToken") or {}
    liquidity = pair.get("liquidity") or {}
    volume = pair.get("volume") or {}

    name = base_token.get("name") or "-"
    symbol = base_token.get("symbol") or "-"
    chain_id = (pair.get("chainId") or boost_info.get("chainId") or "-").lower()
    token_address = base_token.get("address") or boost_info.get("tokenAddress") or "-"
    price_usd = to_float(pair.get("priceUsd"), None)
    liquidity_usd = to_float(liquidity.get("usd"))
    volume_24h = to_float(volume.get("h24"))
    fdv = to_float(pair.get("fdv"))
    market_cap = to_float(pair.get("marketCap"))
    age_hours = get_age_hours(pair.get("pairCreatedAt"))
    dex_id = pair.get("dexId") or "-"
    pair_url = pair.get("url") or "-"
    quote_symbol = quote_token.get("symbol") or "-"
    boost_total = to_float(boost_info.get("totalAmount"))

    fdv_liquidity_ratio = (fdv / liquidity_usd) if liquidity_usd > 0 and fdv > 0 else 0.0
    volume_liquidity_ratio = (volume_24h / liquidity_usd) if liquidity_usd > 0 else 0.0

    score = 100
    reasons: List[str] = []

    # Dex 구조 분석
    if liquidity_usd < 250_000:
        score -= 12
        reasons.append("유동성이 다소 낮음")
    else:
        reasons.append("유동성이 양호")

    if volume_24h < 150_000:
        score -= 10
        reasons.append("24h 거래량이 보통 이하")
    else:
        reasons.append("거래량이 양호")

    if fdv_liquidity_ratio > 15:
        score -= 18
        reasons.append("FDV 대비 유동성 부담")
    elif fdv_liquidity_ratio > 8:
        score -= 8
        reasons.append("FDV/유동성 구조 보통")
    else:
        reasons.append("FDV/유동성 구조 양호")

    if volume_liquidity_ratio < 0.25:
        score -= 10
        reasons.append("거래 회전이 낮음")
    elif volume_liquidity_ratio > 3.0:
        score -= 6
        reasons.append("과열 거래 가능성")
    else:
        reasons.append("거래 회전이 무난")

    if age_hours < 168:
        score -= 12
        reasons.append("상대적으로 신생")
    else:
        reasons.append("최소 생존 기간 통과")

    if boost_total > 0:
        score -= 4
        reasons.append("Boost 노출 기반")

    # DefiLlama 분석
    llama_name = "-"
    llama_tvl = None
    llama_category = "-"
    llama_chain_count = 0

    if llama_protocol:
        llama_name = llama_protocol.get("name") or "-"
        llama_tvl = to_float(llama_protocol.get("tvl"), None)
        llama_category = llama_protocol.get("category") or "-"
        llama_chain_count = len(llama_protocol.get("chains") or [])

        if llama_tvl is None or llama_tvl <= 0:
            score -= 18
            reasons.append("TVL 확인 어려움")
        elif llama_tvl < 5_000_000:
            score -= 10
            reasons.append("TVL이 작음")
        elif llama_tvl < 20_000_000:
            score -= 4
            reasons.append("TVL이 보통")
        else:
            reasons.append("TVL이 양호")

        # TVL 대비 DEX 측 유동성 비교
        if llama_tvl and liquidity_usd > 0:
            tvl_liq_ratio = llama_tvl / liquidity_usd
            if tvl_liq_ratio > 100:
                reasons.append("TVL 규모가 큼")
            elif tvl_liq_ratio < 5:
                score -= 5
                reasons.append("TVL 대비 시장 반응 약함")

        # 카테고리 가점
        if llama_category.lower() in {"dexes", "lending", "yield", "derivatives", "bridge"}:
            score += 3
            reasons.append("주요 DeFi 카테고리")
    else:
        score -= 20
        reasons.append("DefiLlama 매칭 실패")

    if score >= 82:
        grade = "관심"
        verdict = "진짜 디파이 후보로 추가 검토 가치 있음"
    elif score >= 65:
        grade = "관찰"
        verdict = "시장 구조는 무난하나 추가 검증 필요"
    else:
        grade = "위험"
        verdict = "광고성/초기 과열 또는 실체 부족 가능성"

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
        "quote_symbol": quote_symbol,
        "fdv_liquidity_ratio": fdv_liquidity_ratio,
        "volume_liquidity_ratio": volume_liquidity_ratio,
        "score": max(score, 0),
        "grade": grade,
        "verdict": verdict,
        "reasons": reasons[:6],
        "llama_name": llama_name,
        "llama_tvl": llama_tvl,
        "llama_category": llama_category,
        "llama_chain_count": llama_chain_count,
    }


def build_message() -> str:
    boosted_tokens = get_top_boosted_tokens(limit=20)
    protocols = get_llama_protocols()
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

        base_token = best_pair.get("baseToken") or {}
        token_name = base_token.get("name") or ""
        token_symbol = base_token.get("symbol") or ""
        chain = (best_pair.get("chainId") or "").lower()

        llama_protocol = match_llama_protocol(
            token_name=token_name,
            token_symbol=token_symbol,
            chain_id=chain,
            protocols=protocols,
        )

        analyzed = analyze_pair(best_pair, token, llama_protocol)
        analyzed_projects.append(analyzed)

    analyzed_projects.sort(key=lambda x: x["score"], reverse=True)
    final_projects = analyzed_projects[:3]

    if not final_projects:
        return (
            "[오늘의 진짜 디파이 투자 분석 결과]\n\n"
            "조건을 통과한 프로젝트가 없습니다.\n"
            "- 유동성 부족\n"
            "- 거래량 부족\n"
            "- 너무 신생\n"
            "- FDV 과열\n"
            "- DefiLlama 실체 매칭 실패\n"
            "중 하나로 제외되었을 가능성이 큽니다."
        )

    lines: List[str] = []
    lines.append("[오늘의 진짜 디파이 투자 분석 결과]")
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
        lines.append(f"- DefiLlama: {item['llama_name']} / TVL: {fmt_num(item['llama_tvl'])}")
        lines.append(f"- 카테고리: {item['llama_category']} / 멀티체인 수: {item['llama_chain_count']}")
        lines.append(f"- 핵심 이유: {', '.join(item['reasons'])}")
        lines.append(f"- 페어 링크: {item['pair_url']}")
        lines.append("")

    lines.append("주의: 이 버전은 Dexscreener + DefiLlama 기반 분석입니다.")
    lines.append("Revenue / Fees / Holder / LP Lock / 팀 검증은 다음 단계에서 더 붙일 수 있습니다.")
    return "\n".join(lines)


def main() -> None:
    message = build_message()
    send_telegram_message(message)


if __name__ == "__main__":
    main()
