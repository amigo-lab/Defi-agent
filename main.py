import os
import json
import requests
from typing import Any, Dict, List, Optional

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

LLAMA_PROTOCOLS_URL = "https://api.llama.fi/protocols"
DEX_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search"
GECKO_API_BASE = "https://api.geckoterminal.com/api/v2"
HONEYPOT_CHECK_URL = "https://api.honeypot.is/v2/IsHoneypot"
HONEYPOT_TOP_HOLDERS_URL = "https://api.honeypot.is/v2/TopHolders"

STATE_FILE = "state.json"

ALLOWED_CHAINS = {
    "ethereum",
    "arbitrum",
    "base",
    "polygon",
    "bsc",
    "optimism",
    "avalanche",
}

EVM_CHAINS_FOR_SECURITY = {
    "ethereum",
    "arbitrum",
    "base",
    "polygon",
    "bsc",
    "optimism",
    "avalanche",
}

CHAIN_NAME_TO_ID = {
    "ethereum": 1,
    "bsc": 56,
    "polygon": 137,
    "avalanche": 43114,
    "arbitrum": 42161,
    "optimism": 10,
    "base": 8453,
}

GECKO_NETWORK_IDS = {
    "polygon": "polygon_pos",
    "bsc": "bsc",
}

ALLOWED_CATEGORIES = {
    "Dexes",
    "Lending",
    "Yield",
    "Derivatives",
    "Bridge",
    "CDP",
    "RWA",
}

BAD_KEYWORDS = [
    "doge", "inu", "baby", "banana", "pepe", "elon", "cat", "shib", "meme"
]

EXCLUDED_MAJOR_OR_STABLE_SYMBOLS = {
    "USDT", "USDC", "USDC.E", "USDT.E", "USDT0", "DAI", "BUSD", "FDUSD", "TUSD",
    "USDP", "USDD", "MAI",
    "BTC", "WBTC", "BTCB",
    "ETH", "WETH",
    "BNB", "WBNB",
    "MATIC", "WMATIC", "POL", "WPOL",
}

REAL_EARNER_MIN_FEES_30D = 50_000
REAL_EARNER_MIN_REVENUE_30D = 10_000
REAL_EARNER_MIN_LIQUIDITY = 150_000
REAL_EARNER_MAX_FDV_LIQ_RATIO = 30

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DeFiAgentBot/9.0)",
    "Accept": "application/json",
}

MONITOR_GECKO_PAGES = 2


def http_get_json(url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 30) -> Any:
    r = requests.get(url, params=params, headers=REQUEST_HEADERS, timeout=timeout)
    if r.status_code == 400:
        return {}
    r.raise_for_status()
    return r.json()


def send_telegram_message(text: str) -> None:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        raise ValueError("TG_BOT_TOKEN 또는 TG_CHAT_ID가 없습니다.")
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text}
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()


def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_FILE):
        return {"chain_leaders": {}, "top_projects": []}
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state: Dict[str, Any]) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def to_float(value: Any, default: Optional[float] = 0.0) -> Optional[float]:
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
        return f"{num / 1_000_000_000:.2f}B"
    if num >= 1_000_000:
        return f"{num / 1_000_000:.2f}M"
    if num >= 1_000:
        return f"{num / 1_000:.2f}K"
    if num >= 1:
        return f"{num:.2f}"
    return f"{num:.6f}"


def normalize_name(text: str) -> str:
    return "".join(ch.lower() for ch in (text or "") if ch.isalnum())


def is_bad_name(name: str) -> bool:
    n = (name or "").lower()
    return any(k in n for k in BAD_KEYWORDS)


def token_symbol(token: Dict[str, Any]) -> str:
    return (token.get("symbol") or "").strip().upper()


def token_name(token: Dict[str, Any]) -> str:
    return (token.get("name") or "").strip()


def get_pair_uid(pair: Dict[str, Any]) -> str:
    chain_id = (pair.get("chainId") or "").lower()
    pair_address = (pair.get("pairAddress") or "").lower()
    pair_url = (pair.get("url") or "").lower()

    if pair_address:
        return f"{chain_id}:{pair_address}"
    if pair_url:
        return f"{chain_id}:{pair_url}"

    base = pair.get("baseToken") or {}
    quote = pair.get("quoteToken") or {}
    return f"{chain_id}:{token_symbol(base)}:{token_symbol(quote)}"


def get_llama_protocols() -> List[Dict[str, Any]]:
    data = http_get_json(LLAMA_PROTOCOLS_URL, timeout=30)
    if not isinstance(data, list):
        raise ValueError("DefiLlama protocols 응답 형식 오류")
    return data


def search_dex_pairs(query: str) -> List[Dict[str, Any]]:
    query = (query or "").strip()
    if not query:
        return []
    try:
        data = http_get_json(DEX_SEARCH_URL, params={"q": query}, timeout=30)
        return data.get("pairs", []) or []
    except Exception:
        return []


def filter_llama_protocols(protocols: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    result = []

    for p in protocols:
        category = p.get("category")
        tvl = to_float(p.get("tvl"), 0.0) or 0.0
        chains = [c.lower() for c in (p.get("chains") or [])]

        if category not in ALLOWED_CATEGORIES:
            continue
        if tvl < 10_000_000:
            continue
        if not any(c in ALLOWED_CHAINS for c in chains):
            continue

        result.append(p)

    return sorted(result, key=lambda x: to_float(x.get("tvl"), 0.0) or 0.0, reverse=True)


def match_protocol_from_pairs(protocol: Dict[str, Any], pairs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    protocol_name = normalize_name(protocol.get("name") or "")
    protocol_symbol = normalize_name(protocol.get("symbol") or "")

    if not pairs:
        return None

    best_pair = None
    best_score = -1.0

    for pair in pairs:
        base = pair.get("baseToken") or {}
        quote = pair.get("quoteToken") or {}

        base_name = normalize_name(base.get("name") or "")
        base_symbol = normalize_name(base.get("symbol") or "")
        quote_name = normalize_name(quote.get("name") or "")
        quote_symbol = normalize_name(quote.get("symbol") or "")

        liquidity_usd = to_float((pair.get("liquidity") or {}).get("usd"), 0.0) or 0.0
        volume_24h = to_float((pair.get("volume") or {}).get("h24"), 0.0) or 0.0

        score = 0.0

        if protocol_name and protocol_name in {base_name, quote_name}:
            score += 12
        if protocol_symbol and protocol_symbol in {base_symbol, quote_symbol}:
            score += 8
        if protocol_name and protocol_name in base_name:
            score += 4
        if protocol_name and protocol_name in quote_name:
            score += 2

        score += min(liquidity_usd / 1_000_000, 5)
        score += min(volume_24h / 1_000_000, 3)

        if score > best_score:
            best_score = score
            best_pair = pair

    return best_pair


def choose_best_pair_for_protocol(protocol: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    name = (protocol.get("name") or "").strip()
    symbol = (protocol.get("symbol") or "").strip()

    queries = []
    if name:
        queries.append(name)
    if symbol and symbol.lower() != name.lower():
        queries.append(symbol)

    if not queries:
        return None

    all_pairs: List[Dict[str, Any]] = []
    seen = set()

    for q in queries:
        for pair in search_dex_pairs(q):
            uid = get_pair_uid(pair)
            if uid in seen:
                continue
            seen.add(uid)
            all_pairs.append(pair)

    allowed_protocol_chains = {c.lower() for c in (protocol.get("chains") or [])}
    filtered_pairs = []

    for pair in all_pairs:
        chain_id = (pair.get("chainId") or "").lower()
        if chain_id not in ALLOWED_CHAINS:
            continue
        if allowed_protocol_chains and chain_id not in allowed_protocol_chains:
            continue

        liquidity_usd = to_float((pair.get("liquidity") or {}).get("usd"), 0.0) or 0.0
        volume_24h = to_float((pair.get("volume") or {}).get("h24"), 0.0) or 0.0

        if liquidity_usd < 100_000:
            continue
        if volume_24h < 50_000:
            continue

        filtered_pairs.append(pair)

    return match_protocol_from_pairs(protocol, filtered_pairs)


def get_fee_revenue_metrics(protocol: Dict[str, Any]) -> Dict[str, Optional[float]]:
    return {
        "fees_24h": to_float(protocol.get("fees24h"), None),
        "fees_7d": to_float(protocol.get("fees7d"), None),
        "fees_30d": to_float(protocol.get("fees30d"), None),
        "revenue_24h": to_float(protocol.get("revenue24h"), None),
        "revenue_7d": to_float(protocol.get("revenue7d"), None),
        "revenue_30d": to_float(protocol.get("revenue30d"), None),
    }


def passes_profitability_filter(protocol: Dict[str, Any], pair: Dict[str, Any]) -> bool:
    liquidity_usd = to_float((pair.get("liquidity") or {}).get("usd"), 0.0) or 0.0
    fdv = to_float(pair.get("fdv"), 0.0) or 0.0
    volume_24h = to_float((pair.get("volume") or {}).get("h24"), 0.0) or 0.0
    fdv_liquidity_ratio = (fdv / liquidity_usd) if liquidity_usd > 0 and fdv > 0 else 0.0

    if liquidity_usd < 100_000:
        return False
    if volume_24h < 50_000:
        return False
    if fdv_liquidity_ratio > 40:
        return False
    return True


def get_honeypot_check(chain: str, token_address: str) -> Dict[str, Any]:
    if chain not in EVM_CHAINS_FOR_SECURITY or not token_address:
        return {}

    chain_id = CHAIN_NAME_TO_ID.get(chain)
    if not chain_id:
        return {}

    try:
        data = http_get_json(
            HONEYPOT_CHECK_URL,
            params={"chainID": chain_id, "address": token_address},
            timeout=20,
        )
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def get_top_holders(chain: str, token_address: str) -> List[Dict[str, Any]]:
    if chain not in EVM_CHAINS_FOR_SECURITY or not token_address:
        return []

    chain_id = CHAIN_NAME_TO_ID.get(chain)
    if not chain_id:
        return []

    try:
        data = http_get_json(
            HONEYPOT_TOP_HOLDERS_URL,
            params={"chainID": chain_id, "address": token_address},
            timeout=20,
        )
        holders = data.get("holders", []) if isinstance(data, dict) else []
        return holders if isinstance(holders, list) else []
    except Exception:
        return []


def extract_security_signals(pair: Dict[str, Any]) -> Dict[str, Any]:
    chain = (pair.get("chainId") or "").lower()
    base = pair.get("baseToken") or {}
    token_address = base.get("address") or ""

    hp = get_honeypot_check(chain, token_address)
    holders = get_top_holders(chain, token_address)

    is_honeypot = hp.get("honeypotResult", {}).get("isHoneypot")
    buy_tax = to_float(hp.get("simulationResult", {}).get("buyTax"), None)
    sell_tax = to_float(hp.get("simulationResult", {}).get("sellTax"), None)
    transfer_tax = to_float(hp.get("simulationResult", {}).get("transferTax"), None)
    can_buy = hp.get("simulationResult", {}).get("buyGas") is not None
    can_sell = hp.get("simulationResult", {}).get("sellGas") is not None

    top1_pct = None
    top5_pct = None
    if holders:
        percents = []
        for h in holders[:5]:
            pct = to_float(h.get("percentage"), None)
            if pct is not None:
                percents.append(pct)
        if percents:
            top1_pct = percents[0]
            top5_pct = sum(percents)

    risk_flags = []
    if is_honeypot is True:
        risk_flags.append("허니팟 의심")
    if sell_tax is not None and sell_tax > 20:
        risk_flags.append("매도세금 높음")
    if buy_tax is not None and buy_tax > 20:
        risk_flags.append("매수세금 높음")
    if transfer_tax is not None and transfer_tax > 20:
        risk_flags.append("전송세금 높음")
    if can_sell is False:
        risk_flags.append("매도 불가 의심")
    if top1_pct is not None and top1_pct > 20:
        risk_flags.append("상위1홀더 집중")
    if top5_pct is not None and top5_pct > 50:
        risk_flags.append("상위5홀더 집중")

    return {
        "is_honeypot": is_honeypot,
        "buy_tax": buy_tax,
        "sell_tax": sell_tax,
        "transfer_tax": transfer_tax,
        "can_buy": can_buy,
        "can_sell": can_sell,
        "top1_pct": top1_pct,
        "top5_pct": top5_pct,
        "risk_flags": risk_flags,
    }


def analyze_project(protocol: Dict[str, Any], pair: Dict[str, Any]) -> Dict[str, Any]:
    liquidity_usd = to_float((pair.get("liquidity") or {}).get("usd"), 0.0) or 0.0
    volume_24h = to_float((pair.get("volume") or {}).get("h24"), 0.0) or 0.0
    fdv = to_float(pair.get("fdv"), 0.0) or 0.0
    market_cap = to_float(pair.get("marketCap"), 0.0) or 0.0
    price_usd = to_float(pair.get("priceUsd"), None)

    tvl = to_float(protocol.get("tvl"), 0.0) or 0.0
    category = protocol.get("category") or "-"
    chain_id = (pair.get("chainId") or "").lower()
    dex_id = pair.get("dexId") or "-"
    pair_url = pair.get("url") or "-"

    metrics = get_fee_revenue_metrics(protocol)
    fees_24h = metrics["fees_24h"]
    fees_7d = metrics["fees_7d"]
    fees_30d = metrics["fees_30d"]
    revenue_24h = metrics["revenue_24h"]
    revenue_7d = metrics["revenue_7d"]
    revenue_30d = metrics["revenue_30d"]

    security = extract_security_signals(pair)

    fdv_liquidity_ratio = (fdv / liquidity_usd) if liquidity_usd > 0 and fdv > 0 else 0.0
    volume_liquidity_ratio = (volume_24h / liquidity_usd) if liquidity_usd > 0 else 0.0
    tvl_liquidity_ratio = (tvl / liquidity_usd) if liquidity_usd > 0 and tvl > 0 else 0.0
    fee_tvl_ratio_30d = (fees_30d / tvl) if fees_30d and tvl > 0 else None
    revenue_tvl_ratio_30d = (revenue_30d / tvl) if revenue_30d and tvl > 0 else None

    score = 100
    reasons = []

    if tvl >= 100_000_000:
        reasons.append("TVL이 매우 큼")
    elif tvl >= 20_000_000:
        reasons.append("TVL이 양호")
    else:
        score -= 12
        reasons.append("TVL이 크지 않음")

    if liquidity_usd >= 500_000:
        reasons.append("유동성이 양호")
    elif liquidity_usd >= 200_000:
        reasons.append("유동성이 무난")
    else:
        score -= 12
        reasons.append("유동성이 낮음")

    if volume_24h >= 500_000:
        reasons.append("거래량이 강함")
    elif volume_24h >= 150_000:
        reasons.append("거래량이 무난")
    else:
        score -= 10
        reasons.append("거래량이 낮음")

    if fdv_liquidity_ratio > 30:
        score -= 30
        reasons.append("FDV 대비 유동성 과열")
    elif fdv_liquidity_ratio > 20:
        score -= 20
        reasons.append("FDV 대비 유동성 부담")
    elif fdv_liquidity_ratio > 10:
        score -= 10
        reasons.append("FDV/유동성 구조 보통")
    else:
        reasons.append("FDV/유동성 구조 양호")

    if volume_liquidity_ratio < 0.2:
        score -= 8
        reasons.append("거래 회전이 약함")
    elif volume_liquidity_ratio > 3:
        score -= 4
        reasons.append("과열 거래 가능성")
    else:
        reasons.append("거래 회전이 무난")

    if tvl_liquidity_ratio < 5:
        score -= 8
        reasons.append("TVL 대비 시장 반응 약함")
    else:
        reasons.append("TVL 대비 시장 반응 무난")

    if fees_30d is None:
        score -= 15
        reasons.append("Fees 데이터 부족")
    elif fees_30d >= 5_000_000:
        score += 8
        reasons.append("Fees 30d가 강함")
    elif fees_30d >= 500_000:
        score += 3
        reasons.append("Fees 30d가 무난")
    elif fees_30d >= 100_000:
        reasons.append("Fees 30d가 약하지만 존재")
    else:
        score -= 12
        reasons.append("Fees 30d가 매우 약함")

    if revenue_30d is None:
        score -= 18
        reasons.append("Revenue 데이터 부족")
    elif revenue_30d >= 1_000_000:
        score += 10
        reasons.append("Revenue 30d가 강함")
    elif revenue_30d >= 100_000:
        score += 4
        reasons.append("Revenue 30d가 무난")
    elif revenue_30d >= 20_000:
        reasons.append("Revenue 30d가 낮지만 존재")
    else:
        score -= 14
        reasons.append("Revenue 30d가 매우 약함")

    if fee_tvl_ratio_30d is not None:
        if fee_tvl_ratio_30d >= 0.03:
            score += 6
            reasons.append("TVL 대비 Fees 효율 높음")
        elif fee_tvl_ratio_30d < 0.005:
            score -= 6
            reasons.append("TVL 대비 Fees 효율 낮음")

    if revenue_tvl_ratio_30d is not None:
        if revenue_tvl_ratio_30d >= 0.01:
            score += 6
            reasons.append("TVL 대비 Revenue 효율 높음")
        elif revenue_tvl_ratio_30d < 0.002:
            score -= 6
            reasons.append("TVL 대비 Revenue 효율 낮음")

    if category in {"Dexes", "Lending", "Derivatives", "Bridge"}:
        score += 3
        reasons.append("주요 DeFi 카테고리")

    if security["is_honeypot"] is True:
        score -= 40
        reasons.append("허니팟 의심")
    if security["sell_tax"] is not None and security["sell_tax"] > 20:
        score -= 18
        reasons.append("매도세금 높음")
    if security["buy_tax"] is not None and security["buy_tax"] > 20:
        score -= 10
        reasons.append("매수세금 높음")
    if security["can_sell"] is False:
        score -= 35
        reasons.append("매도 불가 의심")
    if security["top1_pct"] is not None and security["top1_pct"] > 20:
        score -= 12
        reasons.append("상위1홀더 집중")
    if security["top5_pct"] is not None and security["top5_pct"] > 50:
        score -= 10
        reasons.append("상위5홀더 집중")

    if score >= 88:
        grade = "관심"
        verdict = "수익성+보안 관점에서 우선 검토 가치 높음"
    elif score >= 72:
        grade = "관찰"
        verdict = "기초체력은 있으나 추가 검증 필요"
    else:
        grade = "위험"
        verdict = "시장 구조/수익성/보안 중 하나 이상 부담"

    return {
        "name": protocol.get("name") or "-",
        "symbol": protocol.get("symbol") or "-",
        "chain": chain_id,
        "category": category,
        "tvl": tvl,
        "price_usd": price_usd,
        "liquidity_usd": liquidity_usd,
        "volume_24h": volume_24h,
        "fdv": fdv,
        "market_cap": market_cap,
        "fdv_liquidity_ratio": fdv_liquidity_ratio,
        "volume_liquidity_ratio": volume_liquidity_ratio,
        "tvl_liquidity_ratio": tvl_liquidity_ratio,
        "fees_24h": fees_24h,
        "fees_7d": fees_7d,
        "fees_30d": fees_30d,
        "revenue_24h": revenue_24h,
        "revenue_7d": revenue_7d,
        "revenue_30d": revenue_30d,
        "fee_tvl_ratio_30d": fee_tvl_ratio_30d,
        "revenue_tvl_ratio_30d": revenue_tvl_ratio_30d,
        "dex_id": dex_id,
        "pair_url": pair_url,
        "score": max(score, 0),
        "grade": grade,
        "verdict": verdict,
        "reasons": reasons[:8],
        "security": security,
    }


def is_real_earner(project: Dict[str, Any]) -> bool:
    fees_30d = to_float(project.get("fees_30d"), None)
    revenue_30d = to_float(project.get("revenue_30d"), None)
    liquidity_usd = to_float(project.get("liquidity_usd"), 0.0) or 0.0
    fdv_liquidity_ratio = to_float(project.get("fdv_liquidity_ratio"), 0.0) or 0.0
    security = project.get("security", {})

    if fees_30d is None or revenue_30d is None:
        return False
    if fees_30d < REAL_EARNER_MIN_FEES_30D:
        return False
    if revenue_30d < REAL_EARNER_MIN_REVENUE_30D:
        return False
    if liquidity_usd < REAL_EARNER_MIN_LIQUIDITY:
        return False
    if fdv_liquidity_ratio > REAL_EARNER_MAX_FDV_LIQ_RATIO:
        return False
    if security.get("is_honeypot") is True:
        return False
    if security.get("can_sell") is False:
        return False
    return True


def build_chain_leaders(projects: List[Dict[str, Any]]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    required_chains = ["ethereum", "arbitrum", "base", "bsc", "polygon"]
    leaders: Dict[str, Dict[str, Dict[str, Any]]] = {chain: {} for chain in required_chains}

    for p in projects:
        chain = p["chain"]
        if chain not in leaders:
            leaders[chain] = {}

        if "liquidity" not in leaders[chain] or p["liquidity_usd"] > leaders[chain]["liquidity"]["liquidity_usd"]:
            leaders[chain]["liquidity"] = p

        if "volume" not in leaders[chain] or p["volume_24h"] > leaders[chain]["volume"]["volume_24h"]:
            leaders[chain]["volume"] = p

    return leaders


def compare_leaders(prev_state: Dict[str, Any], current_leaders: Dict[str, Dict[str, Dict[str, Any]]]) -> List[str]:
    messages = []
    prev_leaders = prev_state.get("chain_leaders", {})

    for chain in ["ethereum", "arbitrum", "base", "bsc", "polygon"]:
        data = current_leaders.get(chain, {})
        prev_chain = prev_leaders.get(chain, {})

        current_liq_name = data.get("liquidity", {}).get("name", "-")
        prev_liq_name = prev_chain.get("liquidity", {}).get("name", "-")
        liq_status = "유지" if current_liq_name == prev_liq_name else f"변경 ({prev_liq_name} → {current_liq_name})"

        current_vol_name = data.get("volume", {}).get("name", "-")
        prev_vol_name = prev_chain.get("volume", {}).get("name", "-")
        vol_status = "유지" if current_vol_name == prev_vol_name else f"변경 ({prev_vol_name} → {current_vol_name})"

        messages.append(f"- {chain} 유동성 1위: {liq_status}")
        messages.append(f"- {chain} 거래량 1위: {vol_status}")

    return messages


def is_excluded_pair(base_symbol: str, quote_symbol: str) -> bool:
    return (
        base_symbol in EXCLUDED_MAJOR_OR_STABLE_SYMBOLS
        and quote_symbol in EXCLUDED_MAJOR_OR_STABLE_SYMBOLS
    )


def gecko_build_included_map(included: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    result = {}
    for item in included or []:
        item_id = item.get("id")
        if item_id:
            result[item_id] = item
    return result


def gecko_fetch_top_pools_page(network_id: str, page: int = 1, order: Optional[str] = None) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "page": page,
        "include": "base_token,quote_token,dex",
    }
    if order:
        params["order"] = order

    url = f"{GECKO_API_BASE}/networks/{network_id}/pools"
    data = http_get_json(url, params=params, timeout=30)

    if not isinstance(data, dict):
        return []

    raw_pools = data.get("data", []) or []
    included_map = gecko_build_included_map(data.get("included", []) or [])
    parsed = []

    for pool in raw_pools:
        attrs = pool.get("attributes", {}) or {}
        rel = pool.get("relationships", {}) or {}

        base_rel = ((rel.get("base_token") or {}).get("data") or {}).get("id")
        quote_rel = ((rel.get("quote_token") or {}).get("data") or {}).get("id")
        dex_rel = ((rel.get("dex") or {}).get("data") or {}).get("id")

        base_item = included_map.get(base_rel, {})
        quote_item = included_map.get(quote_rel, {})
        dex_item = included_map.get(dex_rel, {})

        base_attrs = base_item.get("attributes", {}) or {}
        quote_attrs = quote_item.get("attributes", {}) or {}
        dex_attrs = dex_item.get("attributes", {}) or {}

        base_symbol = (base_attrs.get("symbol") or "").upper()
        quote_symbol = (quote_attrs.get("symbol") or "").upper()
        base_name = base_attrs.get("name") or base_symbol or "?"
        quote_name = quote_attrs.get("name") or quote_symbol or "?"
        dex_name = dex_attrs.get("name") or dex_rel or "-"

        pool_address = (attrs.get("address") or "").lower()
        reserve_in_usd = to_float(attrs.get("reserve_in_usd"), 0.0) or 0.0
        volume_h24 = to_float((attrs.get("volume_usd") or {}).get("h24"), 0.0) or 0.0
        txns_h24 = (attrs.get("transactions") or {}).get("h24", {}) or {}
        buys_h24 = int(to_float(txns_h24.get("buys"), 0) or 0)
        sells_h24 = int(to_float(txns_h24.get("sells"), 0) or 0)

        parsed.append({
            "pool_address": pool_address,
            "label": attrs.get("name") or f"{base_symbol} / {quote_symbol}",
            "base_symbol": base_symbol,
            "quote_symbol": quote_symbol,
            "base_name": base_name,
            "quote_name": quote_name,
            "liquidity_usd": reserve_in_usd,
            "volume_24h": volume_h24,
            "fdv": to_float(attrs.get("fdv_usd"), 0.0) or 0.0,
            "market_cap": to_float(attrs.get("market_cap_usd"), 0.0) or 0.0,
            "price_usd": to_float(attrs.get("base_token_price_usd"), None),
            "pair_url": f"https://www.geckoterminal.com/{network_id}/pools/{pool_address}" if pool_address else "-",
            "dex_id": str(dex_name),
            "buys_h24": buys_h24,
            "sells_h24": sells_h24,
        })

    return parsed


def gecko_monitor_candidate(pool: Dict[str, Any]) -> bool:
    base_name = pool.get("base_name") or ""
    quote_name = pool.get("quote_name") or ""
    base_symbol = (pool.get("base_symbol") or "").upper()
    quote_symbol = (pool.get("quote_symbol") or "").upper()

    if is_bad_name(base_name) or is_bad_name(quote_name):
        return False

    if is_excluded_pair(base_symbol, quote_symbol):
        return False

    liquidity_usd = to_float(pool.get("liquidity_usd"), 0.0) or 0.0
    volume_24h = to_float(pool.get("volume_24h"), 0.0) or 0.0
    buys_h24 = int(pool.get("buys_h24") or 0)
    sells_h24 = int(pool.get("sells_h24") or 0)

    if liquidity_usd < 50_000:
        return False
    if volume_24h < 20_000:
        return False
    if (buys_h24 + sells_h24) < 20:
        return False

    return True


def build_chain_top3_from_gecko(chain_name: str) -> Dict[str, List[Dict[str, Any]]]:
    network_id = GECKO_NETWORK_IDS.get(chain_name)
    if not network_id:
        return {"liquidity": [], "volume": []}

    pools_map: Dict[str, Dict[str, Any]] = {}

    for page in range(1, MONITOR_GECKO_PAGES + 1):
        for order in [None, "h24_volume_usd_desc"]:
            try:
                page_pools = gecko_fetch_top_pools_page(network_id, page=page, order=order)
            except Exception:
                page_pools = []

            for pool in page_pools:
                if not gecko_monitor_candidate(pool):
                    continue

                uid = pool.get("pool_address") or pool.get("pair_url")
                if not uid:
                    continue

                prev = pools_map.get(uid)
                if prev is None:
                    pools_map[uid] = pool
                else:
                    if (
                        pool["liquidity_usd"] > prev["liquidity_usd"]
                        or pool["volume_24h"] > prev["volume_24h"]
                    ):
                        pools_map[uid] = pool

    pools = list(pools_map.values())

    top_liquidity = sorted(
        pools,
        key=lambda x: (x["liquidity_usd"], x["volume_24h"]),
        reverse=True
    )[:3]

    top_volume = sorted(
        pools,
        key=lambda x: (x["volume_24h"], x["liquidity_usd"]),
        reverse=True
    )[:3]

    return {"liquidity": top_liquidity, "volume": top_volume}


def format_monitor_line(rank: int, pool: Dict[str, Any], mode: str) -> str:
    label = pool.get("label") or "-"
    dex_id = pool.get("dex_id") or "-"
    pair_url = pool.get("pair_url") or "-"

    if mode == "liquidity":
        return (
            f"{rank}) {label} / DEX {dex_id} / 유동성 {fmt_num(pool['liquidity_usd'])} "
            f"/ 거래량 {fmt_num(pool['volume_24h'])} / 링크 {pair_url}"
        )

    return (
        f"{rank}) {label} / DEX {dex_id} / 거래량 {fmt_num(pool['volume_24h'])} "
        f"/ 유동성 {fmt_num(pool['liquidity_usd'])} / 링크 {pair_url}"
    )


def build_message(projects: List[Dict[str, Any]], leaders: Dict[str, Dict[str, Dict[str, Any]]], changes: List[str]) -> str:
    safe_projects = [x for x in projects if x["grade"] in {"관심", "관찰"}]
    top_projects = sorted(safe_projects, key=lambda x: x["score"], reverse=True)[:3]
    real_earners = sorted(
        [p for p in projects if is_real_earner(p)],
        key=lambda x: (to_float(x.get("revenue_30d"), 0.0) or 0.0, x["score"]),
        reverse=True
    )[:3]

    lines = []
    lines.append("[오늘의 진짜 디파이 투자 분석 결과]")
    lines.append("")

    if not top_projects:
        lines.append("오늘은 투자 후보 없음")
        lines.append("")
    else:
        for idx, p in enumerate(top_projects, start=1):
            sec = p.get("security", {})
            lines.append(f"{idx}) {p['name']} ({p['symbol']})")
            lines.append(f"- 판정: {p['grade']} / 점수: {p['score']}")
            lines.append(f"- 결론: {p['verdict']}")
            lines.append(f"- 체인: {p['chain']} / 카테고리: {p['category']}")
            lines.append(f"- TVL: {fmt_num(p['tvl'])}")
            lines.append(f"- 가격: {fmt_num(p['price_usd'])} USD")
            lines.append(f"- 유동성: {fmt_num(p['liquidity_usd'])}")
            lines.append(f"- 24h 거래량: {fmt_num(p['volume_24h'])}")
            lines.append(f"- FDV: {fmt_num(p['fdv'])}")
            lines.append(f"- 시총: {fmt_num(p['market_cap'])}")
            lines.append(f"- Fees 30d: {fmt_num(p['fees_30d'])}")
            lines.append(f"- Revenue 30d: {fmt_num(p['revenue_30d'])}")
            lines.append(f"- FDV/유동성: {p['fdv_liquidity_ratio']:.2f}")
            lines.append(f"- 거래량/유동성: {p['volume_liquidity_ratio']:.2f}")
            lines.append(f"- TVL/유동성: {p['tvl_liquidity_ratio']:.2f}")
            lines.append(f"- 홀더 상위1/5 집중: {fmt_num(sec.get('top1_pct'))}% / {fmt_num(sec.get('top5_pct'))}%")
            lines.append(f"- 세금(매수/매도): {fmt_num(sec.get('buy_tax'))}% / {fmt_num(sec.get('sell_tax'))}%")
            lines.append(f"- 보안 경고: {', '.join(sec.get('risk_flags', [])) if sec.get('risk_flags') else '없음'}")
            lines.append(f"- 핵심 이유: {', '.join(p['reasons'])}")
            lines.append(f"- 링크: {p['pair_url']}")
            lines.append("")

    lines.append("[진짜 돈 버는 프로토콜 모드 TOP 3]")
    if real_earners:
        for idx, p in enumerate(real_earners, start=1):
            sec = p.get("security", {})
            lines.append(f"{idx}) {p['name']} ({p['symbol']})")
            lines.append(f"- 체인: {p['chain']} / 카테고리: {p['category']}")
            lines.append(f"- Revenue 30d: {fmt_num(p['revenue_30d'])}")
            lines.append(f"- Fees 30d: {fmt_num(p['fees_30d'])}")
            lines.append(f"- TVL: {fmt_num(p['tvl'])}")
            lines.append(f"- 유동성: {fmt_num(p['liquidity_usd'])}")
            lines.append(f"- FDV/유동성: {p['fdv_liquidity_ratio']:.2f}")
            lines.append(f"- 홀더 상위1/5 집중: {fmt_num(sec.get('top1_pct'))}% / {fmt_num(sec.get('top5_pct'))}%")
            lines.append(f"- 점수: {p['score']}")
            lines.append("")
    else:
        lines.append("- 조건 충족 프로젝트 없음")
        lines.append("")

    lines.append("[체인별 유동성 1위]")
    for chain in ["ethereum", "arbitrum", "base", "bsc", "polygon"]:
        data = leaders.get(chain, {})
        if "liquidity" in data:
            p = data["liquidity"]
            lines.append(f"- {chain}: {p['name']} / 유동성 {fmt_num(p['liquidity_usd'])}")
        else:
            lines.append(f"- {chain}: 기본 필터 통과 프로젝트 없음")

    lines.append("")
    lines.append("[체인별 거래량 1위]")
    for chain in ["ethereum", "arbitrum", "base", "bsc", "polygon"]:
        data = leaders.get(chain, {})
        if "volume" in data:
            p = data["volume"]
            lines.append(f"- {chain}: {p['name']} / 24h 거래량 {fmt_num(p['volume_24h'])}")
        else:
            lines.append(f"- {chain}: 기본 필터 통과 프로젝트 없음")

    bsc_top3 = build_chain_top3_from_gecko("bsc")
    polygon_top3 = build_chain_top3_from_gecko("polygon")

    lines.append("")
    lines.append("[BSC 별도 모니터링 - 유동성 TOP 3]")
    if bsc_top3["liquidity"]:
        for i, p in enumerate(bsc_top3["liquidity"], start=1):
            lines.append(format_monitor_line(i, p, "liquidity"))
    else:
        lines.append("- 후보 없음")

    lines.append("")
    lines.append("[BSC 별도 모니터링 - 거래량 TOP 3]")
    if bsc_top3["volume"]:
        for i, p in enumerate(bsc_top3["volume"], start=1):
            lines.append(format_monitor_line(i, p, "volume"))
    else:
        lines.append("- 후보 없음")

    lines.append("")
    lines.append("[Polygon 별도 모니터링 - 유동성 TOP 3]")
    if polygon_top3["liquidity"]:
        for i, p in enumerate(polygon_top3["liquidity"], start=1):
            lines.append(format_monitor_line(i, p, "liquidity"))
    else:
        lines.append("- 후보 없음")

    lines.append("")
    lines.append("[Polygon 별도 모니터링 - 거래량 TOP 3]")
    if polygon_top3["volume"]:
        for i, p in enumerate(polygon_top3["volume"], start=1):
            lines.append(format_monitor_line(i, p, "volume"))
    else:
        lines.append("- 후보 없음")

    lines.append("")
    lines.append("[전 실행 대비 변화]")
    if changes:
        lines.extend(changes)
    else:
        lines.append("- 비교 가능한 이전 데이터가 없습니다.")

    return "\n".join(lines)


def main() -> None:
    prev_state = load_state()
    protocols = filter_llama_protocols(get_llama_protocols())

    projects = []
    for protocol in protocols[:40]:
        best_pair = choose_best_pair_for_protocol(protocol)
        if not best_pair:
            continue
        if not passes_profitability_filter(protocol, best_pair):
            continue
        projects.append(analyze_project(protocol, best_pair))

    leaders = build_chain_leaders(projects)
    changes = compare_leaders(prev_state, leaders)

    message = build_message(projects, leaders, changes)
    send_telegram_message(message)

    new_state = {
        "chain_leaders": {
            chain: {
                k: {
                    "name": v["name"],
                    "liquidity_usd": v["liquidity_usd"],
                    "volume_24h": v["volume_24h"],
                }
                for k, v in data.items()
            }
            for chain, data in leaders.items()
        },
        "top_projects": [
            {"name": p["name"], "score": p["score"], "chain": p["chain"]}
            for p in sorted(projects, key=lambda x: x["score"], reverse=True)[:3]
        ],
    }
    save_state(new_state)


if __name__ == "__main__":
    main()
