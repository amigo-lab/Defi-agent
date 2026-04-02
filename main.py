import os
import json
import requests
from typing import Any, Dict, List, Optional, Tuple

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

STATE_FILE = "state.json"

LLAMA_PROTOCOLS_URL = "https://api.llama.fi/protocols"
DEX_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search"
DEX_TOKEN_PAIRS_URL = "https://api.dexscreener.com/token-pairs/v1/{chain_id}/{token_address}"
GECKO_POOLS_URL = "https://api.geckoterminal.com/api/v2/networks/{network}/pools?page={page}"
HONEYPOT_CHECK_URL = "https://api.honeypot.is/v2/IsHoneypot"
HONEYPOT_TOP_HOLDERS_URL = "https://api.honeypot.is/v1/TopHolders"

ALLOWED_CHAINS = {
    "ethereum",
    "arbitrum",
    "base",
    "polygon",
    "bsc",
    "optimism",
    "avalanche",
}

CHAIN_TO_GECKO = {
    "polygon": "polygon_pos",
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

EXCLUDE_TOKENS = {
    "usdt", "usdc", "dai", "busd", "usde", "fdusd", "tusd",
    "weth", "eth", "wbtc", "btc", "cbbtc",
    "wbnb", "bnb", "wpol", "matic", "sol", "wsol"
}

BAD_KEYWORDS = [
    "doge", "inu", "baby", "banana", "pepe", "elon", "cat", "shib", "meme"
]

BSC_SEARCH_TERMS = [
    "usdt", "wbnb", "cake", "floki", "btcb", "eth", "fdusd", "usdc",
    "thena", "venus", "lista", "biswap", "wombat", "aster", "slisbnb"
]

POLYGON_SEARCH_TERMS = [
    "LGNS", "QuickSwap", "Aave", "Uniswap", "Balancer", "Curve", "Sushi", "Kyber"
]

REAL_EARNER_MIN_FEES_30D = 50_000
REAL_EARNER_MIN_REVENUE_30D = 10_000
REAL_EARNER_MIN_LIQUIDITY = 150_000
REAL_EARNER_MAX_FDV_LIQ_RATIO = 30

GECKO_PAGES = 10
GECKO_MIN_LIQUIDITY = 50_000
DEX_BSC_MIN_LIQUIDITY = 50_000


def send_telegram_message(text: str) -> None:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        raise ValueError("TG_BOT_TOKEN 또는 TG_CHAT_ID가 없습니다.")

    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text}
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()


def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_FILE):
        return {
            "chain_leaders": {},
            "top_projects": [],
            "gecko_chain_snapshots": {},
            "security_snapshots": {},
            "unified_flow": {},
        }
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
        return f"{num/1_000_000_000:.2f}B"
    if num >= 1_000_000:
        return f"{num/1_000_000:.2f}M"
    if num >= 1_000:
        return f"{num/1_000:.2f}K"
    if num >= 1:
        return f"{num:.2f}"
    return f"{num:.6f}"


def pct_str(value: Any) -> str:
    v = to_float(value, None)
    if v is None:
        return "-"
    return f"{v:.2f}%"


def normalize_name(text: str) -> str:
    return "".join(ch.lower() for ch in (text or "") if ch.isalnum())


def is_bad_name(name: str) -> bool:
    n = (name or "").lower()
    return any(k in n for k in BAD_KEYWORDS)


def is_excluded_token(name: str) -> bool:
    n = normalize_name(name)
    return n in EXCLUDE_TOKENS or any(x in n for x in EXCLUDE_TOKENS)


def is_valid_token(name: str) -> bool:
    if not name:
        return False

    n = normalize_name(name)

    if n in EXCLUDE_TOKENS:
        return False
    if any(x in n for x in EXCLUDE_TOKENS):
        return False
    if is_bad_name(name):
        return False
    if len(n) < 2:
        return False

    return True


def get_llama_protocols() -> List[Dict[str, Any]]:
    r = requests.get(LLAMA_PROTOCOLS_URL, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else []


def search_dex_pairs(query: str) -> List[Dict[str, Any]]:
    query = (query or "").strip()
    if not query:
        return []

    r = requests.get(DEX_SEARCH_URL, params={"q": query}, timeout=30)
    if r.status_code == 400:
        return []
    r.raise_for_status()
    data = r.json()
    return data.get("pairs", []) or []


def get_token_pairs(chain_id: str, token_address: str) -> List[Dict[str, Any]]:
    if not chain_id or not token_address:
        return []
    url = DEX_TOKEN_PAIRS_URL.format(chain_id=chain_id, token_address=token_address)
    r = requests.get(url, timeout=30)
    if r.status_code >= 400:
        return []
    data = r.json()
    return data if isinstance(data, list) else []


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
    best_score = -1

    for pair in pairs:
        base = pair.get("baseToken") or {}
        quote = pair.get("quoteToken") or {}

        base_name = normalize_name(base.get("name") or "")
        base_symbol = normalize_name(base.get("symbol") or "")
        quote_name = normalize_name(quote.get("name") or "")
        quote_symbol = normalize_name(quote.get("symbol") or "")

        score = 0

        if protocol_name and protocol_name in {base_name, quote_name}:
            score += 10
        if protocol_symbol and protocol_symbol in {base_symbol, quote_symbol}:
            score += 6
        if protocol_name and protocol_name in base_name:
            score += 4
        if protocol_name and protocol_name in quote_name:
            score += 2

        liquidity_usd = to_float((pair.get("liquidity") or {}).get("usd"), 0.0) or 0.0
        volume_24h = to_float((pair.get("volume") or {}).get("h24"), 0.0) or 0.0
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
    for q in queries:
        all_pairs.extend(search_dex_pairs(q))

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
    if chain not in ALLOWED_CHAINS or not token_address:
        return {}

    try:
        r = requests.get(
            HONEYPOT_CHECK_URL,
            params={"chainID": chain, "address": token_address},
            timeout=20,
        )
        if r.status_code >= 400:
            return {}
        data = r.json()
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def get_top_holders(chain: str, token_address: str) -> List[Dict[str, Any]]:
    if chain not in ALLOWED_CHAINS or not token_address:
        return []

    try:
        r = requests.get(
            HONEYPOT_TOP_HOLDERS_URL,
            params={"chainID": chain, "address": token_address},
            timeout=20,
        )
        if r.status_code >= 400:
            return []
        data = r.json()
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
    sim = hp.get("simulationResult", {}) if isinstance(hp, dict) else {}

    buy_tax = to_float(sim.get("buyTax"), None)
    sell_tax = to_float(sim.get("sellTax"), None)
    transfer_tax = to_float(sim.get("transferTax"), None)
    can_buy = sim.get("buyGas") is not None
    can_sell = sim.get("sellGas") is not None

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
        "token_address": token_address,
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


def extract_primary_token_from_pool_name(pool_name: str) -> str:
    if not pool_name:
        return "-"

    clean = pool_name.split("(")[0].strip()
    parts = [x.strip() for x in clean.split("/") if x.strip()]

    if len(parts) < 2:
        return clean

    left, right = parts[0], parts[1]
    left_excluded = is_excluded_token(left)
    right_excluded = is_excluded_token(right)

    if left_excluded and not right_excluded:
        return right
    if right_excluded and not left_excluded:
        return left
    return left


def gecko_pool_to_record(pool: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    attr = pool.get("attributes", {}) or {}
    pool_name = attr.get("name") or "-"
    reserve = to_float(attr.get("reserve_in_usd"), 0.0) or 0.0
    volume_obj = attr.get("volume_usd") or {}
    volume_24h = to_float(volume_obj.get("h24"), 0.0) or 0.0
    address = attr.get("address") or ""
    created_at = attr.get("pool_created_at") or ""

    if reserve < GECKO_MIN_LIQUIDITY:
        return None

    token_name = extract_primary_token_from_pool_name(pool_name)

    return {
        "name": token_name,
        "pool_name": pool_name,
        "pool_address": address,
        "liquidity_usd": reserve,
        "volume_24h": volume_24h,
        "created_at": created_at,
    }


def fetch_gecko_chain_pools(network: str, pages: int = GECKO_PAGES) -> List[Dict[str, Any]]:
    pools: List[Dict[str, Any]] = []

    for page in range(1, pages + 1):
        url = GECKO_POOLS_URL.format(network=network, page=page)
        try:
            r = requests.get(url, timeout=20)
            if r.status_code >= 400:
                continue
            data = r.json().get("data", [])
        except Exception:
            continue

        if not isinstance(data, list):
            continue

        for pool in data:
            record = gecko_pool_to_record(pool)
            if record:
                pools.append(record)

    return pools


def merge_gecko_records_by_token(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}

    for r in records:
        key = normalize_name(r["name"])
        if not key:
            continue

        if key not in merged:
            merged[key] = {
                "name": r["name"],
                "liquidity_usd": 0.0,
                "volume_24h": 0.0,
            }

        merged[key]["liquidity_usd"] += r["liquidity_usd"]
        merged[key]["volume_24h"] += r["volume_24h"]

    return list(merged.values())


def compute_flow_signal(chain: str, token: Dict[str, Any], prev_state: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    snap = prev_state.get("gecko_chain_snapshots", {}).get(chain, {})
    prev = snap.get(normalize_name(token["name"]), {})

    prev_liq = to_float(prev.get("liquidity_usd"), 0.0) or 0.0
    prev_vol = to_float(prev.get("volume_24h"), 0.0) or 0.0
    cur_liq = token["liquidity_usd"]
    cur_vol = token["volume_24h"]

    liq_change_pct = ((cur_liq - prev_liq) / prev_liq * 100) if prev_liq > 0 else None
    vol_change_pct = ((cur_vol - prev_vol) / prev_vol * 100) if prev_vol > 0 else None

    signals = []
    if cur_vol >= 5_000_000 and cur_liq >= 5_000_000:
        signals.append("🔥 강한 자금 유입 신호")
    elif cur_vol >= 1_000_000:
        signals.append("📈 거래량 강함")

    if liq_change_pct is not None and liq_change_pct >= 25:
        signals.append("💧 유동성 증가")
    if vol_change_pct is not None and vol_change_pct >= 50:
        signals.append("🚀 거래량 급증")

    return " / ".join(signals), {
        "prev_liquidity_usd": prev_liq,
        "prev_volume_24h": prev_vol,
        "liq_change_pct": liq_change_pct,
        "vol_change_pct": vol_change_pct,
    }


def build_chain_top3_gecko(chain_key: str, prev_state: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    network = CHAIN_TO_GECKO[chain_key]
    raw_pools = fetch_gecko_chain_pools(network, pages=GECKO_PAGES)

    filtered_pools = []
    for pool in raw_pools:
        if is_bad_name(pool["pool_name"]):
            continue
        filtered_pools.append(pool)

    top_liquidity_pools = sorted(
        filtered_pools,
        key=lambda x: x["liquidity_usd"],
        reverse=True,
    )[:3]

    top_volume_pools = sorted(
        filtered_pools,
        key=lambda x: x["volume_24h"],
        reverse=True,
    )[:3]

    merged_tokens = merge_gecko_records_by_token(filtered_pools)

    token_records = []
    for token in merged_tokens:
        if not is_valid_token(token["name"]):
            continue

        signal_text, meta = compute_flow_signal(chain_key, token, prev_state)
        token_records.append({**token, "signal": signal_text, **meta})

    return {
        "liquidity": top_liquidity_pools,
        "volume": top_volume_pools,
        "all": token_records,
    }


def dex_pair_to_record(pair: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    chain_id = (pair.get("chainId") or "").lower()
    if chain_id != "bsc":
        return None

    liquidity_usd = to_float((pair.get("liquidity") or {}).get("usd"), 0.0) or 0.0
    volume_24h = to_float((pair.get("volume") or {}).get("h24"), 0.0) or 0.0

    if liquidity_usd < DEX_BSC_MIN_LIQUIDITY:
        return None

    base = pair.get("baseToken") or {}
    quote = pair.get("quoteToken") or {}
    dex_id = pair.get("dexId") or "-"
    pair_address = pair.get("pairAddress") or ""
    url = pair.get("url") or ""

    base_symbol = (base.get("symbol") or base.get("name") or "").strip()
    quote_symbol = (quote.get("symbol") or quote.get("name") or "").strip()

    if not base_symbol and not quote_symbol:
        return None

    pool_name = f"{base_symbol} / {quote_symbol}".strip(" /")
    token_name = extract_primary_token_from_pool_name(pool_name)

    return {
        "name": token_name,
        "pool_name": pool_name,
        "pool_address": pair_address,
        "liquidity_usd": liquidity_usd,
        "volume_24h": volume_24h,
        "dex_id": dex_id,
        "url": url,
    }


def build_bsc_top3_dex(prev_state: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    all_pairs: List[Dict[str, Any]] = []
    seen_addresses = set()

    for term in BSC_SEARCH_TERMS:
        try:
            pairs = search_dex_pairs(term)
        except Exception:
            continue

        for pair in pairs:
            record = dex_pair_to_record(pair)
            if not record:
                continue

            key = record["pool_address"] or f"{record['pool_name']}::{record.get('dex_id', '-')}"
            if key in seen_addresses:
                continue
            seen_addresses.add(key)

            if is_bad_name(record["pool_name"]):
                continue

            all_pairs.append(record)

    top_liquidity_pools = sorted(
        all_pairs,
        key=lambda x: x["liquidity_usd"],
        reverse=True,
    )[:3]

    top_volume_pools = sorted(
        all_pairs,
        key=lambda x: x["volume_24h"],
        reverse=True,
    )[:3]

    merged_tokens = merge_gecko_records_by_token(all_pairs)

    token_records = []
    for token in merged_tokens:
        if is_bad_name(token["name"]):
            continue
        signal_text, meta = compute_flow_signal("bsc", token, prev_state)
        token_records.append({**token, "signal": signal_text, **meta})

    return {
        "liquidity": top_liquidity_pools,
        "volume": top_volume_pools,
        "all": token_records,
    }


def collect_dex_pairs_for_unified_ranking() -> List[Dict[str, Any]]:
    terms = list(set(BSC_SEARCH_TERMS + POLYGON_SEARCH_TERMS))
    all_pairs: List[Dict[str, Any]] = []

    for term in terms:
        pairs = search_dex_pairs(term)
        for pair in pairs:
            chain = (pair.get("chainId") or "").lower()
            if chain not in {"bsc", "polygon"}:
                continue
            all_pairs.append(pair)

    return all_pairs


def build_unified_top(
    polygon_data: Dict[str, Any],
    bsc_data: Dict[str, Any],
    dex_pairs: List[Dict[str, Any]],
    prev_state: Dict[str, Any],
) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}

    for chain_data in [polygon_data, bsc_data]:
        for token in chain_data["all"]:
            name = token["name"]
            if is_bad_name(name):
                continue

            key = normalize_name(name)
            if key not in merged:
                merged[key] = {"name": name, "liq": 0.0, "vol": 0.0}

            merged[key]["liq"] += token["liquidity_usd"]
            merged[key]["vol"] += token["volume_24h"]

    for pair in dex_pairs:
        base = pair.get("baseToken") or {}
        name = base.get("symbol") or base.get("name") or ""

        if not name:
            continue
        if is_bad_name(name):
            continue
        if is_excluded_token(name):
            continue

        key = normalize_name(name)
        liq = to_float((pair.get("liquidity") or {}).get("usd"), 0.0) or 0.0
        vol = to_float((pair.get("volume") or {}).get("h24"), 0.0) or 0.0

        if liq <= 0:
            continue

        if key not in merged:
            merged[key] = {"name": name, "liq": 0.0, "vol": 0.0}

        merged[key]["liq"] += liq
        merged[key]["vol"] += vol

    prev_unified = prev_state.get("unified_flow", {})
    result = []

    for item in merged.values():
        liq = item["liq"]
        vol = item["vol"]

        if liq < 100_000:
            continue

        prev = prev_unified.get(normalize_name(item["name"]), {})
        prev_liq = to_float(prev.get("liq"), 0.0) or 0.0
        prev_vol = to_float(prev.get("vol"), 0.0) or 0.0

        liq_change_pct = ((liq - prev_liq) / prev_liq * 100) if prev_liq > 0 else None
        vol_change_pct = ((vol - prev_vol) / prev_vol * 100) if prev_vol > 0 else None

        signal = ""
        if vol >= 5_000_000 and liq >= 5_000_000:
            signal = "🔥 대규모 자금 유입"
        elif vol >= 1_000_000:
            signal = "📈 거래량 강함"

        if liq_change_pct is not None and liq_change_pct >= 25:
            signal = (signal + " / 💧 유동성 증가").strip(" /")
        if vol_change_pct is not None and vol_change_pct >= 50:
            signal = (signal + " / 🚀 거래량 급증").strip(" /")

        result.append({
            "name": item["name"],
            "liquidity_usd": liq,
            "volume_24h": vol,
            "signal": signal,
        })

    result.sort(key=lambda x: (x["liquidity_usd"] * 0.6 + x["volume_24h"] * 0.4), reverse=True)
    return result[:10]


def build_message(
    projects: List[Dict[str, Any]],
    leaders: Dict[str, Dict[str, Dict[str, Any]]],
    prev_state: Dict[str, Any],
) -> Tuple[str, Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]:
    top_projects = sorted(projects, key=lambda x: x["score"], reverse=True)[:3]
    real_earners = sorted(
        [p for p in projects if is_real_earner(p)],
        key=lambda x: (to_float(x.get("revenue_30d"), 0.0) or 0.0, x["score"]),
        reverse=True,
    )[:3]

    polygon_top3 = build_chain_top3_gecko("polygon", prev_state)
    bsc_top3 = build_bsc_top3_dex(prev_state)
    dex_pairs = collect_dex_pairs_for_unified_ranking()
    unified_top = build_unified_top(polygon_top3, bsc_top3, dex_pairs, prev_state)

    lines = []
    lines.append("🚀 DeFi 실전 투자 봇 v7.3")
    lines.append("")

    lines.append("[메인 분석 TOP 3]")
    for idx, p in enumerate(top_projects, start=1):
        sec = p.get("security", {})
        lines.append(f"{idx}) {p['name']} ({p['symbol']})")
        lines.append(f"- 판정: {p['grade']} / 점수: {p['score']}")
        lines.append(f"- 체인: {p['chain']} / 카테고리: {p['category']}")
        lines.append(f"- TVL: {fmt_num(p['tvl'])}")
        lines.append(f"- 유동성: {fmt_num(p['liquidity_usd'])}")
        lines.append(f"- 24h 거래량: {fmt_num(p['volume_24h'])}")
        lines.append(f"- Fees 30d: {fmt_num(p['fees_30d'])}")
        lines.append(f"- Revenue 30d: {fmt_num(p['revenue_30d'])}")
        lines.append(f"- FDV/유동성: {p['fdv_liquidity_ratio']:.2f}")
        lines.append(f"- 홀더 상위1/5 집중: {pct_str(sec.get('top1_pct'))} / {pct_str(sec.get('top5_pct'))}")
        lines.append(f"- 세금(매수/매도): {pct_str(sec.get('buy_tax'))} / {pct_str(sec.get('sell_tax'))}")
        lines.append(f"- 러그 경고: {', '.join(sec.get('risk_flags', [])) if sec.get('risk_flags') else '없음'}")
        lines.append("")

    lines.append("[진짜 돈 버는 프로토콜 모드 TOP 3]")
    if real_earners:
        for idx, p in enumerate(real_earners, start=1):
            lines.append(f"{idx}) {p['name']} ({p['symbol']})")
            lines.append(f"- 체인: {p['chain']} / 카테고리: {p['category']}")
            lines.append(f"- Revenue 30d: {fmt_num(p['revenue_30d'])}")
            lines.append(f"- Fees 30d: {fmt_num(p['fees_30d'])}")
            lines.append(f"- 유동성: {fmt_num(p['liquidity_usd'])}")
            lines.append(f"- FDV/유동성: {p['fdv_liquidity_ratio']:.2f}")
            lines.append("")
    else:
        lines.append("- 조건 충족 프로젝트 없음")
        lines.append("")

    lines.append("[통합 투자 TOP 10 - Gecko + Dex / 스테이블·기축 제외]")
    for idx, item in enumerate(unified_top, start=1):
        tail = f" {item['signal']}" if item.get("signal") else ""
        lines.append(
            f"{idx}) {item['name']} / 유동성 {fmt_num(item['liquidity_usd'])} / 거래량 {fmt_num(item['volume_24h'])}{tail}"
        )
    lines.append("")

    lines.append("[Polygon 유동성 TOP 3 - GeckoTerminal 풀 기준]")
    for idx, item in enumerate(polygon_top3["liquidity"], start=1):
        lines.append(
            f"{idx}) {item['pool_name']} / 유동성 {fmt_num(item['liquidity_usd'])} / 거래량 {fmt_num(item['volume_24h'])}"
        )
    lines.append("")

    lines.append("[Polygon 거래량 TOP 3 - GeckoTerminal 풀 기준]")
    for idx, item in enumerate(polygon_top3["volume"], start=1):
        lines.append(
            f"{idx}) {item['pool_name']} / 거래량 {fmt_num(item['volume_24h'])} / 유동성 {fmt_num(item['liquidity_usd'])}"
        )
    lines.append("")

    lines.append("[BSC 유동성 TOP 3 - DexScreener 풀 기준]")
    for idx, item in enumerate(bsc_top3["liquidity"], start=1):
        lines.append(
            f"{idx}) {item['pool_name']} / 유동성 {fmt_num(item['liquidity_usd'])} / 거래량 {fmt_num(item['volume_24h'])}"
        )
    if not bsc_top3["liquidity"]:
        lines.append("- 데이터 없음")
    lines.append("")

    lines.append("[BSC 거래량 TOP 3 - DexScreener 풀 기준]")
    for idx, item in enumerate(bsc_top3["volume"], start=1):
        lines.append(
            f"{idx}) {item['pool_name']} / 거래량 {fmt_num(item['volume_24h'])} / 유동성 {fmt_num(item['liquidity_usd'])}"
        )
    if not bsc_top3["volume"]:
        lines.append("- 데이터 없음")
    lines.append("")

    lines.append("[체인별 유동성 1위]")
    for chain in ["ethereum", "arbitrum", "base", "bsc", "polygon"]:
        leader = leaders.get(chain, {}).get("liquidity")
        if leader:
            lines.append(f"- {chain}: {leader['name']} / 유동성 {fmt_num(leader['liquidity_usd'])}")
        else:
            lines.append(f"- {chain}: 기본 필터 통과 프로젝트 없음")
    lines.append("")

    lines.append("[체인별 거래량 1위]")
    for chain in ["ethereum", "arbitrum", "base", "bsc", "polygon"]:
        leader = leaders.get(chain, {}).get("volume")
        if leader:
            lines.append(f"- {chain}: {leader['name']} / 24h 거래량 {fmt_num(leader['volume_24h'])}")
        else:
            lines.append(f"- {chain}: 기본 필터 통과 프로젝트 없음")
    lines.append("")

    msg = "\n".join(lines)

    next_gecko_snapshot = {
        "polygon": {
            normalize_name(x["name"]): {
                "liquidity_usd": x["liquidity_usd"],
                "volume_24h": x["volume_24h"],
            }
            for x in polygon_top3["all"]
        },
        "bsc": {
            normalize_name(x["name"]): {
                "liquidity_usd": x["liquidity_usd"],
                "volume_24h": x["volume_24h"],
            }
            for x in bsc_top3["all"]
        },
    }

    next_unified = {
        normalize_name(x["name"]): {
            "liq": x["liquidity_usd"],
            "vol": x["volume_24h"],
        }
        for x in unified_top
    }

    return msg, next_gecko_snapshot, next_unified, unified_top


def main() -> None:
    prev_state = load_state()

    protocols = filter_llama_protocols(get_llama_protocols())
    analyzed_projects: List[Dict[str, Any]] = []

    for protocol in protocols:
        try:
            pair = choose_best_pair_for_protocol(protocol)
            if not pair:
                continue
            if not passes_profitability_filter(protocol, pair):
                continue
            analyzed = analyze_project(protocol, pair)
            analyzed_projects.append(analyzed)
        except Exception:
            continue

    leaders = build_chain_leaders(analyzed_projects)

    message, gecko_snapshots, unified_flow, top_projects = build_message(
        analyzed_projects,
        leaders,
        prev_state,
    )

    send_telegram_message(message)

    new_state = {
        "chain_leaders": leaders,
        "top_projects": top_projects,
        "gecko_chain_snapshots": gecko_snapshots,
        "security_snapshots": prev_state.get("security_snapshots", {}),
        "unified_flow": unified_flow,
    }
    save_state(new_state)


if __name__ == "__main__":
    main()
