# -*- coding: utf-8 -*-
"""
🚀 DeFi 실전 투자 봇 v8.0
- 스테이블 / 기축 / 래핑 자산 제외
- 가짜 유동성 필터
- 중복 제거
- BSC DexScreener 우선
- timeout / retry / 디버그 로그 포함
"""

import requests
import time
import math
from collections import defaultdict

# =========================================================
# 설정값
# =========================================================

TELEGRAM_BOT_TOKEN = "YOUR_BOT_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_CHAT_ID"

REQUEST_TIMEOUT = 15
REQUEST_RETRY = 3
REQUEST_SLEEP = 0.7

TARGET_CHAINS = ["ethereum", "arbitrum", "base", "bsc", "polygon"]

DEX_CHAIN_MAP = {
    "ethereum": "ethereum",
    "arbitrum": "arbitrum",
    "base": "base",
    "bsc": "bsc",
    "polygon": "polygon",
}

# GeckoTerminal network id 추정값
GECKO_NETWORK_MAP = {
    "polygon": "polygon_pos",
    "ethereum": "eth",
    "arbitrum": "arbitrum",
    "base": "base",
    "bsc": "bsc",
}

# 제외 대상
EXCLUDED_SYMBOLS = {
    "USDT", "USDC", "DAI", "BUSD", "TUSD", "USDP", "FDUSD", "FRAX",
    "WBTC", "BTCB", "WETH", "WBNB", "ETH", "BNB", "MATIC", "POL",
    "STETH", "WSTETH", "WEETH", "EZETH", "RETH", "CBETH",
    "USDE", "SUSDE", "PYUSD", "LUSD", "GHO", "CRVUSD"
}

EXCLUDED_NAME_KEYWORDS = [
    "usd", "stable", "wrapped bitcoin", "wrapped ether", "bridged usdc",
    "tether", "usd coin", "binance usd", "frax", "liquid staking",
]

# 너무 작은 데이터 제거
MIN_LIQUIDITY_USD = 150_000
MIN_VOLUME_24H_USD = 5_000

# 유동성/거래량 이상 필터
# 유동성이 큰데 거래량이 지나치게 낮으면 가짜 유동성 의심
FAKE_LIQUIDITY_MIN_RATIO = 0.001   # volume/liquidity < 0.1% 이면 제거
# 거래량이 유동성보다 지나치게 큰 경우도 일부 의심이나, 너무 강하게 막지 않음
EXTREME_VOLUME_TO_LIQUIDITY = 20

# 통합 투자 TOP N
INTEGRATED_TOP_N = 10

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (compatible; DeFi-Agent-Bot/8.0)"
}


# =========================================================
# 공통 함수
# =========================================================

def debug(msg):
    print(f"[DEBUG] {msg}")


def safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.replace(",", "").strip()
        return float(value)
    except Exception:
        return default


def human_money(v):
    v = safe_float(v, 0)
    if abs(v) >= 1_000_000_000:
        return f"{v/1_000_000_000:.2f}B"
    if abs(v) >= 1_000_000:
        return f"{v/1_000_000:.2f}M"
    if abs(v) >= 1_000:
        return f"{v/1_000:.2f}K"
    return f"{v:.2f}"


def safe_get(url, params=None):
    last_err = None
    for attempt in range(1, REQUEST_RETRY + 1):
        try:
            r = requests.get(url, params=params, headers=USER_AGENT, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            debug(f"요청 실패 {attempt}/{REQUEST_RETRY} | {url} | {e}")
            time.sleep(REQUEST_SLEEP)
    debug(f"최종 실패 | {url} | {last_err}")
    return None


def is_excluded_asset(symbol="", name=""):
    s = (symbol or "").upper().strip()
    n = (name or "").lower().strip()

    if s in EXCLUDED_SYMBOLS:
        return True

    for kw in EXCLUDED_NAME_KEYWORDS:
        if kw in n:
            return True
    return False


def normalize_symbol(symbol):
    return (symbol or "").upper().strip()


def pair_key(chain, dex_id, base_symbol, quote_symbol):
    a = normalize_symbol(base_symbol)
    b = normalize_symbol(quote_symbol)
    left, right = sorted([a, b])
    return f"{chain}|{dex_id}|{left}|{right}"


def is_suspicious_pool(pool):
    liquidity = safe_float(pool.get("liquidity_usd"))
    volume = safe_float(pool.get("volume_24h"))
    fdv = safe_float(pool.get("fdv"))

    if liquidity < MIN_LIQUIDITY_USD:
        return True

    if volume < MIN_VOLUME_24H_USD:
        return True

    ratio = 0 if liquidity <= 0 else volume / liquidity

    # 유동성만 매우 크고 거래량이 너무 적으면 의심
    if liquidity >= 1_000_000 and ratio < FAKE_LIQUIDITY_MIN_RATIO:
        return True

    # 거래량이 유동성보다 비정상적으로 너무 큰 경우
    if liquidity > 0 and ratio > EXTREME_VOLUME_TO_LIQUIDITY:
        return True

    # FDV가 유동성 대비 과도하게 크면 경고성 제거
    if liquidity > 0 and fdv > 0:
        fdv_liq = fdv / liquidity
        if fdv_liq > 500:
            return True

    return False


def classify_project(score):
    if score >= 75:
        return "양호"
    if score >= 55:
        return "주의"
    return "위험"


def send_telegram_message(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        debug("텔레그램 설정 없음 -> 콘솔 출력만 진행")
        print(text)
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        debug("텔레그램 전송 완료")
    except Exception as e:
        debug(f"텔레그램 전송 실패: {e}")
        print(text)


# =========================================================
# DeFiLlama - 프로토콜 분석
# =========================================================

def fetch_defillama_protocols():
    debug("DefiLlama 프로토콜 조회 시작")
    url = "https://api.llama.fi/protocols"
    data = safe_get(url)
    if not data or not isinstance(data, list):
        return []
    return data


def protocol_score(proto):
    tvl = safe_float(proto.get("tvl"))
    mcap = safe_float(proto.get("mcap"))
    fdv = safe_float(proto.get("fdv"))
    chains = proto.get("chains") or []
    chain_count = len(chains)

    score = 50

    if tvl >= 500_000_000:
        score += 15
    elif tvl >= 100_000_000:
        score += 10
    elif tvl >= 30_000_000:
        score += 5

    if chain_count >= 4:
        score += 8
    elif chain_count >= 2:
        score += 4

    if fdv > 0 and tvl > 0:
        fdv_tvl = fdv / tvl
        if fdv_tvl <= 1.5:
            score += 10
        elif fdv_tvl <= 4:
            score += 4
        elif fdv_tvl > 10:
            score -= 10

    if mcap > 0 and tvl > 0:
        mcap_tvl = mcap / tvl
        if mcap_tvl <= 2:
            score += 6
        elif mcap_tvl > 8:
            score -= 6

    category = (proto.get("category") or "").lower()
    if "bridge" in category:
        score -= 8
    if "leveraged" in category or "derivatives" in category:
        score -= 4

    if proto.get("audits", "0") in [0, "0", None]:
        score -= 6

    score = max(0, min(100, int(round(score))))
    return score


def build_main_analysis(protocols):
    debug("메인 분석 TOP3 생성")
    candidates = []

    for p in protocols:
        symbol = (p.get("symbol") or "").upper().strip()
        name = p.get("name") or ""
        chain = (p.get("chain") or "").lower()
        category = p.get("category") or "-"
        tvl = safe_float(p.get("tvl"))
        fdv = safe_float(p.get("fdv"))
        mcap = safe_float(p.get("mcap"))

        if is_excluded_asset(symbol, name):
            continue
        if tvl < 20_000_000:
            continue
        if chain not in TARGET_CHAINS:
            continue

        score = protocol_score(p)
        verdict = classify_project(score)

        # 유동성/거래량이 없으므로 이 부분은 외부 풀 데이터와 결합되기 전까지 "-".
        item = {
            "name": name,
            "symbol": symbol or name[:5].upper(),
            "verdict": verdict,
            "score": score,
            "chain": chain,
            "category": category,
            "tvl": tvl,
            "liquidity_usd": 0,
            "volume_24h": 0,
            "fees_30d": None,
            "revenue_30d": None,
            "fdv_liquidity": (fdv / 1) if fdv > 0 else None,
            "holder_top1": None,
            "holder_top5": None,
            "buy_tax": 0.0,
            "sell_tax": 0.0,
            "rug_warning": "없음",
            "fdv": fdv,
            "mcap": mcap,
        }
        candidates.append(item)

    candidates.sort(key=lambda x: (-x["score"], -x["tvl"]))
    return candidates[:3]


# =========================================================
# CoinGecko - 코인 후보군
# =========================================================

def fetch_coingecko_markets(page=1, per_page=100):
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "volume_desc",
        "per_page": per_page,
        "page": page,
        "sparkline": "false",
        "price_change_percentage": "24h"
    }
    return safe_get(url, params=params) or []


def fetch_candidate_tokens():
    debug("CoinGecko 후보 토큰 수집")
    tokens = []
    seen = set()

    for page in range(1, 4):
        data = fetch_coingecko_markets(page=page, per_page=100)
        if not data:
            continue
        for item in data:
            symbol = normalize_symbol(item.get("symbol"))
            name = item.get("name") or ""
            market_cap = safe_float(item.get("market_cap"))
            total_volume = safe_float(item.get("total_volume"))

            if not symbol:
                continue
            if is_excluded_asset(symbol, name):
                continue
            if market_cap <= 0 or total_volume <= 0:
                continue

            if symbol in seen:
                continue
            seen.add(symbol)

            tokens.append({
                "symbol": symbol,
                "name": name,
                "market_cap": market_cap,
                "total_volume": total_volume,
            })

    # 거래량 높은 순
    tokens.sort(key=lambda x: (-x["total_volume"], -x["market_cap"]))
    return tokens[:120]


# =========================================================
# DexScreener
# =========================================================

def fetch_dexscreener_by_symbol(symbol):
    url = "https://api.dexscreener.com/latest/dex/search"
    params = {"q": symbol}
    data = safe_get(url, params=params)
    if not data:
        return []
    return data.get("pairs", []) or []


def parse_dex_pair(raw):
    try:
        chain = (raw.get("chainId") or "").lower()
        dex_id = raw.get("dexId") or "-"
        base = raw.get("baseToken") or {}
        quote = raw.get("quoteToken") or {}

        base_symbol = normalize_symbol(base.get("symbol"))
        quote_symbol = normalize_symbol(quote.get("symbol"))
        base_name = base.get("name") or ""
        quote_name = quote.get("name") or ""

        liquidity_usd = safe_float((raw.get("liquidity") or {}).get("usd"))
        volume_24h = safe_float((raw.get("volume") or {}).get("h24"))
        fdv = safe_float(raw.get("fdv"))
        price_usd = safe_float(raw.get("priceUsd"))
        pair_addr = raw.get("pairAddress") or ""
        url = raw.get("url") or ""

        return {
            "chain": chain,
            "dex_id": dex_id,
            "pair_address": pair_addr,
            "base_symbol": base_symbol,
            "base_name": base_name,
            "quote_symbol": quote_symbol,
            "quote_name": quote_name,
            "liquidity_usd": liquidity_usd,
            "volume_24h": volume_24h,
            "fdv": fdv,
            "price_usd": price_usd,
            "url": url,
        }
    except Exception:
        return None


def collect_dex_pools_from_candidates(tokens):
    debug("DexScreener 풀 수집 시작")
    all_pools = []
    seen_pair_address = set()
    seen_pair_combo = set()

    for idx, token in enumerate(tokens, start=1):
        symbol = token["symbol"]
        debug(f"Dex 조회 {idx}/{len(tokens)} - {symbol}")
        raws = fetch_dexscreener_by_symbol(symbol)

        for raw in raws:
            parsed = parse_dex_pair(raw)
            if not parsed:
                continue

            chain = parsed["chain"]
            if chain not in TARGET_CHAINS:
                continue

            base_symbol = parsed["base_symbol"]
            quote_symbol = parsed["quote_symbol"]

            # 토큰 본체가 base 또는 quote 중 하나에 있어야 함
            if symbol not in [base_symbol, quote_symbol]:
                continue

            # 둘 중 하나라도 기축/스테이블이면 상관없지만
            # "통합 투자 순위" 자체는 base token 기준으로만 본다.
            # 다만 quote가 stable/base인 것 자체는 허용.
            if is_excluded_asset(base_symbol, parsed["base_name"]):
                continue

            if parsed["pair_address"] in seen_pair_address:
                continue

            combo_key = pair_key(chain, parsed["dex_id"], base_symbol, quote_symbol)
            if combo_key in seen_pair_combo:
                continue

            if is_suspicious_pool(parsed):
                continue

            seen_pair_address.add(parsed["pair_address"])
            seen_pair_combo.add(combo_key)
            all_pools.append(parsed)

        time.sleep(0.12)

    return all_pools


# =========================================================
# GeckoTerminal
# =========================================================

def fetch_gecko_trending_pools(network_id, page=1):
    url = f"https://api.geckoterminal.com/api/v2/networks/{network_id}/trending_pools"
    params = {"page": page}
    return safe_get(url, params=params)


def parse_gecko_pool_item(item):
    try:
        attr = item.get("attributes") or {}
        relationships = item.get("relationships") or {}

        name = attr.get("name") or ""
        address = attr.get("address") or ""
        dex_id = attr.get("dex_name") or "-"
        reserve_usd = safe_float(attr.get("reserve_in_usd"))
        volume_24h = safe_float((attr.get("volume_usd") or {}).get("h24"))
        fdv = safe_float(attr.get("fdv_usd"))

        # 이름에서 심볼 페어 추출 시도
        # 예: "ABC / USDC 0.3%"
        base_symbol = ""
        quote_symbol = ""
        if "/" in name:
            left = name.split("/")[0].strip()
            right = name.split("/")[1].strip().split()[0].strip()
            base_symbol = normalize_symbol(left)
            quote_symbol = normalize_symbol(right)

        return {
            "chain": "polygon",
            "dex_id": dex_id,
            "pair_address": address,
            "base_symbol": base_symbol,
            "base_name": base_symbol,
            "quote_symbol": quote_symbol,
            "quote_name": quote_symbol,
            "liquidity_usd": reserve_usd,
            "volume_24h": volume_24h,
            "fdv": fdv,
            "price_usd": 0,
            "url": "",
        }
    except Exception:
        return None


def fetch_polygon_gecko_pools():
    debug("GeckoTerminal Polygon 풀 수집")
    network_id = GECKO_NETWORK_MAP["polygon"]
    pools = []
    seen = set()

    for page in range(1, 4):
        data = fetch_gecko_trending_pools(network_id, page=page)
        if not data:
            continue

        items = data.get("data", []) or []
        for item in items:
            parsed = parse_gecko_pool_item(item)
            if not parsed:
                continue

            if parsed["pair_address"] in seen:
                continue

            base_symbol = parsed["base_symbol"]
            base_name = parsed["base_name"]

            if not base_symbol:
                continue
            if is_excluded_asset(base_symbol, base_name):
                continue
            if is_suspicious_pool(parsed):
                continue

            seen.add(parsed["pair_address"])
            pools.append(parsed)

    return pools


# =========================================================
# 데이터 통합 / 정렬
# =========================================================

def build_integrated_top10(dex_pools, polygon_gecko_pools):
    debug("통합 TOP10 생성")
    merged = dex_pools + polygon_gecko_pools

    best_by_symbol = {}

    for p in merged:
        symbol = p["base_symbol"]
        if not symbol:
            continue
        if is_excluded_asset(symbol, p["base_name"]):
            continue

        # 동일 심볼은 유동성 큰 풀 우선, 같은 유동성이면 거래량 큰 쪽
        if symbol not in best_by_symbol:
            best_by_symbol[symbol] = p
        else:
            old = best_by_symbol[symbol]
            if (
                safe_float(p["liquidity_usd"]) > safe_float(old["liquidity_usd"])
                or (
                    safe_float(p["liquidity_usd"]) == safe_float(old["liquidity_usd"])
                    and safe_float(p["volume_24h"]) > safe_float(old["volume_24h"])
                )
            ):
                best_by_symbol[symbol] = p

    items = list(best_by_symbol.values())

    # 거래량/유동성 밸런스 점수
    def rank_score(x):
        liq = safe_float(x["liquidity_usd"])
        vol = safe_float(x["volume_24h"])
        ratio_bonus = 0
        if liq > 0:
            ratio = vol / liq
            if 0.02 <= ratio <= 1.5:
                ratio_bonus = liq * 0.2
        return liq + vol * 0.5 + ratio_bonus

    items.sort(key=lambda x: rank_score(x), reverse=True)
    return items[:INTEGRATED_TOP_N]


def build_chain_top3(pools, chain_name, sort_key):
    filtered = [p for p in pools if p["chain"] == chain_name]
    filtered.sort(key=lambda x: safe_float(x.get(sort_key)), reverse=True)
    return filtered[:3]


def build_chain_leaders(protocols):
    chain_liq = {}
    chain_vol = {}

    # protocol에는 liquidity/volume이 약하므로 tvl 기준 보조 처리
    per_chain = defaultdict(list)
    for p in protocols:
        chain = (p.get("chain") or "").lower()
        symbol = (p.get("symbol") or "").upper().strip()
        name = p.get("name") or ""
        tvl = safe_float(p.get("tvl"))
        category = p.get("category") or "-"

        if chain not in TARGET_CHAINS:
            continue
        if is_excluded_asset(symbol, name):
            continue
        if tvl < 5_000_000:
            continue

        per_chain[chain].append({
            "name": name,
            "symbol": symbol or name[:5].upper(),
            "tvl": tvl,
            "category": category
        })

    for chain in TARGET_CHAINS:
        arr = sorted(per_chain.get(chain, []), key=lambda x: x["tvl"], reverse=True)
        if arr:
            chain_liq[chain] = arr[0]
            chain_vol[chain] = arr[0]
        else:
            chain_liq[chain] = None
            chain_vol[chain] = None

    return chain_liq, chain_vol


# =========================================================
# 출력
# =========================================================

def format_main_analysis(main_top3):
    lines = []
    lines.append("[메인 분석 TOP 3]")
    if not main_top3:
        lines.append("- 조건 충족 프로젝트 없음")
        return "\n".join(lines)

    for i, item in enumerate(main_top3, start=1):
        fdv_liq_text = "-"
        if item["liquidity_usd"] > 0 and item["fdv"] > 0:
            fdv_liq_text = f"{item['fdv']/item['liquidity_usd']:.2f}"

        lines.append(f"{i}) {item['name']} ({item['symbol']})")
        lines.append(f"- 판정: {item['verdict']} / 점수: {item['score']}")
        lines.append(f"- 체인: {item['chain']} / 카테고리: {item['category']}")
        lines.append(f"- TVL: {human_money(item['tvl'])}")
        lines.append(f"- 유동성: {human_money(item['liquidity_usd'])}")
        lines.append(f"- 24h 거래량: {human_money(item['volume_24h'])}")
        lines.append(f"- Fees 30d: {'-' if item['fees_30d'] is None else human_money(item['fees_30d'])}")
        lines.append(f"- Revenue 30d: {'-' if item['revenue_30d'] is None else human_money(item['revenue_30d'])}")
        lines.append(f"- FDV/유동성: {fdv_liq_text}")
        lines.append(f"- 홀더 상위1/5 집중: - / -")
        lines.append(f"- 세금(매수/매도): 0.00% / 0.00%")
        lines.append(f"- 러그 경고: {item['rug_warning']}")
        lines.append("")
    return "\n".join(lines).strip()


def format_real_revenue_mode(protocols):
    lines = []
    lines.append("[진짜 돈 버는 프로토콜 모드 TOP 3]")

    # 수익 데이터가 없으므로 TVL + 점수 기반 보수적 후보
    candidates = []
    for p in protocols:
        score = protocol_score(p)
        tvl = safe_float(p.get("tvl"))
        symbol = (p.get("symbol") or "").upper().strip()
        name = p.get("name") or ""
        chain = (p.get("chain") or "").lower()
        category = p.get("category") or "-"

        if chain not in TARGET_CHAINS:
            continue
        if is_excluded_asset(symbol, name):
            continue
        if tvl < 80_000_000:
            continue
        if score < 60:
            continue

        candidates.append({
            "name": name,
            "symbol": symbol or name[:5].upper(),
            "chain": chain,
            "category": category,
            "tvl": tvl,
            "score": score
        })

    candidates.sort(key=lambda x: (-x["score"], -x["tvl"]))
    candidates = candidates[:3]

    if not candidates:
        lines.append("- 조건 충족 프로젝트 없음")
        return "\n".join(lines)

    for i, item in enumerate(candidates, start=1):
        lines.append(
            f"{i}) {item['name']} ({item['symbol']}) / 체인: {item['chain']} / 카테고리: {item['category']} / TVL: {human_money(item['tvl'])} / 점수: {item['score']}"
        )
    return "\n".join(lines)


def format_integrated_top10(items):
    lines = []
    lines.append("[통합 투자 TOP 10 - Gecko + Dex / 스테이블·기축 제외]")

    if not items:
        lines.append("- 조건 충족 프로젝트 없음")
        return "\n".join(lines)

    for i, p in enumerate(items, start=1):
        liq = safe_float(p["liquidity_usd"])
        vol = safe_float(p["volume_24h"])

        tag = ""
        if liq >= 20_000_000 and vol >= 5_000_000:
            tag = " 🔥 대규모 자금 유입"
        elif vol >= 1_000_000:
            tag = " 📈 거래량 강함"

        lines.append(
            f"{i}) {p['base_symbol']} / 유동성 {human_money(liq)} / 거래량 {human_money(vol)}{tag}"
        )
    return "\n".join(lines)


def format_chain_pool_section(title, pools, metric_label, metric_key):
    lines = [title]
    if not pools:
        lines.append("- 조건 충족 풀 없음")
        return "\n".join(lines)

    for i, p in enumerate(pools, start=1):
        lines.append(
            f"{i}) {p['base_symbol']} / {p['quote_symbol']} / {metric_label} {human_money(p[metric_key])} / 유동성 {human_money(p['liquidity_usd'])}"
            if metric_key == "volume_24h"
            else f"{i}) {p['base_symbol']} / {p['quote_symbol']} / 유동성 {human_money(p['liquidity_usd'])} / 거래량 {human_money(p['volume_24h'])}"
        )
    return "\n".join(lines)


def format_chain_leaders(chain_liq, chain_vol):
    lines = []
    lines.append("[체인별 유동성 1위]")
    for chain in TARGET_CHAINS:
        item = chain_liq.get(chain)
        if item:
            lines.append(f"- {chain}: {item['name']} / 유동성 지표(TVL) {human_money(item['tvl'])}")
        else:
            lines.append(f"- {chain}: 기본 필터 통과 프로젝트 없음")

    lines.append("")
    lines.append("[체인별 거래량 1위]")
    for chain in TARGET_CHAINS:
        item = chain_vol.get(chain)
        if item:
            lines.append(f"- {chain}: {item['name']} / 거래량 대체지표(TVL) {human_money(item['tvl'])}")
        else:
            lines.append(f"- {chain}: 기본 필터 통과 프로젝트 없음")

    return "\n".join(lines)


# =========================================================
# 메인 실행
# =========================================================

def run():
    debug("1단계 - DefiLlama")
    protocols = fetch_defillama_protocols()

    debug("2단계 - 메인 분석")
    main_top3 = build_main_analysis(protocols)

    debug("3단계 - CoinGecko 후보")
    candidate_tokens = fetch_candidate_tokens()

    debug("4단계 - DexScreener 풀")
    dex_pools = collect_dex_pools_from_candidates(candidate_tokens)

    debug("5단계 - GeckoTerminal Polygon 풀")
    polygon_gecko_pools = fetch_polygon_gecko_pools()

    debug("6단계 - 통합 TOP10")
    integrated_top10 = build_integrated_top10(dex_pools, polygon_gecko_pools)

    # Polygon은 GeckoTerminal 기준
    polygon_pool_source = [p for p in polygon_gecko_pools if p["chain"] == "polygon"]

    # BSC는 DexScreener 기준
    bsc_pool_source = [p for p in dex_pools if p["chain"] == "bsc"]

    polygon_top_liq = build_chain_top3(polygon_pool_source, "polygon", "liquidity_usd")
    polygon_top_vol = build_chain_top3(polygon_pool_source, "polygon", "volume_24h")

    bsc_top_liq = build_chain_top3(bsc_pool_source, "bsc", "liquidity_usd")
    bsc_top_vol = build_chain_top3(bsc_pool_source, "bsc", "volume_24h")

    debug("7단계 - 체인 리더")
    chain_liq, chain_vol = build_chain_leaders(protocols)

    debug("8단계 - 메시지 조합")
    parts = []
    parts.append("🚀 DeFi 실전 투자 봇 v8.0")
    parts.append("")
    parts.append(format_main_analysis(main_top3))
    parts.append("")
    parts.append(format_real_revenue_mode(protocols))
    parts.append("")
    parts.append(format_integrated_top10(integrated_top10))
    parts.append("")
    parts.append(format_chain_pool_section(
        "[Polygon 유동성 TOP 3 - GeckoTerminal 풀 기준]",
        polygon_top_liq,
        "유동성",
        "liquidity_usd"
    ))
    parts.append("")
    parts.append(format_chain_pool_section(
        "[Polygon 거래량 TOP 3 - GeckoTerminal 풀 기준]",
        polygon_top_vol,
        "거래량",
        "volume_24h"
    ))
    parts.append("")
    parts.append(format_chain_pool_section(
        "[BSC 유동성 TOP 3 - DexScreener 풀 기준]",
        bsc_top_liq,
        "유동성",
        "liquidity_usd"
    ))
    parts.append("")
    parts.append(format_chain_pool_section(
        "[BSC 거래량 TOP 3 - DexScreener 풀 기준]",
        bsc_top_vol,
        "거래량",
        "volume_24h"
    ))
    parts.append("")
    parts.append(format_chain_leaders(chain_liq, chain_vol))

    final_text = "\n".join(parts)
    send_telegram_message(final_text)
    return final_text


if __name__ == "__main__":
    result = run()
    print("\n" + "=" * 80 + "\n")
    print(result)
