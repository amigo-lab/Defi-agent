# -*- coding: utf-8 -*-
"""
🚀 DeFi 실전 투자 봇 v8.3
- 스테이블/기축/래핑 자산 강력 제외
- 풀에서 실제 투자 대상 토큰(focus token) 재추출
- BTCB, WBNB, WETH, FRAX, USDT, USDC, DAI 반복 노출 방지
- 중복 제거 강화
- 오전 9시 자동 전송 스케줄 포함
"""

import re
import time
import requests
import schedule
from collections import defaultdict

# =========================================================
# 텔레그램 설정
# =========================================================
TELEGRAM_BOT_TOKEN = "YOUR_BOT_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_CHAT_ID"

# =========================================================
# 기본 설정
# =========================================================
REQUEST_TIMEOUT = 15
REQUEST_RETRY = 3
REQUEST_SLEEP = 0.8

TARGET_CHAINS = ["ethereum", "arbitrum", "base", "bsc", "polygon"]

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (compatible; DeFi-Agent-Bot/8.3)"
}

MIN_LIQUIDITY_USD = 150_000
MIN_VOLUME_24H_USD = 5_000
MIN_VOL_LIQ_RATIO = 0.001
MAX_VOL_LIQ_RATIO = 20
MAX_FDV_LIQ_RATIO = 500
INTEGRATED_TOP_N = 10

# =========================================================
# 제외 대상
# =========================================================
EXCLUDED_SYMBOLS = {
    "USDT", "USDC", "DAI", "BUSD", "TUSD", "USDP", "FDUSD", "FRAX",
    "PYUSD", "LUSD", "GHO", "CRVUSD", "USDE", "SUSDE",

    "BTC", "WBTC", "BTCB", "TBTC", "SOLVBTC",
    "ETH", "WETH", "STETH", "WSTETH", "RETH", "WEETH", "EZETH", "CBETH",
    "BNB", "WBNB",
    "MATIC", "WMATIC", "POL", "WPOL",
    "AVAX", "WAVAX",
    "SOL", "WSOL"
}

EXCLUDED_NAME_KEYWORDS = [
    "stable", "usd", "tether", "usd coin", "frax",
    "wrapped bitcoin", "wrapped btc",
    "wrapped ether", "wrapped eth",
    "liquid staking", "restaked",
    "bridged usdc", "bridged usdt", "bridged weth",
    "solvbtc"
]

# =========================================================
# 공통 함수
# =========================================================
def debug(msg):
    print(f"[DEBUG] {msg}")

def safe_float(v, default=0.0):
    try:
        if v is None:
            return default
        if isinstance(v, str):
            v = v.replace(",", "").strip()
        return float(v)
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

def normalize_text(s):
    return (s or "").strip()

def normalize_symbol(symbol):
    s = normalize_text(symbol).upper()
    s = re.sub(r"\s*\d+(\.\d+)?%$", "", s).strip()
    s = re.sub(r"[^A-Z0-9_\-]", "", s)
    s = s.replace("WRAPPEDBTC", "WBTC")
    s = s.replace("WRAPPEDETH", "WETH")
    return s

def clean_pool_token_name(name):
    n = normalize_text(name)
    n = re.sub(r"\s*\d+(\.\d+)?%$", "", n).strip()
    return n

def is_excluded_asset(symbol="", name=""):
    sym = normalize_symbol(symbol)
    nm = clean_pool_token_name(name).lower()

    if sym in EXCLUDED_SYMBOLS:
        return True

    for kw in EXCLUDED_NAME_KEYWORDS:
        if kw in nm:
            return True

    return False

def is_valid_non_base_asset(symbol="", name=""):
    sym = normalize_symbol(symbol)
    nm = clean_pool_token_name(name)

    if not sym:
        return False
    if is_excluded_asset(sym, nm):
        return False
    if len(sym) > 20:
        return False
    return True

def canonical_pair_key(chain, dex_id, sym1, sym2):
    a = normalize_symbol(sym1)
    b = normalize_symbol(sym2)
    left, right = sorted([a, b])
    return f"{chain}|{dex_id}|{left}|{right}"

# =========================================================
# 풀에서 실제 대상 토큰 추출
# =========================================================
def derive_focus_asset(base_symbol, base_name, quote_symbol, quote_name):
    """
    규칙:
    - 한쪽만 제외 대상이면, 제외되지 않은 쪽을 표시 대상 토큰으로 선택
    - 둘 다 제외 대상이면 None
    - 둘 다 제외 대상이 아니면 base 쪽 우선
    """
    base_ex = is_excluded_asset(base_symbol, base_name)
    quote_ex = is_excluded_asset(quote_symbol, quote_name)

    base_symbol = normalize_symbol(base_symbol)
    quote_symbol = normalize_symbol(quote_symbol)

    if base_ex and quote_ex:
        return None

    if not base_ex and quote_ex:
        return {
            "project_symbol": base_symbol,
            "project_name": clean_pool_token_name(base_name or base_symbol),
            "pair_symbol": quote_symbol,
            "pair_name": clean_pool_token_name(quote_name or quote_symbol)
        }

    if base_ex and not quote_ex:
        return {
            "project_symbol": quote_symbol,
            "project_name": clean_pool_token_name(quote_name or quote_symbol),
            "pair_symbol": base_symbol,
            "pair_name": clean_pool_token_name(base_name or base_symbol)
        }

    return {
        "project_symbol": base_symbol,
        "project_name": clean_pool_token_name(base_name or base_symbol),
        "pair_symbol": quote_symbol,
        "pair_name": clean_pool_token_name(quote_name or quote_symbol)
    }

def enrich_focus_asset(pool):
    focus = derive_focus_asset(
        pool.get("base_symbol", ""),
        pool.get("base_name", ""),
        pool.get("quote_symbol", ""),
        pool.get("quote_name", "")
    )
    if not focus:
        return None

    project_symbol = normalize_symbol(focus["project_symbol"])
    project_name = focus["project_name"]

    if not is_valid_non_base_asset(project_symbol, project_name):
        return None

    new_pool = dict(pool)
    new_pool["project_symbol"] = project_symbol
    new_pool["project_name"] = project_name
    new_pool["pair_symbol"] = normalize_symbol(focus["pair_symbol"])
    new_pool["pair_name"] = focus["pair_name"]
    return new_pool

def pool_quality_ok(pool):
    liq = safe_float(pool.get("liquidity_usd"))
    vol = safe_float(pool.get("volume_24h"))
    fdv = safe_float(pool.get("fdv"))

    if liq < MIN_LIQUIDITY_USD:
        return False
    if vol < MIN_VOLUME_24H_USD:
        return False

    if liq > 0:
        ratio = vol / liq
        if liq >= 1_000_000 and ratio < MIN_VOL_LIQ_RATIO:
            return False
        if ratio > MAX_VOL_LIQ_RATIO:
            return False

    if liq > 0 and fdv > 0:
        fdv_liq = fdv / liq
        if fdv_liq > MAX_FDV_LIQ_RATIO:
            return False

    return True

# =========================================================
# 텔레그램
# =========================================================
def send_telegram_message(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
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
# DeFiLlama
# =========================================================
def fetch_defillama_protocols():
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

    if tvl > 0 and fdv > 0:
        fdv_tvl = fdv / tvl
        if fdv_tvl <= 1.5:
            score += 10
        elif fdv_tvl <= 4:
            score += 4
        elif fdv_tvl > 10:
            score -= 10

    if tvl > 0 and mcap > 0:
        mcap_tvl = mcap / tvl
        if mcap_tvl <= 2:
            score += 6
        elif mcap_tvl > 8:
            score -= 6

    category = (proto.get("category") or "").lower()
    if "bridge" in category:
        score -= 8
    if "derivatives" in category or "leveraged" in category:
        score -= 4

    if proto.get("audits", "0") in [0, "0", None]:
        score -= 6

    return max(0, min(100, int(round(score))))

def classify_project(score):
    if score >= 75:
        return "양호"
    if score >= 55:
        return "주의"
    return "위험"

def build_main_analysis(protocols):
    items = []
    for p in protocols:
        symbol = normalize_symbol(p.get("symbol") or "")
        name = p.get("name") or ""
        chain = (p.get("chain") or "").lower()
        category = p.get("category") or "-"
        tvl = safe_float(p.get("tvl"))

        if chain not in TARGET_CHAINS:
            continue
        if tvl < 20_000_000:
            continue
        if is_excluded_asset(symbol, name):
            continue

        score = protocol_score(p)
        items.append({
            "name": name,
            "symbol": symbol or name[:6].upper(),
            "verdict": classify_project(score),
            "score": score,
            "chain": chain,
            "category": category,
            "tvl": tvl,
            "liquidity_usd": 0,
            "volume_24h": 0,
            "rug_warning": "없음",
        })

    items.sort(key=lambda x: (-x["score"], -x["tvl"]))
    return items[:3]

# =========================================================
# CoinGecko
# =========================================================
def fetch_coingecko_markets(page=1, per_page=100):
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "volume_desc",
        "per_page": per_page,
        "page": page,
        "sparkline": "false",
        "price_change_percentage": "24h",
    }
    return safe_get(url, params=params) or []

def fetch_candidate_tokens():
    candidates = []
    seen = set()

    for page in range(1, 4):
        rows = fetch_coingecko_markets(page=page, per_page=100)
        for row in rows:
            symbol = normalize_symbol(row.get("symbol") or "")
            name = row.get("name") or ""
            market_cap = safe_float(row.get("market_cap"))
            total_volume = safe_float(row.get("total_volume"))

            if not symbol:
                continue
            if is_excluded_asset(symbol, name):
                continue
            if market_cap <= 0 or total_volume <= 0:
                continue
            if symbol in seen:
                continue

            seen.add(symbol)
            candidates.append({
                "symbol": symbol,
                "name": name,
                "market_cap": market_cap,
                "total_volume": total_volume,
            })

    candidates.sort(key=lambda x: (-x["total_volume"], -x["market_cap"]))
    return candidates[:120]

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
    base = raw.get("baseToken") or {}
    quote = raw.get("quoteToken") or {}

    return {
        "chain": (raw.get("chainId") or "").lower(),
        "dex_id": raw.get("dexId") or "-",
        "pair_address": raw.get("pairAddress") or "",
        "base_symbol": normalize_symbol(base.get("symbol") or ""),
        "base_name": clean_pool_token_name(base.get("name") or base.get("symbol") or ""),
        "quote_symbol": normalize_symbol(quote.get("symbol") or ""),
        "quote_name": clean_pool_token_name(quote.get("name") or quote.get("symbol") or ""),
        "liquidity_usd": safe_float((raw.get("liquidity") or {}).get("usd")),
        "volume_24h": safe_float((raw.get("volume") or {}).get("h24")),
        "fdv": safe_float(raw.get("fdv")),
        "price_usd": safe_float(raw.get("priceUsd")),
        "url": raw.get("url") or "",
    }

def collect_dex_pools_from_candidates(tokens):
    pools = []
    seen_addr = set()
    seen_key = set()

    for idx, token in enumerate(tokens, start=1):
        symbol = token["symbol"]
        debug(f"Dex 조회 {idx}/{len(tokens)} - {symbol}")

        raws = fetch_dexscreener_by_symbol(symbol)
        for raw in raws:
            p = parse_dex_pair(raw)

            if p["chain"] not in TARGET_CHAINS:
                continue

            # 검색한 심볼이 base 또는 quote 어느 한쪽에 반드시 있어야 함
            if symbol not in {p["base_symbol"], p["quote_symbol"]}:
                continue

            if not pool_quality_ok(p):
                continue

            p2 = enrich_focus_asset(p)
            if not p2:
                continue

            if p["pair_address"] and p["pair_address"] in seen_addr:
                continue

            key = canonical_pair_key(p["chain"], p["dex_id"], p2["project_symbol"], p2["pair_symbol"])
            if key in seen_key:
                continue

            if p["pair_address"]:
                seen_addr.add(p["pair_address"])
            seen_key.add(key)
            pools.append(p2)

        time.sleep(0.12)

    return pools

# =========================================================
# GeckoTerminal (Polygon)
# =========================================================
def fetch_gecko_trending_pools(network_id="polygon_pos", page=1):
    url = f"https://api.geckoterminal.com/api/v2/networks/{network_id}/trending_pools"
    params = {"page": page}
    return safe_get(url, params=params)

def parse_pool_name_from_gecko(name):
    if not name or "/" not in name:
        return "", ""

    cleaned = clean_pool_token_name(name)
    left, right = cleaned.split("/", 1)
    base_symbol = normalize_symbol(left.strip())
    quote_symbol = normalize_symbol(right.strip().split()[0].strip())
    return base_symbol, quote_symbol

def parse_gecko_pool(item):
    attr = item.get("attributes") or {}
    name = attr.get("name") or ""
    base_symbol, quote_symbol = parse_pool_name_from_gecko(name)

    return {
        "chain": "polygon",
        "dex_id": attr.get("dex_name") or "-",
        "pair_address": attr.get("address") or "",
        "base_symbol": base_symbol,
        "base_name": base_symbol,
        "quote_symbol": quote_symbol,
        "quote_name": quote_symbol,
        "liquidity_usd": safe_float(attr.get("reserve_in_usd")),
        "volume_24h": safe_float((attr.get("volume_usd") or {}).get("h24")),
        "fdv": safe_float(attr.get("fdv_usd")),
        "price_usd": 0.0,
        "url": "",
    }

def fetch_polygon_gecko_pools():
    pools = []
    seen_addr = set()
    seen_key = set()

    for page in range(1, 4):
        data = fetch_gecko_trending_pools("polygon_pos", page=page)
        if not data:
            continue

        for item in data.get("data", []) or []:
            p = parse_gecko_pool(item)

            if not p["base_symbol"] or not p["quote_symbol"]:
                continue
            if not pool_quality_ok(p):
                continue

            p2 = enrich_focus_asset(p)
            if not p2:
                continue

            if p["pair_address"] and p["pair_address"] in seen_addr:
                continue

            key = canonical_pair_key(p["chain"], p["dex_id"], p2["project_symbol"], p2["pair_symbol"])
            if key in seen_key:
                continue

            if p["pair_address"]:
                seen_addr.add(p["pair_address"])
            seen_key.add(key)
            pools.append(p2)

    return pools

# =========================================================
# 랭킹
# =========================================================
def choose_best_pool_per_symbol(pools):
    best = {}
    for p in pools:
        symbol = p["project_symbol"]
        liq = safe_float(p["liquidity_usd"])
        vol = safe_float(p["volume_24h"])

        if symbol not in best:
            best[symbol] = p
            continue

        old = best[symbol]
        old_liq = safe_float(old["liquidity_usd"])
        old_vol = safe_float(old["volume_24h"])

        if (liq > old_liq) or (liq == old_liq and vol > old_vol):
            best[symbol] = p

    return list(best.values())

def integrated_rank_score(pool):
    liq = safe_float(pool["liquidity_usd"])
    vol = safe_float(pool["volume_24h"])
    fdv = safe_float(pool["fdv"])

    score = liq + (vol * 0.6)

    if liq > 0:
        ratio = vol / liq
        if 0.02 <= ratio <= 1.5:
            score += liq * 0.15
        elif ratio < 0.002:
            score -= liq * 0.5

    if liq > 0 and fdv > 0:
        fdv_liq = fdv / liq
        if fdv_liq > 50:
            score -= liq * 0.2

    return score

def build_integrated_top10(dex_pools, polygon_gecko_pools):
    merged = dex_pools + polygon_gecko_pools
    unique = choose_best_pool_per_symbol(merged)
    unique.sort(key=integrated_rank_score, reverse=True)
    return unique[:INTEGRATED_TOP_N]

def build_top3_by_chain(pools, chain, metric_key):
    arr = [p for p in pools if p["chain"] == chain]
    arr.sort(key=lambda x: safe_float(x.get(metric_key)), reverse=True)
    return arr[:3]

def build_chain_leaders_from_pools(all_pools):
    chain_liq = {}
    chain_vol = {}

    for chain in TARGET_CHAINS:
        arr = [p for p in all_pools if p["chain"] == chain]

        if not arr:
            chain_liq[chain] = None
            chain_vol[chain] = None
            continue

        chain_liq[chain] = max(arr, key=lambda x: safe_float(x["liquidity_usd"]))
        chain_vol[chain] = max(arr, key=lambda x: safe_float(x["volume_24h"]))

    return chain_liq, chain_vol

# =========================================================
# 출력
# =========================================================
def format_main_analysis(main_top3):
    lines = ["[메인 분석 TOP 3]"]
    if not main_top3:
        lines.append("- 조건 충족 프로젝트 없음")
        return "\n".join(lines)

    for i, item in enumerate(main_top3, start=1):
        lines.append(f"{i}) {item['name']} ({item['symbol']})")
        lines.append(f"- 판정: {item['verdict']} / 점수: {item['score']}")
        lines.append(f"- 체인: {item['chain']} / 카테고리: {item['category']}")
        lines.append(f"- TVL: {human_money(item['tvl'])}")
        lines.append(f"- 유동성: {human_money(item['liquidity_usd'])}")
        lines.append(f"- 24h 거래량: {human_money(item['volume_24h'])}")
        lines.append(f"- Fees 30d: -")
        lines.append(f"- Revenue 30d: -")
        lines.append(f"- FDV/유동성: -")
        lines.append(f"- 홀더 상위1/5 집중: - / -")
        lines.append(f"- 세금(매수/매도): 0.00% / 0.00%")
        lines.append(f"- 러그 경고: {item['rug_warning']}")
        lines.append("")
    return "\n".join(lines).strip()

def format_real_revenue_mode(protocols):
    lines = ["[진짜 돈 버는 프로토콜 모드 TOP 3]"]
    candidates = []

    for p in protocols:
        symbol = normalize_symbol(p.get("symbol") or "")
        name = p.get("name") or ""
        chain = (p.get("chain") or "").lower()
        category = p.get("category") or "-"
        tvl = safe_float(p.get("tvl"))
        score = protocol_score(p)

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
            "symbol": symbol or name[:6].upper(),
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

    for i, x in enumerate(candidates, start=1):
        lines.append(
            f"{i}) {x['name']} ({x['symbol']}) / 체인: {x['chain']} / 카테고리: {x['category']} / TVL: {human_money(x['tvl'])} / 점수: {x['score']}"
        )
    return "\n".join(lines)

def format_integrated_top10(items):
    lines = ["[통합 투자 TOP 10 - Gecko + Dex / 스테이블·기축 제외]"]
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
            f"{i}) {p['project_symbol']} / 유동성 {human_money(liq)} / 거래량 {human_money(vol)}{tag}"
        )
    return "\n".join(lines)

def format_pool_section(title, pools, sort_type):
    lines = [title]
    if not pools:
        lines.append("- 조건 충족 풀 없음")
        return "\n".join(lines)

    for i, p in enumerate(pools, start=1):
        if sort_type == "liq":
            lines.append(
                f"{i}) {p['project_symbol']} / {p['pair_symbol']} / 유동성 {human_money(p['liquidity_usd'])} / 거래량 {human_money(p['volume_24h'])}"
            )
        else:
            lines.append(
                f"{i}) {p['project_symbol']} / {p['pair_symbol']} / 거래량 {human_money(p['volume_24h'])} / 유동성 {human_money(p['liquidity_usd'])}"
            )
    return "\n".join(lines)

def format_chain_leaders(chain_liq, chain_vol):
    lines = ["[체인별 유동성 1위]"]
    for chain in TARGET_CHAINS:
        p = chain_liq.get(chain)
        if p:
            lines.append(f"- {chain}: {p['project_symbol']} / 유동성 {human_money(p['liquidity_usd'])}")
        else:
            lines.append(f"- {chain}: 기본 필터 통과 프로젝트 없음")

    lines.append("")
    lines.append("[체인별 거래량 1위]")
    for chain in TARGET_CHAINS:
        p = chain_vol.get(chain)
        if p:
            lines.append(f"- {chain}: {p['project_symbol']} / 24h 거래량 {human_money(p['volume_24h'])}")
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
    tokens = fetch_candidate_tokens()

    debug("4단계 - DexScreener 풀 수집")
    dex_pools = collect_dex_pools_from_candidates(tokens)

    debug("5단계 - GeckoTerminal Polygon 풀 수집")
    polygon_gecko_pools = fetch_polygon_gecko_pools()

    debug("6단계 - 통합 TOP10")
    integrated_top10 = build_integrated_top10(dex_pools, polygon_gecko_pools)

    debug("7단계 - 체인별 TOP3")
    polygon_source = [p for p in polygon_gecko_pools if p["chain"] == "polygon"]
    bsc_source = [p for p in dex_pools if p["chain"] == "bsc"]

    polygon_top_liq = build_top3_by_chain(polygon_source, "polygon", "liquidity_usd")
    polygon_top_vol = build_top3_by_chain(polygon_source, "polygon", "volume_24h")

    bsc_top_liq = build_top3_by_chain(bsc_source, "bsc", "liquidity_usd")
    bsc_top_vol = build_top3_by_chain(bsc_source, "bsc", "volume_24h")

    debug("8단계 - 체인 리더")
    all_pool_like = dex_pools + polygon_gecko_pools
    chain_liq, chain_vol = build_chain_leaders_from_pools(all_pool_like)

    debug("9단계 - 메시지 조합")
    parts = [
        "🚀 DeFi 실전 투자 봇 v8.3",
        "",
        format_main_analysis(main_top3),
        "",
        format_real_revenue_mode(protocols),
        "",
        format_integrated_top10(integrated_top10),
        "",
        format_pool_section("[Polygon 유동성 TOP 3 - GeckoTerminal 풀 기준]", polygon_top_liq, "liq"),
        "",
        format_pool_section("[Polygon 거래량 TOP 3 - GeckoTerminal 풀 기준]", polygon_top_vol, "vol"),
        "",
        format_pool_section("[BSC 유동성 TOP 3 - DexScreener 풀 기준]", bsc_top_liq, "liq"),
        "",
        format_pool_section("[BSC 거래량 TOP 3 - DexScreener 풀 기준]", bsc_top_vol, "vol"),
        "",
        format_chain_leaders(chain_liq, chain_vol),
    ]

    final_text = "\n".join(parts)
    send_telegram_message(final_text)
    return final_text

# =========================================================
# 스케줄 실행
# =========================================================
from datetime import datetime
from zoneinfo import ZoneInfo

def now_kst():
    return datetime.now(ZoneInfo("Asia/Seoul"))


if __name__ == "__main__":
    print("프로그램 시작")

    # 🔥 여기 추가
    print("현재 한국시간:", now_kst().strftime("%Y-%m-%d %H:%M:%S"))

    job()  # 1회 실행 확인

    schedule.every().day.at("09:00").do(job)
    schedule.every().day.at("21:00").do(job)

    print("스케줄 대기 시작 (KST 기준 09:00 / 21:00)")

    while True:
        schedule.run_pending()
        time.sleep(20)
