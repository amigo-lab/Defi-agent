import os
import json
import requests
from typing import Any, Dict, List, Optional

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

LLAMA_PROTOCOLS_URL = "https://api.llama.fi/protocols"
DEX_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search"

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

ALLOWED_CATEGORIES = {
    "Dexes",
    "Lending",
    "Yield",
    "Derivatives",
    "Bridge",
    "CDP",
    "RWA",
}

# BSC / Polygon 별도 모니터링용 검색 키워드
BSC_SEARCH_TERMS = [
    "PancakeSwap",
    "Venus",
    "THENA",
    "Lista",
    "Biswap",
    "Wombat",
    "Helio",
    "Aster",
]

POLYGON_SEARCH_TERMS = [
    "LGNS",
    "QuickSwap",
    "Aave",
    "Uniswap",
    "Balancer",
    "Curve",
    "Sushi",
    "Kyber",
]


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


def get_llama_protocols() -> List[Dict[str, Any]]:
    r = requests.get(LLAMA_PROTOCOLS_URL, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise ValueError("DefiLlama 응답 형식 오류")
    return data


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


def filter_llama_protocols(protocols: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    result = []
    for p in protocols:
        category = p.get("category")
        tvl = to_float(p.get("tvl"))
        chains = [c.lower() for c in (p.get("chains") or [])]

        if category not in ALLOWED_CATEGORIES:
            continue
        if tvl < 10_000_000:
            continue
        if not any(c in ALLOWED_CHAINS for c in chains):
            continue

        result.append(p)

    return sorted(result, key=lambda x: to_float(x.get("tvl")), reverse=True)


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
        pairs = search_dex_pairs(q)
        all_pairs.extend(pairs)

    filtered_pairs = []
    allowed_protocol_chains = {c.lower() for c in (protocol.get("chains") or [])}

    for pair in all_pairs:
        chain_id = (pair.get("chainId") or "").lower()
        if chain_id not in ALLOWED_CHAINS:
            continue
        if allowed_protocol_chains and chain_id not in allowed_protocol_chains:
            continue

        liquidity_usd = to_float((pair.get("liquidity") or {}).get("usd"))
        volume_24h = to_float((pair.get("volume") or {}).get("h24"))

        if liquidity_usd < 100_000:
            continue
        if volume_24h < 50_000:
            continue

        filtered_pairs.append(pair)

    if not filtered_pairs:
        return None

    return max(
        filtered_pairs,
        key=lambda p: (
            to_float((p.get("liquidity") or {}).get("usd")),
            to_float((p.get("volume") or {}).get("h24")),
        ),
    )


def analyze_project(protocol: Dict[str, Any], pair: Dict[str, Any]) -> Dict[str, Any]:
    liquidity_usd = to_float((pair.get("liquidity") or {}).get("usd"))
    volume_24h = to_float((pair.get("volume") or {}).get("h24"))
    fdv = to_float(pair.get("fdv"))
    market_cap = to_float(pair.get("marketCap"))
    price_usd = to_float(pair.get("priceUsd"), None)

    tvl = to_float(protocol.get("tvl"))
    category = protocol.get("category") or "-"
    chain_id = (pair.get("chainId") or "").lower()
    dex_id = pair.get("dexId") or "-"
    pair_url = pair.get("url") or "-"

    fdv_liquidity_ratio = (fdv / liquidity_usd) if liquidity_usd > 0 and fdv > 0 else 0.0
    volume_liquidity_ratio = (volume_24h / liquidity_usd) if liquidity_usd > 0 else 0.0
    tvl_liquidity_ratio = (tvl / liquidity_usd) if liquidity_usd > 0 and tvl > 0 else 0.0

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

    if fdv_liquidity_ratio > 20:
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

    if category in {"Dexes", "Lending", "Derivatives", "Bridge"}:
        score += 3
        reasons.append("주요 DeFi 카테고리")

    if score >= 85:
        grade = "관심"
        verdict = "진짜 디파이 후보로 우선 검토"
    elif score >= 70:
        grade = "관찰"
        verdict = "추가 확인할 가치 있음"
    else:
        grade = "위험"
        verdict = "시장 구조 또는 밸류 부담 존재"

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
        "dex_id": dex_id,
        "pair_url": pair_url,
        "score": max(score, 0),
        "grade": grade,
        "verdict": verdict,
        "reasons": reasons[:6],
    }


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


def compare_leaders(
    prev_state: Dict[str, Any],
    current_leaders: Dict[str, Dict[str, Dict[str, Any]]]
) -> List[str]:
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


def deduplicate_monitor_projects(projects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: List[Dict[str, Any]] = []
    seen = set()

    for p in projects:
        key = (
            (p.get("name") or "").lower(),
            (p.get("symbol") or "").lower(),
            p.get("pair_url") or "",
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(p)

    return deduped


def build_chain_top3_from_search(chain_name: str, search_terms: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    projects = []

    for term in search_terms:
        pairs = search_dex_pairs(term)

        for pair in pairs:
            pair_chain = (pair.get("chainId") or "").lower()
            if pair_chain != chain_name:
                continue

            liquidity_usd = to_float((pair.get("liquidity") or {}).get("usd"))
            volume_24h = to_float((pair.get("volume") or {}).get("h24"))

            # 별도 모니터링은 너무 빡빡하지 않게 최소 조건만
            if liquidity_usd < 10_000 and volume_24h < 5_000:
                continue

            base = pair.get("baseToken") or {}

            project = {
                "name": base.get("name") or "-",
                "symbol": base.get("symbol") or "-",
                "liquidity_usd": liquidity_usd,
                "volume_24h": volume_24h,
                "fdv": to_float(pair.get("fdv")),
                "market_cap": to_float(pair.get("marketCap")),
                "price_usd": to_float(pair.get("priceUsd"), None),
                "pair_url": pair.get("url") or "-",
            }

            projects.append(project)

    projects = deduplicate_monitor_projects(projects)

    top_liquidity = sorted(projects, key=lambda x: x["liquidity_usd"], reverse=True)[:3]
    top_volume = sorted(projects, key=lambda x: x["volume_24h"], reverse=True)[:3]

    return {
        "liquidity": top_liquidity,
        "volume": top_volume,
    }


def build_message(
    projects: List[Dict[str, Any]],
    leaders: Dict[str, Dict[str, Dict[str, Any]]],
    changes: List[str]
) -> str:
    top_projects = sorted(projects, key=lambda x: x["score"], reverse=True)[:3]

    lines = []
    lines.append("[오늘의 진짜 디파이 투자 분석 결과]")
    lines.append("")

    if not top_projects:
        lines.append("조건을 통과한 프로젝트가 없습니다.")
    else:
        for idx, p in enumerate(top_projects, start=1):
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
            lines.append(f"- FDV/유동성: {p['fdv_liquidity_ratio']:.2f}")
            lines.append(f"- 거래량/유동성: {p['volume_liquidity_ratio']:.2f}")
            lines.append(f"- TVL/유동성: {p['tvl_liquidity_ratio']:.2f}")
            lines.append(f"- DEX: {p['dex_id']}")
            lines.append(f"- 핵심 이유: {', '.join(p['reasons'])}")
            lines.append(f"- 링크: {p['pair_url']}")
            lines.append("")

    lines.append("[체인별 유동성 1위]")
    for chain in ["ethereum", "arbitrum", "base", "bsc", "polygon"]:
        data = leaders.get(chain, {})
        if "liquidity" in data:
            p = data["liquidity"]
            lines.append(f"- {chain}: {p['name']} / 유동성 {fmt_num(p['liquidity_usd'])}")
        else:
            lines.append(f"- {chain}: 조건 통과 프로젝트 없음")

    lines.append("")
    lines.append("[체인별 거래량 1위]")
    for chain in ["ethereum", "arbitrum", "base", "bsc", "polygon"]:
        data = leaders.get(chain, {})
        if "volume" in data:
            p = data["volume"]
            lines.append(f"- {chain}: {p['name']} / 24h 거래량 {fmt_num(p['volume_24h'])}")
        else:
            lines.append(f"- {chain}: 조건 통과 프로젝트 없음")

    bsc_top3 = build_chain_top3_from_search("bsc", BSC_SEARCH_TERMS)
    polygon_top3 = build_chain_top3_from_search("polygon", POLYGON_SEARCH_TERMS)

    lines.append("")
    lines.append("[BSC 별도 모니터링 - 유동성 TOP 3]")
    if bsc_top3["liquidity"]:
        for i, p in enumerate(bsc_top3["liquidity"], start=1):
            lines.append(
                f"{i}) {p['name']} ({p['symbol']}) / 유동성 {fmt_num(p['liquidity_usd'])} / 거래량 {fmt_num(p['volume_24h'])}"
            )
    else:
        lines.append("- 후보 없음")

    lines.append("")
    lines.append("[BSC 별도 모니터링 - 거래량 TOP 3]")
    if bsc_top3["volume"]:
        for i, p in enumerate(bsc_top3["volume"], start=1):
            lines.append(
                f"{i}) {p['name']} ({p['symbol']}) / 거래량 {fmt_num(p['volume_24h'])} / 유동성 {fmt_num(p['liquidity_usd'])}"
            )
    else:
        lines.append("- 후보 없음")

    lines.append("")
    lines.append("[Polygon 별도 모니터링 - 유동성 TOP 3]")
    if polygon_top3["liquidity"]:
        for i, p in enumerate(polygon_top3["liquidity"], start=1):
            lines.append(
                f"{i}) {p['name']} ({p['symbol']}) / 유동성 {fmt_num(p['liquidity_usd'])} / 거래량 {fmt_num(p['volume_24h'])}"
            )
    else:
        lines.append("- 후보 없음")

    lines.append("")
    lines.append("[Polygon 별도 모니터링 - 거래량 TOP 3]")
    if polygon_top3["volume"]:
        for i, p in enumerate(polygon_top3["volume"], start=1):
            lines.append(
                f"{i}) {p['name']} ({p['symbol']}) / 거래량 {fmt_num(p['volume_24h'])} / 유동성 {fmt_num(p['liquidity_usd'])}"
            )
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
            {
                "name": p["name"],
                "score": p["score"],
                "chain": p["chain"],
            }
            for p in sorted(projects, key=lambda x: x["score"], reverse=True)[:3]
        ],
    }
    save_state(new_state)


if __name__ == "__main__":
    main()
