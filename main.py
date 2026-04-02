import os
import requests

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

def send(msg):
    requests.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
                  json={"chat_id": TG_CHAT_ID, "text": msg})

def fmt(x):
    if not x: return "-"
    if x > 1e9: return f"{x/1e9:.2f}B"
    if x > 1e6: return f"{x/1e6:.2f}M"
    if x > 1e3: return f"{x/1e3:.2f}K"
    return f"{x:.2f}"

# 🔥 GeckoTerminal (전체 풀 기반)
def get_gecko_top(chain):
    url = f"https://api.geckoterminal.com/api/v2/networks/{chain}/pools?page=1"

    try:
        data = requests.get(url, timeout=20).json()["data"]
    except:
        return [], []

    pools = []

    for p in data:
        attr = p["attributes"]

        liq = float(attr.get("reserve_in_usd") or 0)
        vol = float(attr.get("volume_usd_24h") or 0)

        if liq < 50000:
            continue

        pools.append({
            "name": attr.get("name"),
            "liq": liq,
            "vol": vol
        })

    top_liq = sorted(pools, key=lambda x: x["liq"], reverse=True)[:3]
    top_vol = sorted(pools, key=lambda x: x["vol"], reverse=True)[:3]

    return top_liq, top_vol

# 🔥 Whale / 유입 감지
def detect_flow(p):
    vol = p["vol"]
    liq = p["liq"]

    signal = ""

    if vol > 1_000_000 and liq > 1_000_000:
        signal = "🔥 고래 유입 강함"

    elif vol > 500_000:
        signal = "📈 거래량 증가"

    return signal

def main():

    msg = "🚀 DeFi 실전 투자 봇 v5\n\n"

    # 🔥 Polygon (진짜 TOP)
    poly_liq, poly_vol = get_gecko_top("polygon_pos")

    msg += "📊 Polygon 유동성 TOP3\n"
    for p in poly_liq:
        msg += f"- {p['name']} / {fmt(p['liq'])}\n"

    msg += "\n📊 Polygon 거래량 TOP3\n"
    for p in poly_vol:
        signal = detect_flow(p)
        msg += f"- {p['name']} / {fmt(p['vol'])} {signal}\n"

    # 🔥 BSC
    bsc_liq, bsc_vol = get_gecko_top("bsc")

    msg += "\n📊 BSC 유동성 TOP3\n"
    for p in bsc_liq:
        msg += f"- {p['name']} / {fmt(p['liq'])}\n"

    msg += "\n📊 BSC 거래량 TOP3\n"
    for p in bsc_vol:
        signal = detect_flow(p)
        msg += f"- {p['name']} / {fmt(p['vol'])} {signal}\n"

    send(msg)

if __name__ == "__main__":
    main()
