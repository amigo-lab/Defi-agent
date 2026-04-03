import os
import json
import requests
from typing import Any, Dict, List

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

LLAMA = "https://api.llama.fi/protocols"
DEX = "https://api.dexscreener.com/latest/dex/search"

# 🔥 밈/잡코인 필터
BAD_KEYWORDS = [
    "doge","inu","baby","banana","pepe","elon","cat","shib"
]

# 🔥 수익 기준 완화
MIN_FEES = 50_000
MIN_REV = 10_000
MIN_LIQ = 150_000

def send(msg):
    requests.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
                  json={"chat_id": TG_CHAT_ID, "text": msg})

def f(x):
    try: return float(x)
    except: return 0

def fmt(x):
    if not x: return "-"
    if x>1e9: return f"{x/1e9:.2f}B"
    if x>1e6: return f"{x/1e6:.2f}M"
    if x>1e3: return f"{x/1e3:.2f}K"
    return f"{x:.2f}"

def is_bad(name):
    n=name.lower()
    return any(k in n for k in BAD_KEYWORDS)

def get_protocols():
    return requests.get(LLAMA).json()

def search(q):
    return requests.get(DEX, params={"q": q}).json().get("pairs", [])

def analyze(p, pair):
    liq=f(pair.get("liquidity",{}).get("usd"))
    vol=f(pair.get("volume",{}).get("h24"))
    fdv=f(pair.get("fdv"))
    tvl=f(p.get("tvl"))

    fees=f(p.get("fees30d"))
    rev=f(p.get("revenue30d"))

    score=100

    if liq<200k: score-=10
    if vol<150k: score-=10
    if liq>0 and fdv/liq>20: score-=20

    if not fees: score-=10
    if not rev: score-=15

    return {
        "name":p["name"],
        "symbol":p.get("symbol"),
        "liq":liq,
        "vol":vol,
        "fdv":fdv,
        "tvl":tvl,
        "fees":fees,
        "rev":rev,
        "score":score,
        "url":pair.get("url"),
        "chain":pair.get("chainId")
    }

def is_real(p):
    return (
        p["fees"]>=MIN_FEES and
        p["rev"]>=MIN_REV and
        p["liq"]>=MIN_LIQ
    )

# 🔥 중복 제거 (토큰당 1개)
def dedupe(items):
    best={}
    for p in items:
        key=p["name"]
        if key not in best or p["liq"]>best[key]["liq"]:
            best[key]=p
    return list(best.values())

def build_chain(chain, terms):
    data=[]
    for t in terms:
        for p in search(t):
            if p.get("chainId")!=chain:
                continue
            name=p.get("baseToken",{}).get("name","")
            if is_bad(name):
                continue

            data.append({
                "name":name,
                "symbol":p.get("baseToken",{}).get("symbol"),
                "liq":f(p.get("liquidity",{}).get("usd")),
                "vol":f(p.get("volume",{}).get("h24"))
            })

    data=dedupe(data)

    liq_top=sorted(data,key=lambda x:x["liq"],reverse=True)[:3]
    vol_top=sorted(data,key=lambda x:x["vol"],reverse=True)[:3]

    return liq_top, vol_top

def main():

    ps=get_protocols()
    result=[]

    for p in ps[:40]:
        pairs=search(p["name"])
        if not pairs: continue

        pair=pairs[0]

        if f(pair.get("liquidity",{}).get("usd"))<100000:
            continue

        result.append(analyze(p,pair))

    # 🔥 메인 필터 (위험 제거)
    safe=[x for x in result if x["score"]>=70]

    top=sorted(safe,key=lambda x:x["score"],reverse=True)[:3]

    earners=[x for x in result if is_real(x)]
    earners=sorted(earners,key=lambda x:x["rev"],reverse=True)[:3]

    msg="📊 오늘의 디파이 투자 분석\n\n"

    # 메인
    if not top:
        msg+="❗ 오늘은 투자 후보 없음\n\n"
    else:
        for i,p in enumerate(top,1):
            msg+=f"{i}) {p['name']} ({p['symbol']})\n"
            msg+=f"점수: {p['score']}\n"
            msg+=f"TVL: {fmt(p['tvl'])}\n"
            msg+=f"유동성: {fmt(p['liq'])}\n"
            msg+=f"거래량: {fmt(p['vol'])}\n\n"

    # 🔥 진짜 돈 버는 프로토콜
    msg+="💰 돈 버는 프로토콜\n"
    if not earners:
        msg+="- 없음\n\n"
    else:
        for p in earners:
            msg+=f"- {p['name']} / Revenue {fmt(p['rev'])}\n"
        msg+="\n"

    # 🔥 체인 모니터링
    BSC=["Pancake","Venus","THENA","Lista"]
    POLY=["LGNS","QuickSwap","Aave","Curve"]

    b_liq,b_vol=build_chain("bsc",BSC)
    p_liq,p_vol=build_chain("polygon",POLY)

    msg+="📊 BSC 유동성 TOP3\n"
    for x in b_liq:
        msg+=f"- {x['name']} {fmt(x['liq'])}\n"

    msg+="\n📊 Polygon 유동성 TOP3\n"
    for x in p_liq:
        msg+=f"- {x['name']} {fmt(x['liq'])}\n"

    send(msg)

if __name__=="__main__":
    main()
