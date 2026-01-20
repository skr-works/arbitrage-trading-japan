from __future__ import annotations

import json
import os
import time
import random
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
import yfinance as yf

# ファイルパス
STATE_PATH = Path("state.json")

# ====== 設定 ======
# 1. 裁定買い残
ARB_MA_DAYS = 20
ARB_BUY_RATIO_TH = 1.5

# 2. SQ接近
SQ_NEAR_DAYS = 5

# 3. 価格位置判定（日経平均）
INDEX_LOOKBACK = "3y"
INDEX_PCTL_TH = 0.90
TICKER_PRICE = "^N225"

# URL
IRBANK_URL = "https://irbank.net/market/arbitrage"

# User-Agent (Webブラウザ偽装)
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def fmt_bool(x: bool) -> str:
    return "TRUE" if x else "FALSE"


def fmt_num(x) -> str:
    if x is None:
        return "N/A"
    try:
        return f"{float(x):,.4f}"
    except Exception:
        return str(x)


def pick_level(alert: bool, conds: Dict[str, bool]) -> Tuple[str, str]:
    # 成立している条件の数をカウント (NoneはFalse扱い)
    true_cnt = sum(1 for v in conds.values() if v is True)
    
    if alert:
        return (
            "LEVEL 3: WARNING (警戒)",
            "【警戒】急変しやすい条件が揃っています。ポジション縮小・ヘッジ推奨。",
        )
    if true_cnt >= 2:
        return (
            "LEVEL 2: CAUTION (注意)",
            "【注意】複数の歪みが出ています。レバ/一括エントリーは避けてください。",
        )
    return ("LEVEL 1: NORMAL (正常)", "【順行】構造的な危機シグナルは点灯していません。")


def print_report(latest: Dict):
    idx = latest["inputs"]["index"]
    conds = latest["conditions"]
    metrics = latest["metrics"]
    thr = latest["thresholds"]
    alert = latest["alert"]["volatility_risk"]

    level_title, headline = pick_level(alert, conds)

    print("#" * 60)
    print(f"   {level_title}")
    print("#" * 60)
    print("")
    print("[総合判定メッセージ]")
    print(headline)
    print("")
    print("#" * 60)
    print("=" * 60)
    print("📊 市場構造・急変リスク検知レポート")
    print("=" * 60)
    print(f"AsOf: {latest['asof']}")
    print("")
    print("[入力データ]")
    print(f"- 裁定取引 (IR BANK): {latest['inputs']['arb_date']}")
    print(f"- 指数価格 (Yahoo!) : {idx.get('index_latest_date', '取得失敗')}")
    print("-" * 60)
    print("")

    # 1) Arbitrage
    print("1. 裁定買い残の蓄積 (Arbitrage Stretch)")
    val = metrics['arb_buy_ratio_ma20']
    print(f"   結果: {fmt_num(val)} 倍 (閾値: >= {thr['arb_buy_ratio_ma20_hot']}) → [{fmt_bool(conds['arb_buy_hot'])}]")
    if val is None:
        print("   - データ不足または取得エラー")
    elif conds['arb_buy_hot']:
        print("   - [警戒] 裁定残が積み上がっています。解消売りに注意。")
    else:
        print("   - [正常] 裁定残は許容範囲内です。")
    print("")

    # 2) SQ near
    print(f"2. SQ接近 (SQ Near: 残り{metrics['days_to_2nd_fri']}日)")
    print(f"   結果: [{fmt_bool(conds['sq_near'])}] (閾値: <= {thr['sq_near_days']}日)")
    print("")

    # 3) Liquidity (Skipped based on user request)
    print("3. 市場流動性 (Liquidity)")
    print("   結果: [SKIP] (指数出来高の使用停止指示により判定除外)")
    print("   ※ 判定ロジックから一時的に外しています（常にFALSE扱い）")
    print("")

    # 4) Index high zone
    print(f"4. 指数高値圏 (High Zone: p{int(thr['index_pctl']*100)})")
    print(f"   結果: [{fmt_bool(conds['index_high_zone'])}]")
    if conds['index_high_zone'] is None:
        print("   - データ取得エラー")
    elif conds['index_high_zone']:
        print("   - [警戒] 価格が過去分布の上位に位置しています。")
    else:
        print("   - [中立] 高値圏ではありません。")
    print("")

    print("-" * 60)
    print(f"ALERT_VOLATILITY_RISK = {fmt_bool(alert)}")
    if alert:
        print("理由: " + ", ".join(latest["alert"]["reasons"]))


def sess() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    return s


def load_state() -> Dict:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except:
            pass
    return {
        "meta": {"created_at": datetime.now().isoformat(), "version": 3},
        "history": [],
        "latest": {},
    }


def save_state(state: Dict) -> None:
    state["meta"]["updated_at"] = datetime.now().isoformat()
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def upsert_history(state: Dict, record: Dict) -> None:
    ds = record.get("date")
    if not ds:
        return
        
    hist = state["history"]
    for i, r in enumerate(hist):
        if r.get("date") == ds:
            merged = dict(r)
            for k, v in record.items():
                if v is not None:
                    merged[k] = v
            hist[i] = merged
            break
    else:
        hist.append(record)

    # 日付順ソート & 古いデータ削除
    hist.sort(key=lambda x: x.get("date", ""))
    if len(hist) > MAX_HISTORY_DAYS:
        state["history"] = hist[-MAX_HISTORY_DAYS:]


def parse_japanese_number(s: str) -> float:
    """'10億4878万' -> float"""
    s = s.replace(",", "").strip()
    if not s or s == "-":
        return 0.0
    
    units = {'兆': 10**12, '億': 10**8, '万': 10**4}
    total = 0.0
    current_num = ""
    
    for char in s:
        if char.isdigit() or char == '.':
            current_num += char
        elif char in units:
            if current_num:
                total += float(current_num) * units[char]
                current_num = ""
    if current_num:
        total += float(current_num)
    return total


def fetch_arbitrage_from_irbank(s: requests.Session) -> Tuple[Optional[date], Optional[float], Optional[float]]:
    """
    IR BANKから最新の裁定残（株数）を取得。
    """
    try:
        r = s.get(IRBANK_URL, timeout=20)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text, "html.parser")
        
        header = soup.find(id="c_Shares")
        if not header:
            return None, None, None
        
        table = header.find_next("table")
        if not table:
            return None, None, None
        
        rows = table.find_all("tr")
        current_year = date.today().year
        
        for row in rows:
            # 年取得
            if "occ" in row.get("class", []):
                td = row.find("td")
                if td and td.text.strip().isdigit():
                    current_year = int(td.text.strip())
                continue
            
            # データ行
            td_date = row.find("td", class_="lf")
            if not td_date:
                continue
            
            # [買残, 前比, 売残, 前比]
            cells = row.find_all("td", class_="rt")
            if len(cells) < 3:
                continue
                
            date_str = td_date.get_text(strip=True)
            buy_str = cells[0].get_text(strip=True)
            sell_str = cells[2].get_text(strip=True)
            
            try:
                m, d = map(int, date_str.split("/"))
                data_dt = date(current_year, m, d)
                
                # 株数を千株単位に変換
                buy_val = parse_japanese_number(buy_str) / 1000.0
                sell_val = parse_japanese_number(sell_str) / 1000.0
                
                return data_dt, buy_val, sell_val
            except:
                continue
                
    except Exception as e:
        print(f"[Warning] IR BANK fetch error: {e}")
    
    return None, None, None


def fetch_index_data_with_retry(ticker: str) -> Optional[Dict]:
    """
    Yahoo Financeから価格データを取得（リトライ付き）
    """
    max_retries = 3
    for i in range(max_retries):
        try:
            # 指数出来高は使わないが、価格位置判定のためにCloseは必要
            df = yf.download(ticker, period=INDEX_LOOKBACK, interval="1d", progress=False)
            
            if df is None or df.empty:
                # 空の場合は少し待ってリトライ
                time.sleep(2)
                continue
                
            # マルチインデックスカラム対策 (yfinance v0.2.x以降)
            if isinstance(df.columns, pd.MultiIndex):
                try:
                    # 'Close' カラム配下の ticker 名を取得してSeries化
                    close = df["Close"][ticker]
                except KeyError:
                    # 構造が違う場合、単にCloseを取ってみる
                    close = df["Close"]
            else:
                close = df["Close"]

            close = close.dropna()
            if close.empty:
                return None

            latest_close = float(close.iloc[-1])
            q = float(close.quantile(INDEX_PCTL_TH))
            
            return {
                "ticker": ticker,
                "latest_close": latest_close,
                "threshold_close": q,
                "index_high_zone": (latest_close >= q),
                "index_latest_date": close.index[-1].date().isoformat(),
            }
            
        except Exception as e:
            print(f"[Warning] YFinance retry {i+1}/{max_retries} failed: {e}")
            time.sleep(3 + i * 2)  # Backoff
            
    print(f"[Error] Failed to fetch data for {ticker} after retries.")
    return None


def get_days_to_sq(today: date) -> int:
    y, m = today.year, today.month
    first_day = date(y, m, 1)
    # 0=Mon, 4=Fri
    first_fri_day = (4 - first_day.weekday() + 7) % 7 + 1
    second_fri_day = first_fri_day + 7
    sq_date = date(y, m, second_fri_day)
    
    if today > sq_date:
        # 翌月のSQ
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
        first_day = date(y, m, 1)
        first_fri_day = (4 - first_day.weekday() + 7) % 7 + 1
        sq_date = date(y, m, first_fri_day + 7)
        
    return (sq_date - today).days


def compute_latest(state: Dict, index_info: Optional[Dict]) -> Dict:
    hist = state["history"]
    
    # --- 1. 裁定残 (Average Ratio) ---
    arb_days = [r for r in hist if isinstance(r.get("arb_buy"), (int, float))]
    arb_days.sort(key=lambda x: x["date"])
    
    arb_buy_hot = False
    arb_ratio = None
    
    inputs_arb = {"arb_date": None, "arb_buy": None, "arb_sell": None}
    
    if arb_days:
        latest_r = arb_days[-1]
        inputs_arb = {
            "arb_date": latest_r["date"],
            "arb_buy": float(latest_r["arb_buy"]),
            "arb_sell": float(latest_r["arb_sell"]),
        }
        
        # 過去データの平均を算出
        vals = [float(r["arb_buy"]) for r in arb_days[-MA_DAYS:]]
        if len(vals) >= 1: # データがあれば計算する
            ma = sum(vals) / len(vals)
            if ma > 0:
                arb_ratio = float(latest_r["arb_buy"]) / ma
                arb_buy_hot = (arb_ratio >= ARB_BUY_RATIO_TH)

    # --- 2. SQ接近 ---
    today = date.today()
    d2sq = get_days_to_sq(today)
    sq_near = (d2sq <= SQ_NEAR_DAYS)

    # --- 3. 流動性 (SKIP) ---
    # ユーザー指示により指数の出来高は使わない。
    # 代替手段がないため、この判定は常に False (リスク要因ではない) とする。
    prime_vol_thin = False 
    vol_ratio = None

    # --- 4. 指数高値圏 ---
    idx_high = False
    idx_dict = index_info if index_info else {}
    if index_info:
        idx_high = index_info.get("index_high_zone", False)

    # --- 総合判定 ---
    # 条件: 裁定買い残大 & SQ接近 & (流動性薄) & 高値圏
    # 流動性はSKIPなので、実質3条件 or 流動性無視
    # 仕様書通りなら "AND" だが、流動性データがないためそこはTrueとみなすか？
    # -> 安全側に倒して「流動性が薄い」判定は出さない（Alertになりにくくする）
    
    # アラートロジック:
    # Liquidity判定ができないので、それ以外の3つが揃ったらALERTとする、あるいはLEVEL2止まりにする。
    # ここでは「流動性判定を除いた3要素」で判定する。
    alert = arb_buy_hot and sq_near and idx_high
    
    reasons = []
    if arb_buy_hot: reasons.append("裁定買い残過剰")
    if sq_near: reasons.append("SQ接近")
    if prime_vol_thin: reasons.append("流動性低下")
    if idx_high: reasons.append("指数高値圏")

    return {
        "asof": datetime.now().astimezone().isoformat(),
        "inputs": {
            **inputs_arb,
            "prime_volume_date": None,
            "prime_volume": None,
            "index": idx_dict,
        },
        "metrics": {
            "arb_buy_ratio_ma20": arb_ratio,
            "prime_volume_ratio_ma20": vol_ratio,
            "days_to_2nd_fri": d2sq,
        },
        "thresholds": {
            "arb_buy_ratio_ma20_hot": ARB_BUY_RATIO_TH,
            "sq_near_days": SQ_NEAR_DAYS,
            "index_pctl": INDEX_PCTL_TH,
        },
        "conditions": {
            "arb_buy_hot": arb_buy_hot,
            "sq_near": sq_near,
            "prime_volume_thin": prime_vol_thin,
            "index_high_zone": idx_high,
        },
        "alert": {
            "volatility_risk": alert,
            "reasons": reasons,
        }
    }


def main():
    s = sess()
    state = load_state()

    # 1. IR BANK取得
    try:
        dt, buy, sell = fetch_arbitrage_from_irbank(s)
        if dt:
            upsert_history(state, {
                "date": dt.isoformat(),
                "arb_buy": buy,
                "arb_sell": sell,
                "arb_net": buy - sell if (buy and sell) else None,
                "src": "irbank"
            })
    except Exception as e:
        print(f"IR BANK process failed: {e}")

    # 2. 指数データ取得 (YFinance)
    # 失敗してもスクリプトを止めない
    index_info = fetch_index_data_with_retry(TICKER_PRICE)

    # 3. 判定 & 保存
    try:
        latest = compute_latest(state, index_info)
        state["latest"] = latest
        
        # ファイル保存は最後に行う (Gitエラー防止のため必ず作成)
        save_state(state)
        
        # レポート出力
        print_report(latest)
        
    except Exception as e:
        print(f"Compute/Save failed: {e}")
        # 万が一のときもstate.jsonだけは更新しておく(タイムスタンプのみ)
        save_state(state)

if __name__ == "__main__":
    main()
