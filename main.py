"""
日本株市場「歪み破裂リスク検知システム」 (Distortion Burst Risk Detection)
仕様書バージョン: 確定版
Target Python: 3.11
"""

import json
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup

# ==========================================
# 0. 設定・定数定義 (Configuration)
# ==========================================

STATE_PATH = Path("state.json")
MAX_HISTORY_YEARS = 5
MAX_HISTORY_DAYS = int(245 * MAX_HISTORY_YEARS)  # 約1225日

# 閾値設定
VOL_MA_DAYS = 20

# SQ設定
SQ_NEAR_THRESHOLD = 5  # 日 (暦日)
MAJOR_SQ_MONTHS = {3, 6, 9, 12}

# 流動性不整合 (仕様書 4.3.2)
VOL_RATIO_THIN = 0.85
P_MOVE_TH = 1.0  # %
P_SPIKE_TH = 2.0  # %

# TOPIX位置 (仕様書 4.4.1)
INDEX_LOOKBACK = "3y"
INDEX_MIN_DATAPOINTS = 500
PCTL_HIGH = 0.90
DEV200_HIGH = 0.08
PCTL_LOW = 0.10
DEV200_LOW = -0.08

# URL / Tickers
URL_IRBANK = "https://irbank.net/market/arbitrage"
URL_NIKKEI = "https://www.nikkei.com/markets/kabu/japanidx/"
TICKER_TOPX = "^TOPX"  # TOPIX (Main)
TICKER_N225 = "^N225"  # Nikkei 225 (Sub)
TICKER_FUT = "NK=F"    # Nikkei 225 Futures (Sub)

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# ==========================================
# 1. ユーティリティ (Utilities)
# ==========================================

def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    return s

def parse_jp_num(s: str) -> float:
    """日本語単位（兆, 億, 万）を含む文字列を数値(float)に変換"""
    s = str(s).replace(",", "").strip()
    if not s or s in ["-", "--", "－"]:
        return 0.0
    
    units = {'兆': 10**12, '億': 10**8, '万': 10**4}
    total = 0.0
    current_num = ""
    
    try:
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
    except ValueError:
        return 0.0

def load_state() -> Dict[str, Any]:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"history": [], "meta": {"version": 2}}

def save_state(state: Dict[str, Any]):
    if "history" in state:
        state["history"].sort(key=lambda x: x.get("date", ""))
        if len(state["history"]) > MAX_HISTORY_DAYS:
            state["history"] = state["history"][-MAX_HISTORY_DAYS:]
    
    try:
        STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[ERROR] Failed to save state: {e}", file=sys.stderr)

# ==========================================
# 2. データ取得 (Data Fetching)
# ==========================================

def fetch_irbank_arbitrage(s: requests.Session) -> Optional[Dict[str, Any]]:
    """IRBankから裁定残高(Net)を取得"""
    try:
        r = s.get(URL_IRBANK, timeout=20)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text, "html.parser")

        header = soup.find(id="c_Shares")
        if not header: return None
        table = header.find_next("table")
        if not table: return None
        
        rows = table.find_all("tr")
        current_year = date.today().year
        
        for row in rows:
            if "occ" in row.get("class", []):
                td = row.find("td")
                if td and td.text.strip().isdigit():
                    current_year = int(td.text.strip())
                continue
            
            td_date = row.find("td", class_="lf")
            if not td_date: continue
            cells = row.find_all("td", class_="rt")
            if len(cells) < 3: continue
            
            date_str = td_date.get_text(strip=True)
            if "/" not in date_str: continue

            try:
                m, d = map(int, date_str.split("/"))
                dt = date(current_year, m, d)
                if dt > date.today() + timedelta(days=7):
                    dt = date(current_year - 1, m, d)
                
                buy_val = parse_jp_num(cells[0].get_text(strip=True))
                sell_val = parse_jp_num(cells[2].get_text(strip=True))
                
                if buy_val == 0.0 and sell_val == 0.0: continue

                return {
                    "date": dt.isoformat(),
                    "arb_buy": int(buy_val),
                    "arb_sell": int(sell_val),
                    "arb_net": int(buy_val - sell_val)
                }
            except Exception:
                continue
    except Exception as e:
        print(f"[WARN] IRBank fetch failed: {e}", file=sys.stderr)
    return None

def fetch_nikkei_market(s: requests.Session) -> Optional[Dict[str, Any]]:
    """日経からプライム売買高を取得"""
    try:
        r = s.get(URL_NIKKEI, timeout=20)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text, "html.parser")
        
        tables = soup.find_all("table")
        for tbl in tables:
            th = tbl.find("th", string=re.compile("売買高"))
            if th:
                tds = th.find_parent("tr").find_all("td")
                if tds:
                    vol_str = tds[0].get_text(strip=True)
                    vol_val = parse_jp_num(vol_str)
                    if vol_val > 0:
                        return {"prime_volume": int(vol_val)}
    except Exception as e:
        print(f"[WARN] Nikkei fetch failed: {e}", file=sys.stderr)
    return None

def fetch_yfinance_data() -> Dict[str, Any]:
    """yfinanceから価格データを取得 (TOPIX, N225, Futures)"""
    data = {}
    tickers = f"{TICKER_TOPX} {TICKER_N225} {TICKER_FUT}"
    try:
        df = yf.download(tickers, period=INDEX_LOOKBACK, interval="1d", progress=False, auto_adjust=False)
        if df.empty: return {}

        adj_close = df["Close"] if "Close" in df else df
        if len(adj_close) < 2: return {}

        # 共通処理: 前日比計算用
        def get_series(ticker):
            if ticker in adj_close.columns:
                return adj_close[ticker].dropna()
            return None

        # TOPIX
        s_topx = get_series(TICKER_TOPX)
        if s_topx is not None:
            data["topx_history"] = s_topx
            data["topx_latest"] = float(s_topx.iloc[-1])
            data["topx_prev"] = float(s_topx.iloc[-2])
            # 前日比%
            data["topx_chg_pct"] = ((data["topx_latest"] / data["topx_prev"]) - 1) * 100

        # N225 (補助)
        s_n225 = get_series(TICKER_N225)
        if s_n225 is not None:
            data["n225_latest"] = float(s_n225.iloc[-1])
            data["n225_prev"] = float(s_n225.iloc[-2])
            data["n225_chg_pct"] = ((data["n225_latest"] / data["n225_prev"]) - 1) * 100
            data["n225_history"] = s_n225 # Basis計算用

        # Futures (Basis)
        s_fut = get_series(TICKER_FUT)
        if s_fut is not None:
            data["fut_history"] = s_fut

    except Exception as e:
        print(f"[WARN] yfinance fetch failed: {e}", file=sys.stderr)
    
    return data

# ==========================================
# 3. ロジック & 判定 (Logic & Evaluation)
# ==========================================

class MarketAnalyzer:
    def __init__(self, state: Dict[str, Any], ir_data: Optional[Dict], nk_data: Optional[Dict], yf_data: Dict):
        self.state = state
        self.ir_data = ir_data
        self.nk_data = nk_data
        self.yf_data = yf_data
        
        self.today = date.today()
        self.result = {
            "date": self.today.isoformat(),
            "level": "INFO", # Default
            "conditions": {},
            "metrics": {}
        }

    def update_state(self) -> bool:
        """データ更新・保存判定 (仕様書 3.3)"""
        if not self.ir_data or not self.nk_data:
            print("[INFO] Data missing. Skip.")
            return False

        data_date_str = self.ir_data["date"]
        data_date = date.fromisoformat(data_date_str)
        
        if data_date > self.today: return False
        
        hist = self.state["history"]
        if hist:
            last = hist[-1]
            if last.get("date") == data_date_str:
                # 同値チェック
                if (last.get("arb_net") == self.ir_data["arb_net"] and 
                    last.get("prime_volume") == self.nk_data["prime_volume"]):
                    print(f"[INFO] Data for {data_date_str} unchanged. Skip.")
                    return False
                else:
                    hist.pop() # 更新のため削除

        record = {
            "date": data_date_str,
            "arb_buy": self.ir_data["arb_buy"],
            "arb_sell": self.ir_data["arb_sell"],
            "arb_net": self.ir_data["arb_net"],
            "prime_volume": self.nk_data["prime_volume"]
        }
        hist.append(record)
        return True

    def _get_history_df(self) -> pd.DataFrame:
        df = pd.DataFrame(self.state["history"])
        if not df.empty and "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date").sort_index()
        return df

    def evaluate(self):
        df = self._get_history_df()
        if df.empty: return

        latest = df.iloc[-1]
        
        # --- 4.1 裁定残高ロジック (ARB_STUCK) ---
        arb_stuck = False
        delta3, delta5, delta25 = 0, 0, 0
        
        if len(df) >= 26:
            curr = latest["arb_net"]
            delta3 = curr - df["arb_net"].iloc[-4] # t-3 (index -4)
            delta5 = curr - df["arb_net"].iloc[-6]
            delta25 = curr - df["arb_net"].iloc[-26]
            
            # 定義: Δ5 >= 0 かつ Δ25 > 0
            if delta5 >= 0 and delta25 > 0:
                arb_stuck = True
        
        # --- 4.2 SQロジック ---
        d2sq = self._calc_days_to_sq(self.today)
        sq_month = (self.today + timedelta(days=d2sq)).month
        is_major_sq = sq_month in MAJOR_SQ_MONTHS
        
        # 条件: D2SQ <= 5 かつ メジャーSQ
        major_sq_near = (d2sq <= SQ_NEAR_THRESHOLD) and is_major_sq

        # --- 4.3 流動性ロジック (LIQ_MISMATCH) ---
        liq_mismatch = False
        vol_ratio = 0.0
        px_move = 0.0
        
        if len(df) >= VOL_MA_DAYS and "prime_volume" in latest:
            # MA20
            vol_ma = df["prime_volume"].iloc[-VOL_MA_DAYS:].mean()
            vol_ratio = latest["prime_volume"] / vol_ma
            
            # 変動率 (TOPIX優先, なければN225)
            if "topx_chg_pct" in self.yf_data:
                px_move = abs(self.yf_data["topx_chg_pct"])
            elif "n225_chg_pct" in self.yf_data:
                px_move = abs(self.yf_data["n225_chg_pct"])
            
            # 不整合判定
            # 1. 薄いのに動く
            cond1 = (vol_ratio <= 0.85) and (px_move >= P_MOVE_TH)
            # 2. 普通なのに飛ぶ
            cond2 = (vol_ratio > 0.85) and (px_move >= P_SPIKE_TH)
            
            if cond1 or cond2:
                liq_mismatch = True

        # --- 4.4 価格ロジック ---
        idx_high_topix = False
        basis_stuck = False
        pctl, dev200 = 0.0, 0.0
        
        # A) TOPIX位置
        if "topx_history" in self.yf_data:
            s = self.yf_data["topx_history"]
            if len(s) >= INDEX_MIN_DATAPOINTS:
                curr = self.yf_data["topx_latest"]
                # PCTL
                pctl = (s < curr).mean()
                # DEV200
                ma200 = s.rolling(200).mean().iloc[-1]
                if ma200 > 0:
                    dev200 = (curr / ma200) - 1
                    
                    # 高値圏: PCTL >= 0.90 AND DEV200 >= +0.08
                    if pctl >= PCTL_HIGH and dev200 >= DEV200_HIGH:
                        idx_high_topix = True

        # B) 裁定ストレス (Basis)
        if "n225_history" in self.yf_data and "fut_history" in self.yf_data:
            # 直近5営業日のBasis絶対値推移
            s_n225 = self.yf_data["n225_history"]
            s_fut = self.yf_data["fut_history"]
            common = s_n225.index.intersection(s_fut.index)
            if len(common) >= 5:
                # 日付アライメント
                basis_series = (s_fut.loc[common] - s_n225.loc[common]).abs().tail(5)
                # "縮小しない" 判定 (簡易実装: 傾きが非負、または平均より最新が大きい)
                # ここでは「最新値 >= 期間平均」を「縮まっていない」とみなす
                if basis_series.iloc[-1] >= basis_series.mean():
                    basis_stuck = True

        # --- 5. 総合判定 (仕様書 5.1, 5.2) ---
        
        # 価格側の補助条件 (いずれか必須)
        price_condition = idx_high_topix or basis_stuck
        
        # WARNING (LEVEL 3)
        # 1. ARB_STUCK
        # 2. MAJOR_SQ_NEAR
        # 3. LIQ_MISMATCH
        # 4. PRICE_CONDITION
        is_warning = arb_stuck and major_sq_near and liq_mismatch and price_condition
        
        # CAUTION (LEVEL 2)
        # ARB_STUCK + LIQ_MISMATCH + (SQ or Price)
        is_caution = arb_stuck and liq_mismatch and (major_sq_near or price_condition)
        
        level = "LEVEL 1: NORMAL"
        if is_warning:
            level = "LEVEL 3: WARNING"
        elif is_caution:
            level = "LEVEL 2: CAUTION"

        self.result.update({
            "level": level,
            "conditions": {
                "ARB_STUCK": arb_stuck,
                "MAJOR_SQ_NEAR": major_sq_near,
                "LIQ_MISMATCH": liq_mismatch,
                "IDX_HIGH_TOPIX": idx_high_topix,
                "BASIS_STUCK_NK": basis_stuck
            },
            "metrics": {
                "arb_net": int(latest["arb_net"]),
                "delta3": int(delta3),
                "delta5": int(delta5),
                "delta25": int(delta25),
                "days_to_sq": d2sq,
                "vol_ratio": round(vol_ratio, 2),
                "px_move": round(px_move, 2),
                "pctl": round(pctl, 2),
                "dev200": round(dev200, 3)
            }
        })

    def _calc_days_to_sq(self, base_date: date) -> int:
        y, m = base_date.year, base_date.month
        def get_2nd_fri(yr, mo):
            first = date(yr, mo, 1)
            # 0=Mon, 4=Fri
            return first + timedelta(days=((4 - first.weekday() + 7) % 7) + 7)
        
        sq = get_2nd_fri(y, m)
        if base_date > sq:
            if m == 12: sq = get_2nd_fri(y + 1, 1)
            else: sq = get_2nd_fri(y, m + 1)
        return (sq - base_date).days

    def report(self):
        r = self.result
        c = r["conditions"]
        m = r["metrics"]
        
        print("=" * 60)
        print(f"日本株市場 構造歪み検知レポート (Date: {r['date']})")
        print(f"判定: {r['level']}")
        print("-" * 60)
        print(f"[1] 裁定スタック (ARB_STUCK): {c['ARB_STUCK']}")
        print(f"    Net: {m.get('arb_net'):,} / Δ5: {m.get('delta5'):,} / Δ25: {m.get('delta25'):,}")
        print(f"    (Δ3: {m.get('delta3'):,})")
        
        print(f"[2] メジャーSQ接近 (MAJOR_SQ_NEAR): {c['MAJOR_SQ_NEAR']}")
        print(f"    残り {m.get('days_to_sq')} 日")
        
        print(f"[3] 流動性不整合 (LIQ_MISMATCH): {c['LIQ_MISMATCH']}")
        print(f"    Vol比: {m.get('vol_ratio')}倍 / 変動率: {m.get('px_move')}%")
        
        print(f"[4] 価格/乖離 (PRICE_COND): {c['IDX_HIGH_TOPIX'] or c['BASIS_STUCK_NK']}")
        print(f"    TOPIX高値圏: {c['IDX_HIGH_TOPIX']} (PCTL: {m.get('pctl')}, DEV200: {m.get('dev200')})")
        print(f"    Basis是正不能: {c['BASIS_STUCK_NK']}")
        print("=" * 60)

# ==========================================
# 4. Main
# ==========================================

def main():
    s = get_session()
    
    # データ取得
    ir_data = fetch_irbank_arbitrage(s)
    nk_data = fetch_nikkei_market(s)
    yf_data = fetch_yfinance_data()
    
    # 判定
    state = load_state()
    analyzer = MarketAnalyzer(state, ir_data, nk_data, yf_data)
    
    if analyzer.update_state():
        save_state(state)
        analyzer.evaluate()
        analyzer.report()
    else:
        print("No update required.")

if __name__ == "__main__":
    main()
