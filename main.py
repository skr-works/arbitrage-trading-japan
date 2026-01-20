from __future__ import annotations

import json
import os
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
import requests
from bs4 import BeautifulSoup
import yfinance as yf

# ====== 設定（仕様書準拠） ======
STATE_PATH = Path("state.json")

# 1. 裁定買い残の異常蓄積
ARB_MA_DAYS = 20
ARB_BUY_RATIO_TH = 1.5  # 平均比 1.5倍以上で警戒

# 2. SQ接近
SQ_NEAR_DAYS = 5        # 残り5日以内で警戒

# 3. プライム市場全体の出来高低下
VOL_MA_DAYS = 20
PRIME_VOL_RATIO_TH = 0.85 # 平均比 0.85倍以下で「薄い」と判定

# 4. 指数の価格位置（高値圏）
INDEX_TICKER = "^N225"
INDEX_LOOKBACK = "3y"
INDEX_PCTL_TH = 0.90    # 上位90%点以上で高値圏

# データソース
URL_IRBANK = "https://irbank.net/market/arbitrage"
URL_NIKKEI = "https://www.nikkei.com/markets/kabu/japanidx/"

# User-Agent
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


# ====== ユーティリティ ======

def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    return s

def parse_jp_num(s: str) -> float:
    """日本語数値（10億4878万など）をfloat（単位：株）に変換"""
    s = s.replace(",", "").strip()
    if not s or s == "-" or s == "--":
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

def get_days_to_sq(base_date: date) -> int:
    """指定日から直近のSQ（第2金曜日）までの日数を計算"""
    y, m = base_date.year, base_date.month
    
    def get_sq_date(year, month):
        first_day = date(year, month, 1)
        # 0=Mon, 4=Fri. (4 - weekday + 7) % 7 は「第1金曜までの日数」
        days_to_first_fri = (4 - first_day.weekday() + 7) % 7
        return first_day + timedelta(days=days_to_first_fri + 7) # 第2金曜

    sq_date = get_sq_date(y, m)
    
    # すでに過ぎていれば翌月のSQ
    if base_date > sq_date:
        if m == 12:
            sq_date = get_sq_date(y + 1, 1)
        else:
            sq_date = get_sq_date(y, m + 1)
            
    return (sq_date - base_date).days

# ====== データ取得 ======

def fetch_arbitrage_data(s: requests.Session) -> Tuple[Optional[date], float, float, List[float]]:
    """
    IR BANKから最新の裁定買い残・売り残と、過去の買い残履歴を取得
    Returns: (日付, 最新買い残, 最新売り残, 買い残履歴リスト)
    """
    try:
        r = s.get(URL_IRBANK, timeout=20)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text, "html.parser")
        
        header = soup.find(id="c_Shares")
        if not header:
            return None, 0, 0, []
        
        table = header.find_next("table")
        rows = table.find_all("tr")
        
        current_year = date.today().year
        arb_history = [] # 買い残の履歴
        latest_data = None
        
        for row in rows:
            # 年取得
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
            buy_str = cells[0].get_text(strip=True)
            sell_str = cells[2].get_text(strip=True)
            
            try:
                # 単位は「株」に戻してから「千株」にするか、そのまま扱うか。
                # Nikkeiの売買高も「万株」単位など表記揺れがあるため、全て「単元株数(1株)」に統一して扱う。
                buy_val = parse_jp_num(buy_str)
                sell_val = parse_jp_num(sell_str)
                arb_history.append(buy_val)
                
                # 最新行（ループの最初の方で見つかるはずだが、念のため日付チェック）
                if "/" in date_str and latest_data is None:
                    m, d = map(int, date_str.split("/"))
                    dt = date(current_year, m, d)
                    # 年またぎ補正
                    if dt > date.today() + timedelta(days=7):
                        dt = date(current_year - 1, m, d)
                    latest_data = (dt, buy_val, sell_val)
            except:
                continue
                
        # 履歴は新しい順に入っているので、時系列順（古い順）になおす
        arb_history.reverse()
        
        if latest_data:
            return latest_data[0], latest_data[1], latest_data[2], arb_history
            
    except Exception as e:
        print(f"[Error] IR BANK Fetch: {e}")
        
    return None, 0, 0, []

def fetch_prime_volume(s: requests.Session) -> Tuple[Optional[date], float]:
    """
    日経電子版から「プライム市場 売買高」を取得
    Returns: (日付, 売買高[株])
    """
    try:
        r = s.get(URL_NIKKEI, timeout=20)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text, "html.parser")
        
        # 1. 日付の特定（タイトル等から）
        page_date = date.today()
        h1 = soup.find("h1", class_="m-headline_text")
        if h1:
            # 例: "国内の株式指標・東証（20日）" -> 日付だけ抽出して現在年月と結合
            m_text = re.search(r"（(\d+)日）", h1.text)
            if m_text:
                day = int(m_text.group(1))
                # 簡易的に今月と仮定（月またぎのリスクはあるが、日経は当日か前日データ）
                # より厳密にはフッター等の更新日時を見る
                pass

        # 2. 売買高の取得
        # 「売買高・売買代金・騰落銘柄数」のテーブルを探す
        # <td class="td3">216,974万株</td> のような形式
        tables = soup.find_all("table")
        for tbl in tables:
            th = tbl.find("th", string=re.compile("売買高"))
            if th:
                # この行の「プライム」列（通常最初のtd）を取得
                tds = th.find_parent("tr").find_all("td")
                if tds:
                    vol_str = tds[0].get_text(strip=True) # プライム列
                    vol_val = parse_jp_num(vol_str)
                    return page_date, vol_val
                    
    except Exception as e:
        print(f"[Error] Nikkei Fetch: {e}")
        
    return None, 0.0

def fetch_index_position() -> Dict:
    """
    YFinanceで日経平均の過去3年データを取得し、現在位置（パーセンタイル）を計算
    """
    try:
        df = yf.download(INDEX_TICKER, period=INDEX_LOOKBACK, interval="1d", progress=False)
        if df.empty:
            return {}
        
        # Series化
        close = df["Close"]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        close = close.dropna()
        
        latest_price = float(close.iloc[-1])
        # ランク計算 (0.0 - 1.0)
        rank = (close < latest_price).mean()
        
        return {
            "latest_price": latest_price,
            "percentile": rank,
            "threshold": close.quantile(INDEX_PCTL_TH),
            "is_high_zone": rank >= INDEX_PCTL_TH
        }
    except Exception as e:
        print(f"[Error] YFinance Fetch: {e}")
        return {}

# ====== 状態管理 ======

def load_state() -> Dict:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except:
            pass
    return {"history": []}

def save_state(state: Dict):
    try:
        STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[Error] Save State: {e}")

def update_volume_history(state: Dict, dt: date, vol: float):
    """プライム出来高を履歴に追加（MA計算用）"""
    ds = dt.isoformat()
    hist = state["history"]
    
    # 既存データの更新確認
    found = False
    for r in hist:
        if r["date"] == ds:
            r["prime_volume"] = vol
            found = True
            break
    if not found:
        hist.append({"date": ds, "prime_volume": vol})
    
    # ソートして最新20日+αを残す
    hist.sort(key=lambda x: x["date"])
    if len(hist) > 100: # バッファを持たせて保持
        state["history"] = hist[-100:]

def get_volume_ma(state: Dict, window: int) -> Optional[float]:
    hist = state["history"]
    vols = [r["prime_volume"] for r in hist if "prime_volume" in r and r["prime_volume"] > 0]
    
    if len(vols) < window:
        return None # データ不足
    
    # 直近window個の平均
    recent = vols[-window:]
    return sum(recent) / len(recent)

# ====== メインロジック ======

def main():
    print("=== 日本株市場「構造歪み・急変リスク検知システム」 ===")
    s = get_session()
    state = load_state()
    
    # 1. データ取得
    arb_date, arb_buy, arb_sell, arb_hist = fetch_arbitrage_data(s)
    vol_date, prime_vol = fetch_prime_volume(s)
    idx_info = fetch_index_position()
    
    # 出来高履歴の更新
    if vol_date and prime_vol > 0:
        update_volume_history(state, vol_date, prime_vol)
        save_state(state) # データを確保
    
    # 日付チェック
    today = date.today()
    report_dt = arb_date if arb_date else today
    
    # ====== 判定ロジック ======
    
    # ① 裁定買い残の異常蓄積
    cond_arb_hot = False
    arb_ratio = 0.0
    if arb_buy > 0 and len(arb_hist) >= ARB_MA_DAYS:
        # IR BANKから取得した履歴でMA計算
        # histは古い順。直近20個の平均
        ma_arb = sum(arb_hist[-ARB_MA_DAYS:]) / ARB_MA_DAYS
        arb_ratio = arb_buy / ma_arb
        cond_arb_hot = (arb_ratio >= ARB_BUY_RATIO_TH)
    
    # ② SQ接近
    d2sq = get_days_to_sq(report_dt)
    cond_sq_near = (d2sq <= SQ_NEAR_DAYS)
    
    # ③ プライム市場全体の出来高低下
    cond_vol_thin = False
    vol_ratio = 0.0
    vol_ma = get_volume_ma(state, VOL_MA_DAYS)
    
    if vol_ma and prime_vol > 0:
        vol_ratio = prime_vol / vol_ma
        cond_vol_thin = (vol_ratio <= PRIME_VOL_RATIO_TH)
    else:
        # 初回実行時などで履歴がない場合は判定不能（Falseとする）
        # ※「判定思想」に基づき、条件が揃わない限りアラートは出さない
        pass 

    # ④ 指数の価格位置（高値圏）
    cond_idx_high = idx_info.get("is_high_zone", False)
    
    # ====== 総合判定 ======
    # 「構造 × 時間 × 流動性 × 価格位置」が 同時成立したか
    # データ不足(None)の項目はFalse扱いとなるため安全側に倒れる
    alert_triggered = cond_arb_hot and cond_sq_near and cond_vol_thin and cond_idx_high
    
    # LEVEL判定
    true_count = sum([cond_arb_hot, cond_sq_near, cond_vol_thin, cond_idx_high])
    
    if alert_triggered:
        level = "LEVEL 3: WARNING (警戒)"
        msg = "構造・時間・流動性・価格の全条件が成立。市場は壊れやすい状態です。\n新規投入抑制、ポジション縮小を推奨。"
    elif true_count >= 2:
        level = "LEVEL 2: CAUTION (注意)"
        msg = "複数の歪みが観測されています。レバレッジや一括エントリーは避けてください。"
    else:
        level = "LEVEL 1: NORMAL (正常)"
        msg = "構造的な危機シグナルは点灯していません。通常運用可能です。"

    # ====== レポート出力 ======
    print(f"\n[判定結果] {level}")
    print(msg)
    print("-" * 40)
    
    print("1. 裁定買い残の異常蓄積")
    print(f"   現在: {arb_buy/100000000:.2f}億株 (MA20比: {arb_ratio:.2f}倍)")
    print(f"   判定: {cond_arb_hot} (閾値 {ARB_BUY_RATIO_TH}倍以上)")
    
    print("\n2. SQ接近")
    print(f"   残り日数: {d2sq}日")
    print(f"   判定: {cond_sq_near} (閾値 {SQ_NEAR_DAYS}日以内)")
    
    print("\n3. プライム出来高低下 (流動性)")
    if vol_ma:
        print(f"   現在: {prime_vol/100000000:.2f}億株 (MA20比: {vol_ratio:.2f}倍)")
        print(f"   判定: {cond_vol_thin} (閾値 {PRIME_VOL_RATIO_TH}倍以下)")
    else:
        print(f"   現在: {prime_vol/100000000:.2f}億株")
        print("   判定: データ不足のため保留 (state.jsonに蓄積中)")

    print("\n4. 指数高値圏")
    pctl = idx_info.get('percentile', 0) * 100
    print(f"   現在位置: {pctl:.1f}%点 (過去3年分布)")
    print(f"   判定: {cond_idx_high} (閾値 {int(INDEX_PCTL_TH*100)}%以上)")
    
    print("=" * 40)
    print(f"ALERT_VOLATILITY_RISK = {alert_triggered}")

if __name__ == "__main__":
    main()
