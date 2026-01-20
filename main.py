from __future__ import annotations

import os
import re
import sys
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
import yfinance as yf

# ====== 設定・定数 ======
# 1. 裁定買い残（IR BANKより取得）
ARB_MA_DAYS = 20
ARB_BUY_RATIO_TH = 1.5  # 閾値: 平均の1.5倍

# 2. SQ接近
SQ_NEAR_DAYS = 5        # 閾値: SQまで5日以内

# 3. プライム出来高（TOPIX出来高で代用）
VOL_MA_DAYS = 20
PRIME_VOL_RATIO_TH = 0.85 # 閾値: 平均の85%以下（閑散）

# 4. 指数高値圏（日経平均）
INDEX_LOOKBACK_YEARS = 3
INDEX_PCTL_TH = 0.90    # 閾値: 過去3年の90%点以上

# URL / Ticker
IRBANK_URL = "https://irbank.net/market/arbitrage"
TICKER_PRICE = "^N225"  # 日経平均（価格位置判定用）
TICKER_LIQUIDITY = "^TOPX"  # TOPIX（プライム出来高判定用）

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

# ====== ユーティリティ ======

def parse_japanese_number(s: str) -> float:
    """
    '10億4878万' や '6938万' のような文字列を数値(float)に変換する。
    単位は「株」として返す。
    """
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
        else:
            pass # 無視
            
    if current_num:
        total += float(current_num)
        
    return total

def get_next_sq_date(base_date: date) -> date:
    """
    基準日以降の直近のSQ日（第二金曜日）を算出する。
    """
    # 当月
    y, m = base_date.year, base_date.month
    
    def get_2nd_friday(year, month):
        first_day = date(year, month, 1)
        # 0=Mon, 4=Fri. (4 - weekday) % 7 gives days to first Friday
        days_to_first_fri = (4 - first_day.weekday()) % 7
        first_fri = first_day + timedelta(days=days_to_first_fri)
        return first_fri + timedelta(days=7) # 2nd Friday

    this_month_sq = get_2nd_friday(y, m)
    
    if base_date <= this_month_sq:
        return this_month_sq
    else:
        # 翌月
        if m == 12:
            return get_2nd_friday(y + 1, 1)
        else:
            return get_2nd_friday(y, m + 1)

# ====== データ取得ロジック ======

def fetch_arbitrage_data() -> pd.DataFrame:
    """
    IR BANKから裁定残高（株数）の履歴を取得する。
    戻り値: DataFrame (index=Date, columns=[buy_shares, sell_shares])
    """
    try:
        r = requests.get(IRBANK_URL, headers={"User-Agent": UA}, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        
        # 「株数」セクションを探す
        header = soup.find(id="c_Shares")
        if not header:
            raise RuntimeError("IRBANK: Shares header not found")
        
        table = header.find_next("table")
        rows = table.find_all("tr")
        
        data = []
        current_year = date.today().year # デフォルト
        
        # IR BANKのテーブルは新しい順に並んでいる
        for row in rows:
            # 年の取得 (<td class="ct">2026</td>)
            if "occ" in row.get("class", []):
                td = row.find("td")
                if td and td.text.strip().isdigit():
                    current_year = int(td.text.strip())
                continue
            
            # データ行 (<td class="lf weaken">01/16</td>)
            date_td = row.find("td", class_="lf")
            if not date_td:
                continue
            
            # 数値セル (class="rt")
            # 構成: [買い残, 前日比, 売り残, 前日比]
            cells = row.find_all("td", class_="rt")
            if len(cells) < 3:
                continue
                
            date_str = date_td.get_text(strip=True)
            buy_str = cells[0].get_text(strip=True)
            sell_str = cells[2].get_text(strip=True) # index 2 is sell pos
            
            try:
                mo, da = map(int, date_str.split("/"))
                dt = date(current_year, mo, da)
                
                # 日本語数値をパース
                buy_val = parse_japanese_number(buy_str)
                sell_val = parse_japanese_number(sell_str)
                
                data.append({
                    "date": pd.Timestamp(dt),
                    "arb_buy": buy_val,
                    "arb_sell": sell_val
                })
            except ValueError:
                continue
                
        if not data:
            raise RuntimeError("IRBANK: No valid data parsed")
            
        df = pd.DataFrame(data).sort_values("date")
        df.set_index("date", inplace=True)
        return df
        
    except Exception as e:
        print(f"Error fetching IRBANK: {e}")
        return pd.DataFrame()

def fetch_market_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    yfinanceから日経平均(価格判定用)とTOPIX(出来高判定用)を取得
    """
    # 1. 日経平均（価格位置用、過去3年）
    n225 = yf.download(TICKER_PRICE, period="3y", interval="1d", progress=False)
    if n225.empty:
        raise RuntimeError(f"yfinance: No data for {TICKER_PRICE}")
    
    # 2. TOPIX（出来高用、直近）
    # ※ プライム出来高の代用としてTOPIXのVolumeを使用する
    topx = yf.download(TICKER_LIQUIDITY, period="6mo", interval="1d", progress=False)
    if topx.empty:
        raise RuntimeError(f"yfinance: No data for {TICKER_LIQUIDITY}")
        
    return n225, topx

# ====== メイン判定ロジック ======

def analyze_market_structure():
    print("=== 日本株市場「構造歪み・急変リスク検知システム」 ===")
    print(f"Report Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("-" * 60)

    # 1. データ取得
    print("[1/4] Fetching Arbitrage Data (IR BANK)...")
    df_arb = fetch_arbitrage_data()
    if df_arb.empty:
        print("CRITICAL ERROR: Failed to fetch Arbitrage data.")
        return

    print("[2/4] Fetching Market Data (Yahoo Finance)...")
    try:
        df_price, df_vol = fetch_market_data()
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        return

    # 最新データポイントの特定
    latest_arb = df_arb.iloc[-1]
    latest_price = df_price.iloc[-1]
    latest_vol = df_vol.iloc[-1]
    
    today = date.today()
    arb_date = latest_arb.name.date()
    
    # --- 判定1: 裁定買い残の異常蓄積 ---
    # 過去20日の平均と比較
    arb_series = df_arb["arb_buy"]
    if len(arb_series) >= ARB_MA_DAYS:
        arb_ma = arb_series.rolling(window=ARB_MA_DAYS).mean().iloc[-1]
        arb_ratio = latest_arb["arb_buy"] / arb_ma
    else:
        arb_ma = 0
        arb_ratio = 0
    
    is_arb_distorted = (arb_ratio >= ARB_BUY_RATIO_TH)
    
    # --- 判定2: SQ接近 ---
    # 次回SQ日までの日数
    next_sq = get_next_sq_date(today)
    days_to_sq = (next_sq - today).days
    is_sq_near = (0 <= days_to_sq <= SQ_NEAR_DAYS)

    # --- 判定3: プライム市場(TOPIX)の出来高低下 ---
    # 直近の出来高と20日平均の比較
    vol_series = df_vol["Volume"]
    # yfinanceのVolumeが0の場合のクリーニング
    vol_series = vol_series.replace(0, pd.NA).ffill()
    
    if len(vol_series) >= VOL_MA_DAYS:
        vol_ma = vol_series.rolling(window=VOL_MA_DAYS).mean().iloc[-1]
        current_vol = vol_series.iloc[-1]
        vol_ratio = float(current_vol / vol_ma)
    else:
        vol_ratio = 1.0
        
    is_liquidity_thin = (vol_ratio <= PRIME_VOL_RATIO_TH)

    # --- 判定4: 指数の価格位置（高値圏） ---
    # 過去3年のCloseデータのパーセンタイル
    close_series = df_price["Close"]
    current_price = float(close_series.iloc[-1])
    p_threshold = float(close_series.quantile(INDEX_PCTL_TH))
    is_high_zone = (current_price >= p_threshold)

    # ====== レポート出力 ======
    
    # フォーマット用
    arb_unit_str = f"{latest_arb['arb_buy']/100000000:.1f}億株"
    
    print("\n【構造要因分析】")
    
    # 1. Arbitrage
    res_mark = "!! 異常 !!" if is_arb_distorted else "正常"
    print(f"1. 裁定買い残の蓄積  : [{res_mark}]")
    print(f"   現在: {arb_unit_str} (20日平均比: {arb_ratio:.2f}倍)")
    print(f"   基準: {ARB_BUY_RATIO_TH}倍以上で「解消待ち売り圧力」が高いと判定")
    print(f"   データ日付: {arb_date}")

    # 2. SQ
    res_mark = "!! 接近 !!" if is_sq_near else "遠い"
    print(f"\n2. SQ接近 (点火装置): [{res_mark}]")
    print(f"   次回SQ: {next_sq} (あと {days_to_sq} 日)")
    print(f"   基準: 残り{SQ_NEAR_DAYS}日以内で警戒モード")

    # 3. Liquidity
    res_mark = "!! 薄い !!" if is_liquidity_thin else "正常"
    print(f"\n3. 市場流動性 (受け皿): [{res_mark}]")
    print(f"   出来高乖離: 平均比 {vol_ratio:.2f}倍 (TOPIX代用)")
    print(f"   基準: {PRIME_VOL_RATIO_TH}倍以下で「逃げ場が狭い」と判定")

    # 4. Price Position
    res_mark = "!! 高値圏 !!" if is_high_zone else "中立/安値"
    print(f"\n4. 価格位置 (ポテンシャル): [{res_mark}]")
    print(f"   現在値: {current_price:,.0f} (3年分布の {int(INDEX_PCTL_TH*100)}%点: {p_threshold:,.0f})")
    print(f"   意味: 崩れた際の値幅ポテンシャル")

    print("-" * 60)
    
    # 総合判定
    conditions = [is_arb_distorted, is_sq_near, is_liquidity_thin, is_high_zone]
    true_count = sum(conditions)
    
    print("【最終構造判定】")
    if true_count == 4:
        level = "LEVEL 3: WARNING (警戒)"
        msg = "全ての構造的条件が成立しています。市場は極めて壊れやすい状態です。\n新規買いは停止し、ポジションの縮小・ヘッジを最優先してください。"
    elif true_count >= 2:
        level = "LEVEL 2: CAUTION (注意)"
        msg = "複数の歪みが観測されています。レバレッジや一括エントリーは避け、\n資金管理を厳格に行ってください。"
    else:
        level = "LEVEL 1: NORMAL (正常)"
        msg = "構造的な危機シグナルは点灯していません。通常のトレードルールに従ってください。"

    print(f"判定: {level}")
    print(f"成立条件数: {true_count} / 4")
    print("\n>> アクション指針:")
    print(msg)
    print("=" * 60)

if __name__ == "__main__":
    analyze_market_structure()
