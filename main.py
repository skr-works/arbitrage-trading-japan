from __future__ import annotations

import json
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
import yfinance as yf

# =========================
# 設定（仕様書準拠・確定版）
# =========================

STATE_PATH = Path("state.json")

# データソース（JPXは絶対に使わない）
URL_IRBANK = "https://irbank.net/market/arbitrage"
URL_NIKKEI = "https://www.nikkei.com/markets/kabu/japanidx/"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# state.json 保持期間（5年）
STATE_MAX_RECORDS = 1400  # 245営業日/年 ×5=1225 なので余裕を持たせる

# SQ
SQ_NEAR_DAYS = 5
MAJOR_SQ_MONTHS = {3, 6, 9, 12}

# 裁定ネット残（ARB_NET = BUY - SELL）の営業日差分
ARB_DELTA_SHORT = 3
ARB_DELTA_MAIN = 5
ARB_DELTA_LONG = 25

# 出来高不整合
VOL_MA_DAYS = 20
VOL_RATIO_THIN = 0.85
P_MOVE_TH = 1.0    # 薄いのに動く
P_SPIKE_TH = 2.0   # 普通でも飛ぶ

# 価格（yfinance）
# 重要：^TOPX は取れない環境があるため、Yahoo日本のTOPIXコード 998405.T を採用
TOPIX_TICKER = "998405.T"
N225_TICKER = "^N225"

# 日経先物（yfinance）
# 重要：NK=F が取れない環境があるため、Yahoo Financeで取得できる NIY=F を採用
N225_FUT_TICKER = "NIY=F"

INDEX_LOOKBACK = "3y"
TOPIX_PCTL_HIGH = 0.90
TOPIX_PCTL_LOW = 0.10
TOPIX_DEV200_TH = 0.08
TOPIX_MIN_POINTS_3Y = 500

# 先物−現物（縮小しない＝ストレス）
BASIS_LOOKBACK_DAYS = 20
BASIS_SHRINK_WINDOW = 5


# =========================
# ユーティリティ
# =========================

def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    return s


def parse_jp_num(s: str) -> Optional[float]:
    """
    日本語数値（例：216,974万株 / 10億4878万 / 1.2兆 など）を float（単位：株）に変換。
    仕様： "-" / "--" は「無効」として None を返す（0扱いしない）。
    """
    if s is None:
        return None
    s = s.replace(",", "").strip()
    if not s or s in {"-", "--"}:
        return None

    units = {"兆": 10**12, "億": 10**8, "万": 10**4}
    total = 0.0
    current = ""

    for ch in s:
        if ch.isdigit() or ch == ".":
            current += ch
        elif ch in units:
            if current:
                total += float(current) * units[ch]
                current = ""
        else:
            # "株" 等は無視
            pass

    if current:
        total += float(current)
    return total


def get_days_to_sq(base_date: date) -> int:
    """指定日から直近のSQ（第2金曜日）までの日数（カレンダー日）"""
    y, m = base_date.year, base_date.month

    def get_sq_date(year: int, month: int) -> date:
        first = date(year, month, 1)
        days_to_first_fri = (4 - first.weekday() + 7) % 7
        return first + timedelta(days=days_to_first_fri + 7)

    sq = get_sq_date(y, m)
    if base_date > sq:
        if m == 12:
            sq = get_sq_date(y + 1, 1)
        else:
            sq = get_sq_date(y, m + 1)
    return (sq - base_date).days


def is_major_sq_month(d: date) -> bool:
    return d.month in MAJOR_SQ_MONTHS


def safe_pct_change(series: pd.Series) -> Optional[float]:
    """直近2点から前日比%（符号付き）を返す。取得不能なら None。"""
    if series is None or series.dropna().shape[0] < 2:
        return None
    s = series.dropna()
    prev = float(s.iloc[-2])
    last = float(s.iloc[-1])
    if prev == 0:
        return None
    return (last / prev - 1.0) * 100.0


# =========================
# データ取得
# =========================

def fetch_arbitrage_data(s: requests.Session) -> Tuple[Optional[date], Optional[float], Optional[float], List[float]]:
    """
    IRBankから最新の裁定買い残・売り残、過去の裁定ネット残（買い−売り）履歴を取得
    Returns: (最新日付, 最新買い残, 最新売り残, ネット残履歴[古→新])
    """
    try:
        r = s.get(URL_IRBANK, timeout=20)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text, "html.parser")

        header = soup.find(id="c_Shares")
        if not header:
            return None, None, None, []

        table = header.find_next("table")
        rows = table.find_all("tr")

        current_year = date.today().year
        net_hist_newest_first: List[float] = []
        latest_data: Optional[Tuple[date, float, float]] = None

        for row in rows:
            if "occ" in row.get("class", []):
                td = row.find("td")
                if td and td.text.strip().isdigit():
                    current_year = int(td.text.strip())
                continue

            td_date = row.find("td", class_="lf")
            if not td_date:
                continue

            cells = row.find_all("td", class_="rt")
            if len(cells) < 3:
                continue

            date_str = td_date.get_text(strip=True)
            buy_str = cells[0].get_text(strip=True)
            sell_str = cells[2].get_text(strip=True)

            buy_val = parse_jp_num(buy_str)
            sell_val = parse_jp_num(sell_str)
            if buy_val is None or sell_val is None:
                continue

            net_val = float(buy_val - sell_val)
            net_hist_newest_first.append(net_val)

            if "/" in date_str and latest_data is None:
                try:
                    mm, dd = map(int, date_str.split("/"))
                    dt = date(current_year, mm, dd)
                    if dt > date.today() + timedelta(days=7):
                        dt = date(current_year - 1, mm, dd)
                    latest_data = (dt, float(buy_val), float(sell_val))
                except Exception:
                    pass

        net_hist_newest_first.reverse()

        if latest_data:
            return latest_data[0], latest_data[1], latest_data[2], net_hist_newest_first

    except Exception as e:
        print(f"[Error] IRBank fetch: {e}")

    return None, None, None, []


def fetch_prime_volume(s: requests.Session) -> Tuple[Optional[date], Optional[float]]:
    """日経電子版から「プライム市場 売買高」を取得。取れないなら (None, None)。"""
    try:
        r = s.get(URL_NIKKEI, timeout=20)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text, "html.parser")

        page_date = date.today()

        tables = soup.find_all("table")
        for tbl in tables:
            th = tbl.find("th", string=re.compile("売買高"))
            if not th:
                continue
            tr = th.find_parent("tr")
            if not tr:
                continue
            tds = tr.find_all("td")
            if not tds:
                continue

            vol_str = tds[0].get_text(strip=True)
            vol_val = parse_jp_num(vol_str)
            if vol_val is None:
                return None, None
            return page_date, float(vol_val)

    except Exception as e:
        print(f"[Error] Nikkei fetch: {e}")

    return None, None


def fetch_yf_series(ticker: str, period: str, interval: str = "1d") -> Optional[pd.Series]:
    """yfinanceから終値Seriesを取得。失敗したらNone。"""
    try:
        df = yf.download(ticker, period=period, interval=interval, progress=False)
        if df is None or df.empty:
            return None
        close = df["Close"]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        close = close.dropna()
        if close.empty:
            return None
        return close
    except Exception:
        return None


def compute_topix_position() -> Dict:
    """TOPIX（998405.T）の価格位置（PCTL×DEV200 AND）"""
    close = fetch_yf_series(TOPIX_TICKER, period=INDEX_LOOKBACK, interval="1d")
    if close is None or close.shape[0] < TOPIX_MIN_POINTS_3Y:
        return {"ok": False}

    latest = float(close.iloc[-1])

    pctl = float((close < latest).mean())

    if close.shape[0] < 200:
        return {"ok": False}
    ma200 = float(close.rolling(200).mean().iloc[-1])
    if ma200 == 0 or pd.isna(ma200):
        return {"ok": False}
    dev200 = float(latest / ma200 - 1.0)

    idx_high = (pctl >= TOPIX_PCTL_HIGH) and (dev200 >= TOPIX_DEV200_TH)
    idx_low = (pctl <= TOPIX_PCTL_LOW) and (dev200 <= -TOPIX_DEV200_TH)

    return {
        "ok": True,
        "latest_price": latest,
        "pctl": pctl,
        "dev200": dev200,
        "idx_high_topix": bool(idx_high),
        "idx_low_topix": bool(idx_low),
    }


def compute_daily_move_pct() -> Dict:
    """前日比%（TOPIXメイン、ダメならN225）"""
    topix_close = fetch_yf_series(TOPIX_TICKER, period="10d", interval="1d")
    pct = safe_pct_change(topix_close) if topix_close is not None else None
    if pct is not None:
        return {"ok": True, "source": "TOPIX", "pct": float(pct)}

    n225_close = fetch_yf_series(N225_TICKER, period="10d", interval="1d")
    pct = safe_pct_change(n225_close) if n225_close is not None else None
    if pct is not None:
        return {"ok": True, "source": "N225", "pct": float(pct)}

    return {"ok": False, "source": None, "pct": None}


def compute_basis_stuck_nk() -> Dict:
    """
    日経先物（NIY=F）−日経平均（^N225）が「5営業日前より縮小していない」か
    """
    fut = fetch_yf_series(N225_FUT_TICKER, period=f"{BASIS_LOOKBACK_DAYS}d", interval="1d")
    spot = fetch_yf_series(N225_TICKER, period=f"{BASIS_LOOKBACK_DAYS}d", interval="1d")
    if fut is None or spot is None:
        return {"ok": False}

    df = pd.DataFrame({"fut": fut, "spot": spot}).dropna()
    if df.shape[0] < (BASIS_SHRINK_WINDOW + 1):
        return {"ok": False}

    basis = df["fut"] - df["spot"]
    basis_today = float(basis.iloc[-1])
    basis_5ago = float(basis.iloc[-(BASIS_SHRINK_WINDOW + 1)])

    stuck = abs(basis_today) >= abs(basis_5ago)

    return {
        "ok": True,
        "basis_today": basis_today,
        "basis_5ago": basis_5ago,
        "stuck": bool(stuck),
    }


# =========================
# state.json（出来高履歴）
# =========================

def load_state() -> Dict:
    if STATE_PATH.exists():
        try:
            st = json.loads(STATE_PATH.read_text(encoding="utf-8"))
            if isinstance(st, dict) and "history" in st and isinstance(st["history"], list):
                return st
        except Exception:
            pass
    return {"history": []}


def save_state(state: Dict) -> None:
    try:
        STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[Error] Save state: {e}")


def update_volume_history(state: Dict, dt: date, vol: float) -> bool:
    """プライム出来高を履歴に追加（5年保持）。同日同値なら更新しない。"""
    ds = dt.isoformat()
    hist = state["history"]

    for r in hist:
        if r.get("date") == ds:
            old = r.get("prime_volume")
            if old == vol:
                return False
            r["prime_volume"] = vol
            return True

    hist.append({"date": ds, "prime_volume": vol})
    hist.sort(key=lambda x: x.get("date", ""))

    if len(hist) > STATE_MAX_RECORDS:
        state["history"] = hist[-STATE_MAX_RECORDS:]
    return True


def get_volume_ma(state: Dict, window: int) -> Optional[float]:
    hist = state.get("history", [])
    vols = [
        r["prime_volume"]
        for r in hist
        if isinstance(r.get("prime_volume"), (int, float)) and r["prime_volume"] > 0
    ]
    if len(vols) < window:
        return None
    recent = vols[-window:]
    return float(sum(recent) / len(recent))


# =========================
# 判定ロジック
# =========================

def calc_delta(net_hist: List[float], lag: int) -> Optional[float]:
    """営業日ベース：net_hist は古→新。lag本前との差分。足りなければNone"""
    if net_hist is None or len(net_hist) < (lag + 1):
        return None
    return float(net_hist[-1] - net_hist[-(lag + 1)])


def main():
    print("=== 日本株市場「歪み破裂リスク検知システム（仕様確定版）」 ===")

    s = get_session()
    state = load_state()

    # 1) データ取得（IRBank / 日経 / yfinance）
    arb_date, arb_buy, arb_sell, arb_net_hist = fetch_arbitrage_data(s)
    vol_date, prime_vol = fetch_prime_volume(s)

    topix_pos = compute_topix_position()
    move_info = compute_daily_move_pct()
    basis_info = compute_basis_stuck_nk()

    today = date.today()
    report_dt = arb_date if arb_date else today

    # 2) 非取引日/更新なし対策（保存と判定）
    state_updated = False
    if vol_date and isinstance(prime_vol, (int, float)) and prime_vol and prime_vol > 0:
        state_updated = update_volume_history(state, vol_date, float(prime_vol))
        if state_updated:
            save_state(state)

    # IRBankも日経も取れないなら「判定しない」
    if arb_date is None and (vol_date is None or prime_vol is None):
        print("\n[INFO] データ更新が確認できないため、本日は判定をスキップします（非取引日/取得失敗の可能性）。")
        return

    # 3) 裁定ネット残ロジック（Δ3/Δ5/Δ25）
    d3 = calc_delta(arb_net_hist, ARB_DELTA_SHORT)
    d5 = calc_delta(arb_net_hist, ARB_DELTA_MAIN)
    d25 = calc_delta(arb_net_hist, ARB_DELTA_LONG)

    arb_stuck = False
    arb_info_boost = False
    if d5 is not None and d25 is not None:
        arb_stuck = (d5 >= 0) and (d25 > 0)
    if d3 is not None:
        arb_info_boost = (d3 >= 0)

    # 4) SQ（メジャーSQ接近を重視）
    d2sq = get_days_to_sq(report_dt)
    major_sq_near = (d2sq <= SQ_NEAR_DAYS) and is_major_sq_month(report_dt)

    # 5) 流動性の質低下（出来高×価格変動の不整合）
    liq_mismatch = False
    vol_ma = get_volume_ma(state, VOL_MA_DAYS)
    vol_ratio = None
    px_move_abs = None
    px_move_src = None

    if vol_ma is not None and isinstance(prime_vol, (int, float)) and prime_vol and prime_vol > 0 and move_info.get("ok"):
        vol_ratio = float(prime_vol) / float(vol_ma)
        px_move_src = move_info.get("source")
        px_move_abs = abs(float(move_info.get("pct")))

        if (vol_ratio <= VOL_RATIO_THIN and px_move_abs >= P_MOVE_TH) or (vol_ratio > VOL_RATIO_THIN and px_move_abs >= P_SPIKE_TH):
            liq_mismatch = True

    # 6) 価格位置（TOPIX）
    idx_high_topix = bool(topix_pos.get("idx_high_topix")) if topix_pos.get("ok") else False
    topix_pctl = float(topix_pos.get("pctl")) if topix_pos.get("ok") and "pctl" in topix_pos else None
    topix_dev200 = float(topix_pos.get("dev200")) if topix_pos.get("ok") and "dev200" in topix_pos else None

    # 7) 裁定ストレス補助（先物−現物が縮まらない）
    basis_stuck_nk = bool(basis_info.get("stuck")) if basis_info.get("ok") else False

    # 8) 総合判定（仕様確定）
    warning = arb_stuck and major_sq_near and liq_mismatch and (idx_high_topix or basis_stuck_nk)
    caution = (not warning) and arb_stuck and liq_mismatch and (major_sq_near or idx_high_topix or basis_stuck_nk)

    if warning:
        level = "LEVEL 3: WARNING (警戒)"
        msg = "裁定の是正不全×メジャーSQ接近×流動性不整合が同時成立。市場が壊れやすい環境です。"
    elif caution:
        level = "LEVEL 2: CAUTION (注意)"
        msg = "裁定の是正不全と流動性不整合が観測。イベント要因次第で荒れやすい状態です。"
    else:
        level = "LEVEL 1: NORMAL (正常)"
        msg = "歪み破裂リスクの主要条件は成立していません。"

    # =========================
    # レポート出力
    # =========================
    print(f"\n[判定日] {report_dt.isoformat()}")
    print(f"[判定結果] {level}")
    print(msg)
    print("-" * 50)

    print("A) 裁定ネット残（IRBank）")
    if arb_buy is not None and arb_sell is not None:
        arb_net_latest = arb_buy - arb_sell
        print(f"   最新 BUY: {arb_buy/1e8:.2f}億株 / SELL: {arb_sell/1e8:.2f}億株 / NET: {arb_net_latest/1e8:.2f}億株")
    else:
        print("   最新 BUY/SELL: 取得失敗")
    print(f"   Δ{ARB_DELTA_SHORT}: {d3 if d3 is not None else 'N/A'}  (INFO加点={arb_info_boost})")
    print(f"   Δ{ARB_DELTA_MAIN}:  {d5 if d5 is not None else 'N/A'}")
    print(f"   Δ{ARB_DELTA_LONG}: {d25 if d25 is not None else 'N/A'}")
    print(f"   ARB_STUCK(必須): {arb_stuck}")

    print("\nB) SQ（メジャーSQ重視）")
    print(f"   D2SQ: {d2sq}日 / メジャーSQ月: {is_major_sq_month(report_dt)} / MAJOR_SQ_NEAR: {major_sq_near}")

    print("\nC) 流動性の質（出来高×価格変動 不整合）")
    if vol_ma is None or prime_vol is None:
        print("   データ不足（state.json蓄積中 or 日経取得失敗）")
    else:
        if vol_ratio is not None:
            print(f"   プライム売買高: {float(prime_vol)/1e8:.2f}億株 / MA20: {float(vol_ma)/1e8:.2f}億株 / 比率: {vol_ratio:.2f}")
        else:
            print(f"   プライム売買高: {float(prime_vol)/1e8:.2f}億株 / MA20: {float(vol_ma)/1e8:.2f}億株")

    if px_move_abs is not None:
        print(f"   価格変動(|前日比%|): {px_move_abs:.2f}%（ソース: {px_move_src}）")
    else:
        print("   価格変動: 取得失敗（yfinance）")
    print(f"   LIQ_MISMATCH: {liq_mismatch}")

    print("\nD) TOPIX 価格位置（PCTL×DEV200 AND）")
    if topix_pos.get("ok"):
        print(f"   ティッカー: {TOPIX_TICKER}")
        print(f"   PCTL(3y): {topix_pctl*100:.1f}%点 / DEV200: {topix_dev200*100:.2f}%")
        print(f"   IDX_HIGH_TOPIX: {idx_high_topix}")
    else:
        print(f"   TOPIX位置: データ不足/取得失敗（ティッカー: {TOPIX_TICKER}）")

    print("\nE) 裁定ストレス補助（日経先物−現物が縮まらない）")
    if basis_info.get("ok"):
        print(f"   先物ティッカー: {N225_FUT_TICKER} / 現物: {N225_TICKER}")
        print(f"   BASIS 今日: {basis_info['basis_today']:.2f} / 5営業日前: {basis_info['basis_5ago']:.2f}")
        print(f"   BASIS_STUCK: {basis_stuck_nk}")
    else:
        print(f"   BASIS: 取得失敗/データ不足（先物ティッカー: {N225_FUT_TICKER}）")

    print("\nF) state.json")
    print(f"   更新: {state_updated} / 保持件数: {len(state.get('history', []))} / 上限: {STATE_MAX_RECORDS}")
    print("=" * 50)
    print(f"ALERT_VOLATILITY_RISK = {warning}")


if __name__ == "__main__":
    main()
