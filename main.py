from __future__ import annotations

import json
import os  # 追加（URLを環境変数から受け取る）
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
import yfinance as yf
import jpholiday  # 追加（祝日停止）

# =========================
# 設定（仕様書準拠・確定版）
# =========================

STATE_PATH = Path("state.json")

# 入力（URLは環境変数から注入。コード上に直書きしない）
SRC_A_URL = os.environ.get("SRC_A_URL", "").strip()
SRC_B_URL = os.environ.get("SRC_B_URL", "").strip()

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
# 重要：TOPIXは「価格」用途のみ、1306.T（TOPIX連動ETF）で代用する。998405.Tは使用しない。
TOPIX_TICKER = "1306.T"
N225_TICKER = "^N225"

# 日経先物（yfinance）
N225_FUT_TICKER = "NIY=F"

INDEX_LOOKBACK = "3y"
TOPIX_PCTL_HIGH = 0.90
TOPIX_PCTL_LOW = 0.10
TOPIX_DEV200_TH = 0.08
TOPIX_MIN_POINTS_3Y = 500

# 先物−現物（縮小しない＝ストレス）
BASIS_LOOKBACK_DAYS = 20
BASIS_SHRINK_WINDOW = 5

# -------------------------
# 追加：修正仕様（MARGIN / EMERGENCY）
# -------------------------

# ARB MARGIN：比率 + FLOOR
ARB_MARGIN_5_RATIO = 0.005   # m5=0.5%
ARB_MARGIN_25_RATIO = 0.010  # m25=1.0%
ARB_FLOOR_5_MED_RATIO = 0.001   # FLOOR_5 = median * 0.1%
ARB_FLOOR_25_MED_RATIO = 0.002  # FLOOR_25 = median * 0.2%

# ARB高水準：直近ウィンドウ（1y相当=245本、足りなければ6m相当=126本）
ARB_PCTL_YEAR_POINTS = 245
ARB_PCTL_HALF_POINTS = 126
ARB_HIGH_PCTL = 0.80

# EMERGENCY：q=0.99 と固定3%のmax
EMERGENCY_FIXED_TH = 3.0
EMERGENCY_Q = 0.99
MOVE_LOOKBACK = "1y"
MOVE_MIN_POINTS = 60  # 少なすぎる分位は不安定なので最低限


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


def is_market_closed(d: date) -> bool:
    """
    停止条件：
      - 土日
      - 国民の祝日（jpholiday）
      - 12/31〜1/3（固定停止）
    """
    if d.weekday() >= 5:
        return True
    if (d.month == 12 and d.day == 31) or (d.month == 1 and d.day in (1, 2, 3)):
        return True
    if jpholiday.is_holiday(d):
        return True
    return False


# -------------------------
# 追加：ARB統計（分位・中央値・MARGIN）
# -------------------------

def _arb_window(net_hist: List[float]) -> Optional[List[float]]:
    if not net_hist:
        return None
    if len(net_hist) >= ARB_PCTL_YEAR_POINTS:
        return net_hist[-ARB_PCTL_YEAR_POINTS:]
    if len(net_hist) >= ARB_PCTL_HALF_POINTS:
        return net_hist[-ARB_PCTL_HALF_POINTS:]
    return None


def compute_arb_stats(net_hist: List[float], arb_net_latest: float) -> Optional[Dict]:
    """
    ARBの分位・中央値・MARGINを返す。
    - pctl: 直近値がウィンドウ内でどの位置か（< latest の比率）
    - med_abs: ウィンドウ内の |net| の中央値（FLOOR用）
    - margin_5 / margin_25: 比率 + FLOOR
    """
    w = _arb_window(net_hist)
    if w is None:
        return None

    s = pd.Series(w).dropna()
    if s.empty:
        return None

    latest = float(arb_net_latest)
    pctl = float((s < latest).mean())

    med_abs = float(pd.Series([abs(float(x)) for x in s.tolist()]).median())
    floor_5 = med_abs * ARB_FLOOR_5_MED_RATIO
    floor_25 = med_abs * ARB_FLOOR_25_MED_RATIO

    base = abs(latest)
    margin_5 = max(base * ARB_MARGIN_5_RATIO, floor_5)
    margin_25 = max(base * ARB_MARGIN_25_RATIO, floor_25)

    return {
        "pctl": pctl,
        "arb_high": bool(pctl >= ARB_HIGH_PCTL),
        "med_abs": med_abs,
        "floor_5": float(floor_5),
        "floor_25": float(floor_25),
        "margin_5": float(margin_5),
        "margin_25": float(margin_25),
        "window_n": int(s.shape[0]),
    }


# -------------------------
# 追加：EMERGENCY閾値（q=0.99）
# -------------------------

def compute_move_abs_q99() -> Optional[float]:
    """
    |前日比%| の分布（直近1y）から q=0.99 閾値を返す。
    TOPIX(1306.T)優先、無理ならN225。
    """
    close = fetch_yf_series(TOPIX_TICKER, period=MOVE_LOOKBACK, interval="1d")
    if close is None or close.dropna().shape[0] < MOVE_MIN_POINTS:
        close = fetch_yf_series(N225_TICKER, period=MOVE_LOOKBACK, interval="1d")
        if close is None or close.dropna().shape[0] < MOVE_MIN_POINTS:
            return None

    c = close.dropna().astype(float)
    pct = c.pct_change() * 100.0
    abs_pct = pct.abs().dropna()
    if abs_pct.shape[0] < MOVE_MIN_POINTS:
        return None

    q = float(abs_pct.quantile(EMERGENCY_Q))
    return q


# =========================
# データ取得
# =========================

def fetch_arbitrage_data(s: requests.Session) -> Tuple[Optional[date], Optional[float], Optional[float], List[float]]:
    """
    SRC_A から、最新の買い残・売り残と、過去のネット残（買い−売り）履歴を取得
    Returns: (最新日付, 最新買い残, 最新売り残, ネット残履歴[古→新])
    """
    if not SRC_A_URL:
        return None, None, None, []

    try:
        r = s.get(SRC_A_URL, timeout=20)
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
        print(f"[Error] SRC_A fetch: {e}")

    return None, None, None, []


def fetch_prime_volume(s: requests.Session) -> Tuple[Optional[date], Optional[float]]:
    """SRC_B から「プライム市場 売買高」を取得。取れないなら (None, None)。"""
    if not SRC_B_URL:
        return None, None

    try:
        r = s.get(SRC_B_URL, timeout=20)
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
        print(f"[Error] SRC_B fetch: {e}")

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
    """TOPIX（1306.T）の価格位置（PCTL×DEV200 AND）"""
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
    （修正：下落警戒の符号重視フラグ basis_stress_down を追加）
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
    basis_stress_down = (basis_today < 0) and (basis_today <= basis_5ago)

    return {
        "ok": True,
        "basis_today": basis_today,
        "basis_5ago": basis_5ago,
        "stuck": bool(stuck),
        "basis_stress_down": bool(basis_stress_down),
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

    today = date.today()
    if is_market_closed(today):
        print(f"\n[INFO] 休場日のため停止します（判定・保存ともに実施しません）: {today.isoformat()}")
        return

    if not SRC_A_URL or not SRC_B_URL:
        print("\n[INFO] 入力が未設定のため停止します（SRC_A_URL / SRC_B_URL）")
        return

    s = get_session()
    state = load_state()

    # 1) データ取得（SRC_A / SRC_B / yfinance）
    arb_date, arb_buy, arb_sell, arb_net_hist = fetch_arbitrage_data(s)
    vol_date, prime_vol = fetch_prime_volume(s)

    topix_pos = compute_topix_position()
    move_info = compute_daily_move_pct()
    basis_info = compute_basis_stuck_nk()

    # 修正：判定日を arb_date > vol_date > today の優先順で決める
    report_dt = arb_date if arb_date else (vol_date if vol_date else today)

    # 2) 非取引日/更新なし対策（保存と判定）
    state_updated = False
    if vol_date and isinstance(prime_vol, (int, float)) and prime_vol and prime_vol > 0:
        state_updated = update_volume_history(state, vol_date, float(prime_vol))
        if state_updated:
            save_state(state)

    # 修正：判定不能（INSUFFICIENT）を導入（欠損をNORMALに落とさない）
    insufficient_reasons: List[str] = []

    # 必須：ARB（最新BUY/SELLとΔ5計算と分位窓）
    if arb_buy is None or arb_sell is None:
        insufficient_reasons.append("ARB: 最新BUY/SELLが取得不能")
    d5 = calc_delta(arb_net_hist, ARB_DELTA_MAIN)
    if d5 is None:
        insufficient_reasons.append("ARB: Δ5が計算不能（履歴不足）")

    # 必須：出来高（当日値とMA20）
    vol_ma = get_volume_ma(state, VOL_MA_DAYS)
    if prime_vol is None or not isinstance(prime_vol, (int, float)) or not prime_vol or prime_vol <= 0:
        insufficient_reasons.append("VOL: プライム売買高が取得不能")
    if vol_ma is None:
        insufficient_reasons.append("VOL: MA20が計算不能（state.json履歴不足）")

    # 必須：価格変動（当日）
    if not move_info.get("ok"):
        insufficient_reasons.append("PX: 前日比%が取得不能（yfinance）")

    # 必須：EMERGENCY分位（q=0.99）
    q99_move = compute_move_abs_q99()
    if q99_move is None:
        insufficient_reasons.append("EMERGENCY: q=0.99閾値が算出不能（履歴不足/取得失敗）")

    # report_dtが確定できないケース（念のため）
    if report_dt is None:
        insufficient_reasons.append("DATE: report_dtが確定不能")

    if insufficient_reasons:
        print(f"\n[判定日] {today.isoformat()}")
        print("[判定結果] LEVEL 0: INSUFFICIENT (判定不能)")
        print("必要データが揃わないため、本日は判定しません。")
        print("-" * 50)
        for r in insufficient_reasons:
            print(f" - {r}")
        print("=" * 50)
        print("ALERT_VOLATILITY_RISK = False")
        return

    # 3) 裁定ネット残ロジック（Δ3/Δ5/Δ25） ※d5は上で計算済み
    d3 = calc_delta(arb_net_hist, ARB_DELTA_SHORT)
    d25 = calc_delta(arb_net_hist, ARB_DELTA_LONG)

    arb_net_latest = float(arb_buy - arb_sell)

    arb_stats = compute_arb_stats(arb_net_hist, arb_net_latest)
    if arb_stats is None or d25 is None:
        print(f"\n[判定日] {report_dt.isoformat()}")
        print("[判定結果] LEVEL 0: INSUFFICIENT (判定不能)")
        print("ARB統計（分位/中央値/MARGIN）またはΔ25が計算できないため、本日は判定しません。")
        print("=" * 50)
        print("ALERT_VOLATILITY_RISK = False")
        return

    # 修正：ARB_STUCKをWEAK/STRONGに分割（MARGIN比率＋FLOOR）
    margin_5 = float(arb_stats["margin_5"])
    margin_25 = float(arb_stats["margin_25"])
    arb_high = bool(arb_stats["arb_high"])

    arb_stuck_weak = (d5 >= -margin_5)
    arb_stuck_strong = (d5 >= -margin_5) and (d25 >= -margin_25) and arb_high

    arb_info_boost = False
    if d3 is not None:
        arb_info_boost = (d3 >= 0)

    # 4) SQ（メジャーSQ接近を重視） ※修正：必須ではなくブーストに使う
    d2sq = get_days_to_sq(report_dt)
    major_sq_near = (d2sq <= SQ_NEAR_DAYS) and is_major_sq_month(report_dt)

    # 5) 流動性の質低下（出来高×価格変動の不整合）
    liq_mismatch = False
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

    # 7) 裁定ストレス補助（先物−現物）
    basis_stuck_nk = bool(basis_info.get("stuck")) if basis_info.get("ok") else False
    basis_stress_down = bool(basis_info.get("basis_stress_down")) if basis_info.get("ok") else False

    # 8) EMERGENCY（q=0.99 と固定3%のmax）
    em_th = max(float(EMERGENCY_FIXED_TH), float(q99_move))
    emergency_move = (px_move_abs is not None) and (float(px_move_abs) >= em_th)

    # 9) 総合判定（修正）
    # WARNING:
    #   A) ARB_STRONG × LIQ × (IDX_HIGH or BASIS_STRESS_DOWN)
    #   B) ARB_WEAK × LIQ × EMERGENCY（SQ不要）
    warning = (
        (arb_stuck_strong and liq_mismatch and (idx_high_topix or basis_stress_down))
        or (arb_stuck_weak and liq_mismatch and emergency_move)
    )

    # CAUTION:
    #   ARB（WEAK/STRONG）× LIQ × (SQ or IDX_HIGH or BASIS_STRESS_DOWN or EMERGENCY)
    caution = (
        (not warning)
        and (arb_stuck_weak or arb_stuck_strong)
        and liq_mismatch
        and (major_sq_near or idx_high_topix or basis_stress_down or emergency_move)
    )

    # 修正：SQブースト（CAUTION かつ major_sq_near なら WARNING に格上げ）
    sq_boosted = False
    if caution and major_sq_near:
        warning = True
        caution = False
        sq_boosted = True

    if warning:
        level = "LEVEL 3: WARNING (警戒)"
        if sq_boosted:
            msg = "裁定×流動性不整合が成立し、メジャーSQ接近により警戒へ格上げ。市場が壊れやすい環境です。"
        else:
            msg = "裁定×流動性不整合の条件が強く成立。市場が壊れやすい環境です。"
    elif caution:
        level = "LEVEL 2: CAUTION (注意)"
        msg = "裁定の歪みと流動性不整合が観測。イベント要因次第で荒れやすい状態です。"
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

    print("A) 裁定ネット残（SRC_A）")
    print(f"   最新 BUY: {arb_buy/1e8:.2f}億株 / SELL: {arb_sell/1e8:.2f}億株 / NET: {arb_net_latest/1e8:.2f}億株")
    print(f"   Δ{ARB_DELTA_SHORT}: {d3 if d3 is not None else 'N/A'}  (INFO加点={arb_info_boost})")
    print(f"   Δ{ARB_DELTA_MAIN}:  {d5 if d5 is not None else 'N/A'}")
    print(f"   Δ{ARB_DELTA_LONG}: {d25 if d25 is not None else 'N/A'}")
    print(f"   ARB_PCTL(window_n={arb_stats['window_n']}): {arb_stats['pctl']:.3f} / ARB_HIGH: {arb_high}")
    print(f"   MARGIN_5: {margin_5:.0f} / MARGIN_25: {margin_25:.0f}")
    print(f"   ARB_STUCK_WEAK: {arb_stuck_weak} / ARB_STUCK_STRONG: {arb_stuck_strong}")

    print("\nB) SQ（メジャーSQはブースト要因）")
    print(f"   D2SQ: {d2sq}日 / メジャーSQ月: {is_major_sq_month(report_dt)} / MAJOR_SQ_NEAR: {major_sq_near}")

    print("\nC) 流動性の質（出来高×価格変動 不整合）")
    if vol_ratio is not None:
        print(f"   プライム売買高: {float(prime_vol)/1e8:.2f}億株 / MA20: {float(vol_ma)/1e8:.2f}億株 / 比率: {vol_ratio:.2f}")
    else:
        print("   データ不足（state.json蓄積中 or 取得失敗）")

    if px_move_abs is not None:
        print(f"   価格変動(|前日比%|): {px_move_abs:.2f}%（ソース: {px_move_src}）")
        print(f"   EMERGENCY_TH: max({EMERGENCY_FIXED_TH:.1f}%, q99={q99_move:.2f}%) = {em_th:.2f}% / EMERGENCY_MOVE: {emergency_move}")
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

    print("\nE) 裁定ストレス補助（日経先物−現物）")
    if basis_info.get("ok"):
        print(f"   先物ティッカー: {N225_FUT_TICKER} / 現物: {N225_TICKER}")
        print(f"   BASIS 今日: {basis_info['basis_today']:.2f} / 5営業日前: {basis_info['basis_5ago']:.2f}")
        print(f"   BASIS_STUCK: {basis_stuck_nk} / BASIS_STRESS_DOWN: {basis_stress_down}")
    else:
        print(f"   BASIS: 取得失敗/データ不足（先物ティッカー: {N225_FUT_TICKER}）")

    print("\nF) state.json")
    print(f"   更新: {state_updated} / 保持件数: {len(state.get('history', []))} / 上限: {STATE_MAX_RECORDS}")
    print("=" * 50)
    print(f"ALERT_VOLATILITY_RISK = {warning}")


if __name__ == "__main__":
    main()
