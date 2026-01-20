from __future__ import annotations

import io
import json
import os
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
import yfinance as yf
import pdfplumber

STATE_PATH = Path("state.json")

# ====== 仕様（あなたの決定） ======
MA_DAYS = 20
MAX_HISTORY_DAYS = 900  # 3年 ≒ 756営業日 + バッファ

ARB_BUY_RATIO_TH = 1.5
PRIME_VOL_RATIO_TH = 0.85
SQ_NEAR_DAYS = 5

# 指数高値圏（過去3年）判定
INDEX_PCTL = 0.90  # 90%点
INDEX_TICKER = os.getenv("INDEX_TICKER", "^N225")  # デフォ: 日経225。TOPIXにしたければ ^TOPX 等を設定
INDEX_LOOKBACK = "3y"

JPX_PROGRAM_URL = "https://www.jpx.co.jp/markets/statistics-equities/program/index.html"
JPX_DAILY_URL = "https://www.jpx.co.jp/markets/statistics-equities/daily/index.html"

UA = "Mozilla/5.0 (compatible; jpx-bot/1.0; +https://github.com/)"


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
    """
    LEVELのルール（固定）:
      - LEVEL 3: ALERT=True
      - LEVEL 2: ALERT=False かつ 条件が2つ以上TRUE
      - LEVEL 1: それ以外
    """
    true_cnt = sum(1 for v in conds.values() if v)
    if alert:
        return (
            "LEVEL 3: WARNING (警戒)",
            "【警戒】急変しやすい条件が揃っています。建玉サイズ・新規投入を抑え、SQ週は特に慎重に。",
        )
    if true_cnt >= 2:
        return (
            "LEVEL 2: CAUTION (注意)",
            "【注意】一部の歪みが出ています。無理な買い方（レバ/一括）を避け、分割と余力重視。",
        )
    return ("LEVEL 1: NORMAL (正常)", "【順行】構造的な危機条件は未成立。通常運用で問題ありません。")


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
    print("📊 JPX 裁定・SQ・流動性レポート (v1.0)")
    print("=" * 60)
    print(f"AsOf: {latest['asof']}")
    print("")
    print("[入力データの日付]")
    print(f"- 裁定取引（JPX）: {latest['inputs']['arb_date']}  ※JPXは遅延の可能性あり")
    print(f"- プライム出来高（JPX日報）: {latest['inputs']['prime_volume_date']}")
    print(f"- 指数（{idx['ticker']}）: {idx['index_latest_date']}（終値ベース）")
    print("-" * 60)
    print("")

    # 1) Arbitrage
    print("1. Condition: Arbitrage Stretch（裁定買い残の積み上がり）")
    print(
        f"   結果: {fmt_num(metrics['arb_buy_ratio_ma20'])}  (閾値: >= {thr['arb_buy_ratio_ma20_hot']}) → [{fmt_bool(conds['arb_buy_hot'])}]"
    )
    print("   [分析]:")
    if metrics["arb_buy_ratio_ma20"] is None:
        print("   - データ不足（MA20未成立）。20営業日分が貯まるまで判定保留。")
    elif conds["arb_buy_hot"]:
        print("   - 裁定買い残が平常より大きく、解消が走ると現物売り圧が出やすい状態です。")
    else:
        print("   - 裁定買い残は平常レンジ。需給の“火薬庫”は大きくありません。")
    print("")

    # 2) SQ near
    print("2. Trigger: SQ Near（SQ接近）")
    print(
        f"   結果: days_to_2nd_fri = {metrics['days_to_2nd_fri']}  (閾値: <= {thr['sq_near_days']}) → [{fmt_bool(conds['sq_near'])}]"
    )
    print("   [分析]:")
    if metrics["days_to_2nd_fri"] is None:
        print("   - 日付計算に失敗（想定外）。")
    elif conds["sq_near"]:
        print("   - 価格差が締まりやすい期間。裁定の解消が同方向に出ると値が飛びやすい。")
    else:
        print("   - SQは近くありません。イベント要因は弱い。")
    print("")

    # 3) Prime volume thin
    print("3. Trigger: Prime Liquidity Thin（プライム流動性の薄さ）")
    print(
        f"   結果: {fmt_num(metrics['prime_volume_ratio_ma20'])}  (閾値: <= {thr['prime_volume_ratio_ma20_thin']}) → [{fmt_bool(conds['prime_volume_thin'])}]"
    )
    print("   [分析]:")
    if metrics["prime_volume_ratio_ma20"] is None:
        print("   - データ不足（MA20未成立）。20営業日分が貯まるまで判定保留。")
    elif conds["prime_volume_thin"]:
        print("   - 市場の受け皿が薄い。小さな解消でも値が滑りやすい局面です。")
    else:
        print("   - 出来高は平常域。受け皿は極端に薄くありません。")
    print("")

    # 4) Index high zone
    print(f"4. Condition: Index High Zone（指数の高値圏：過去3年 p{int(thr['index_pctl']*100)}）")
    print(
        f"   結果: latest_close={fmt_num(idx['latest_close'])}, threshold={fmt_num(idx['threshold_close'])} → [{fmt_bool(conds['index_high_zone'])}]"
    )
    print("   [分析]:")
    if conds["index_high_zone"]:
        print("   - 価格位置が上側に寄っています。崩れるときの下方向の振れが出やすい側です。")
    else:
        print("   - 高値圏ではありません。価格位置の“上詰まり”要因は弱い。")
    print("-" * 60)

    print("")
    print("[最終判定]")
    print(f"ALERT_VOLATILITY_RISK = {fmt_bool(alert)}")
    print(f"Rule: {latest['alert']['rule']}")
    if alert:
        print("成立条件:")
        for k in latest["alert"]["reasons"]:
            print(f"- {k}")
    print("")


def sess() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    return s


def _abs_url(base: str, href: str) -> str:
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return "https://www.jpx.co.jp" + href
    base_dir = base.rsplit("/", 1)[0] + "/"
    return base_dir + href


def load_state() -> Dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return {
        "meta": {"created_at": datetime.now().isoformat(), "updated_at": None, "version": 2},
        "history": [],
        "latest": {},
    }


def save_state(state: Dict) -> None:
    state["meta"]["updated_at"] = datetime.now().isoformat()
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def upsert_history(state: Dict, record: Dict) -> None:
    ds = record["date"]
    hist = state["history"]

    for i, r in enumerate(hist):
        if r.get("date") == ds:
            merged = dict(r)
            for k, v in record.items():
                if k == "signals":
                    merged.setdefault("signals", {})
                    merged["signals"].update(v or {})
                else:
                    if v is not None:
                        merged[k] = v
            hist[i] = merged
            break
    else:
        hist.append(record)

    hist.sort(key=lambda x: x.get("date"))
    if len(hist) > MAX_HISTORY_DAYS:
        state["history"] = hist[-MAX_HISTORY_DAYS:]


def ma_ratio(values: List[float], window: int = MA_DAYS) -> Optional[float]:
    if len(values) < window:
        return None
    s = pd.Series(values, dtype="float64")
    ma = float(s.tail(window).mean())
    if ma == 0:
        return None
    return float(s.iloc[-1] / ma)


def days_to_2nd_friday(today: date) -> int:
    y, m = today.year, today.month
    first = date(y, m, 1)
    days_to_fri = (4 - first.weekday()) % 7  # 4=Fri
    first_fri = first + timedelta(days=days_to_fri)
    second_fri = first_fri + timedelta(days=7)
    return (second_fri - today).days


def is_sq_near(today: date) -> Tuple[bool, int]:
    d = days_to_2nd_friday(today)
    return (0 <= d <= SQ_NEAR_DAYS), d


# ========= JPX 裁定取引 (PDF Ver.) =========
def fetch_latest_arbitrage_url(s: requests.Session) -> Tuple[date, str]:
    """
    JPXのページから最新の「裁定取引」PDFのURLを取得する。
    """
    r = s.get(JPX_PROGRAM_URL, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    candidates = []

    # Method 1: Row Scanning (Text based)
    for tr in soup.find_all("tr"):
        text = tr.get_text(" ", strip=True)
        text = re.sub(r"\s+", " ", text)
        
        # Pattern A: 2026年1月16日
        m = re.search(r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日", text)
        if not m:
            m = re.search(r"(\d{4})/\s*(\d{1,2})/\s*(\d{1,2})", text)
            
        if m:
            y, mo, d = map(int, m.groups())
            dt = date(y, mo, d)
            link = tr.find("a", href=re.compile(r"\.pdf", re.IGNORECASE))
            if link:
                url = _abs_url(JPX_PROGRAM_URL, link["href"])
                candidates.append((dt, url))
                continue

    # Method 2: Filename Scanning (Fallback)
    if not candidates:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not re.search(r"\.pdf", href, re.IGNORECASE):
                continue
            
            url = _abs_url(JPX_PROGRAM_URL, href)
            filename = href.split("/")[-1]
            
            # Pattern C: 20260116.pdf
            m8 = re.search(r"(20\d{2})(\d{2})(\d{2})", filename)
            if m8:
                y, mo, d = map(int, m8.groups())
                candidates.append((date(y, mo, d), url))
                continue
            
            # Pattern D: 260116.pdf (YYMMDD)
            m6 = re.search(r"(\d{2})(\d{2})(\d{2})", filename)
            if m6:
                y_short, mo, d = map(int, m6.groups())
                if 1 <= mo <= 12 and 1 <= d <= 31:
                    y = 2000 + y_short
                    candidates.append((date(y, mo, d), url))
                    continue

    if not candidates:
        raise RuntimeError("JPX program page: No arbitrage PDF links found.")

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0]


def download_bytes(s: requests.Session, url: str) -> bytes:
    r = s.get(url, timeout=60)
    r.raise_for_status()
    return r.content


def extract_arbitrage_data_from_pdf(pdf_bytes: bytes) -> Tuple[float, float]:
    """
    裁定取引PDFから「裁定売り残」「裁定買い残」を抽出する。
    ターゲット: 「2. 裁定取引に係る現物ポジション」の表にある「株数」行
    """
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        # 全ページのテーブルを抽出して探索
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                # テーブル内の各行をチェック
                for row in table:
                    # Noneを除去して文字列化
                    clean_row = [str(x).replace(",", "").replace(" ", "").strip() if x else "" for x in row]
                    
                    # 1列目が「株数」またはそれに類する行を探す
                    if len(clean_row) > 0 and "株数" in clean_row[0]:
                        # 数値が含まれる要素を抽出
                        nums = []
                        for cell in clean_row:
                            # 数字のみ抽出（マイナス等は考慮しない、残高は正数前提）
                            # ただしOCR等で改行が入る場合があるため "123\n456" 等もケア
                            # ここでは単純に "1048783" のような数値を探す
                            ms = re.findall(r"\d+", cell)
                            # 連結して数値化（例: "1,048, 783" -> ["1048", "783"] -> 1048783）
                            if ms:
                                val_str = "".join(ms)
                                try:
                                    nums.append(float(val_str))
                                except:
                                    pass
                        
                        # 抽出できた数値の数で判定
                        # 通常の並び: [株数, 売り当限, 売り合計, 売り翌限, 買い当限, 買い翌限, 買い合計]
                        # 合計列は通常「売り合計」「買い合計」の2つが必要。
                        # 数値リストの中で、売り残と買い残を特定する。
                        # 経験則: 
                        #   数値が2つしかない -> [売り合計, 買い合計] (簡易表)
                        #   数値が5つ以上ある -> 詳細表。
                        #     売り合計 = 前半の大きな値 (あるいは2番目)
                        #     買い合計 = 最後の値 (あるいは最後から2番目)
                        
                        if len(nums) >= 2:
                            # PDFの構造上、右端が「買い合計」、その少し左が「売り合計」であることが多い
                            # 具体的には:
                            #   Row: [株数, Sell_Near, Sell_Total, Sell_Far, Buy_Near, Buy_Far, Buy_Total]
                            #   nums: [Sell_Near, Sell_Total, Sell_Far, Buy_Near, Buy_Far, Buy_Total] (0を除く)
                            #   売り合計: nums[1] (Sell_Total)
                            #   買い合計: nums[-1] (Buy_Total)
                            
                            # ただし、Sell_Farなどが0で省略されたり結合されたりする場合があるため、
                            # より安全な策として「最後」を買い残、「真ん中あたり」を売り残と推定するが、
                            # ここではPDFの典型的レイアウト（提供されたPDF）に従う。
                            
                            arb_buy = nums[-1]  # 最後の数値が買い残合計
                            
                            # 売り残は、数値が多ければ2番目、少なければ1番目
                            if len(nums) >= 5:
                                arb_sell = nums[1]
                            else:
                                arb_sell = nums[0]
                                
                            return arb_buy, arb_sell

    raise RuntimeError("Arbitrage data (Buy/Sell positions) not found in PDF tables.")


# ========= JPX 日報（プライム売買高）(Robust Ver. 3) =========
def fetch_latest_daily_pdf_url(s: requests.Session) -> Tuple[date, str]:
    r = s.get(JPX_DAILY_URL, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    candidates = []

    # Method 1: Row Scanning
    for tr in soup.find_all("tr"):
        text = tr.get_text(" ", strip=True)
        text = re.sub(r"\s+", " ", text)
        
        m = re.search(r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日", text)
        if not m:
            m = re.search(r"(\d{4})/\s*(\d{1,2})/\s*(\d{1,2})", text)
        
        if m:
            y, mo, d = map(int, m.groups())
            dt = date(y, mo, d)
            link = tr.find("a", href=re.compile(r"\.pdf", re.IGNORECASE))
            if link:
                url = _abs_url(JPX_DAILY_URL, link["href"])
                candidates.append((dt, url))
                continue

    # Method 2: Filename Scanning
    if not candidates:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not re.search(r"\.pdf", href, re.IGNORECASE):
                continue
            
            url = _abs_url(JPX_DAILY_URL, href)
            filename = href.split("/")[-1]
            
            # Pattern C: 20260116.pdf
            m8 = re.search(r"(20\d{2})(\d{2})(\d{2})", filename)
            if m8:
                y, mo, d = map(int, m8.groups())
                candidates.append((date(y, mo, d), url))
                continue

            # Pattern D: 260116.pdf (YYMMDD)
            m6 = re.search(r"(\d{2})(\d{2})(\d{2})", filename)
            if m6:
                y_short, mo, d = map(int, m6.groups())
                if 1 <= mo <= 12 and 1 <= d <= 31:
                    y = 2000 + y_short
                    candidates.append((date(y, mo, d), url))
                    continue

    if not candidates:
        raise RuntimeError("JPX daily report page: No PDF links found (all methods failed).")

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0]


def extract_prime_volume_from_pdf(pdf_bytes: bytes) -> float:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        lines: List[str] = []
        for page in pdf.pages:
            t = page.extract_text() or ""
            for ln in t.splitlines():
                ln = ln.strip()
                if ln:
                    lines.append(ln)

    keys = ["プライム", "Prime", "東証プライム"]
    for ln in lines:
        if any(k in ln for k in keys):
            nums = re.findall(r"\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?", ln)
            if not nums:
                continue
            vals = []
            for s in nums:
                try:
                    vals.append(float(s.replace(",", "")))
                except Exception:
                    pass
            if vals:
                return max(vals)

    raise RuntimeError("Prime volume not found in daily PDF text")


# ========= 指数（過去3年の高値圏） =========
def fetch_index_high_zone(ticker: str, pctl: float, lookback: str) -> Dict:
    """
    過去3年の終値分布に対して、最新終値がpctl以上かを判定。
    """
    df = yf.download(ticker, period=lookback, interval="1d", auto_adjust=False, progress=False)
    if df is None or df.empty:
        raise RuntimeError(f"yfinance: no data for ticker={ticker}")

    if "Close" not in df.columns:
        raise RuntimeError(f"yfinance: Close not found for ticker={ticker}")

    close = df["Close"].dropna()
    if close.empty:
        raise RuntimeError(f"yfinance: Close empty for ticker={ticker}")

    latest_close = float(close.iloc[-1])
    q = float(close.quantile(pctl))
    high_zone = latest_close >= q

    return {
        "ticker": ticker,
        "lookback": lookback,
        "pctl": pctl,
        "latest_close": latest_close,
        "threshold_close": q,
        "index_high_zone": bool(high_zone),
        "index_latest_date": close.index[-1].date().isoformat(),
    }


def compute_latest(state: Dict, index_info: Dict) -> Dict:
    hist = state["history"]

    arb_days = [r for r in hist if isinstance(r.get("arb_buy"), (int, float))]
    vol_days = [r for r in hist if isinstance(r.get("prime_volume"), (int, float))]

    latest = {
        "asof": datetime.now().astimezone().isoformat(),
        "inputs": {
            "arb_date": None,
            "arb_buy": None,
            "arb_sell": None,
            "prime_volume_date": None,
            "prime_volume": None,
            "index": index_info,
        },
        "metrics": {
            "arb_buy_ratio_ma20": None,
            "prime_volume_ratio_ma20": None,
            "days_to_2nd_fri": None,
            "index_latest_close": index_info["latest_close"],
            "index_threshold_close_pctl": index_info["threshold_close"],
        },
        "thresholds": {
            "arb_buy_ratio_ma20_hot": ARB_BUY_RATIO_TH,
            "prime_volume_ratio_ma20_thin": PRIME_VOL_RATIO_TH,
            "sq_near_days": SQ_NEAR_DAYS,
            "index_pctl": INDEX_PCTL,
        },
        "conditions": {
            "arb_buy_hot": False,
            "sq_near": False,
            "prime_volume_thin": False,
            "index_high_zone": bool(index_info["index_high_zone"]),
        },
        "alert": {
            "volatility_risk": False,
            "rule": "arb_buy_hot & sq_near & prime_volume_thin & index_high_zone",
            "reasons": [],
        },
    }

    # --- arbitrage condition ---
    if arb_days:
        arb_days.sort(key=lambda x: x["date"])
        series = [float(r["arb_buy"]) for r in arb_days]
        ratio = ma_ratio(series, MA_DAYS)
        arb_latest = arb_days[-1]
        today = date.fromisoformat(arb_latest["date"])
        sq_near, d2f = is_sq_near(today)

        latest["inputs"]["arb_date"] = arb_latest["date"]
        latest["inputs"]["arb_buy"] = float(arb_latest["arb_buy"])
        latest["inputs"]["arb_sell"] = float(arb_latest["arb_sell"])

        latest["metrics"]["arb_buy_ratio_ma20"] = ratio
        latest["metrics"]["days_to_2nd_fri"] = d2f

        arb_hot = ratio is not None and ratio >= ARB_BUY_RATIO_TH
        latest["conditions"]["arb_buy_hot"] = bool(arb_hot)
        latest["conditions"]["sq_near"] = bool(sq_near)

    # --- volume condition ---
    if vol_days:
        vol_days.sort(key=lambda x: x["date"])
        series = [float(r["prime_volume"]) for r in vol_days]
        ratio = ma_ratio(series, MA_DAYS)
        vol_latest = vol_days[-1]

        latest["inputs"]["prime_volume_date"] = vol_latest["date"]
        latest["inputs"]["prime_volume"] = float(vol_latest["prime_volume"])

        latest["metrics"]["prime_volume_ratio_ma20"] = ratio

        vol_thin = ratio is not None and ratio <= PRIME_VOL_RATIO_TH
        latest["conditions"]["prime_volume_thin"] = bool(vol_thin)

    # --- alert ---
    c = latest["conditions"]
    alert = c["arb_buy_hot"] and c["sq_near"] and c["prime_volume_thin"] and c["index_high_zone"]
    latest["alert"]["volatility_risk"] = bool(alert)
    latest["alert"]["reasons"] = [k for k, v in c.items() if v]

    state["latest"] = latest
    return latest


def main() -> None:
    s = sess()
    state = load_state()

    # 1) 裁定残（最新分） PDF版
    arb_dt, arb_url = fetch_latest_arbitrage_url(s)
    arb_pdf = download_bytes(s, arb_url)
    arb_buy, arb_sell = extract_arbitrage_data_from_pdf(arb_pdf)

    upsert_history(
        state,
        {
            "date": arb_dt.isoformat(),
            "arb_buy": arb_buy,
            "arb_sell": arb_sell,
            "arb_net": arb_buy - arb_sell,
            "prime_volume": None,
            "signals": {},
            "src": {"arb_pdf": arb_url},
        },
    )

    # 2) 日報（最新分）プライム売買高
    vol_dt, pdf_url = fetch_latest_daily_pdf_url(s)
    pdf_bytes = download_bytes(s, pdf_url)
    prime_volume = extract_prime_volume_from_pdf(pdf_bytes)

    upsert_history(
        state,
        {
            "date": vol_dt.isoformat(),
            "arb_buy": None,
            "arb_sell": None,
            "arb_net": None,
            "prime_volume": prime_volume,
            "signals": {},
            "src": {"daily_pdf": pdf_url},
        },
    )

    # 3) 指数高値圏
    index_info = fetch_index_high_zone(INDEX_TICKER, INDEX_PCTL, INDEX_LOOKBACK)

    # 4) 判定まとめ（latest）
    latest = compute_latest(state, index_info)
    save_state(state)

    # 5) runログ出力（レポート形式）
    print_report(latest)


if __name__ == "__main__":
    main()
