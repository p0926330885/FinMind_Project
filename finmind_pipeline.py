#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
板塊泡泡系統 — 第一階段 FinMind 資料管線（法人資金主線）
v1.5.1（缺洞感知版）

本次升級（vs v1.5）：
  ① 加 FORCE_REFETCH_DAYS（預設 15 天）：每次跑都強制回抓最近 N 天，
     即使 SQLite 快取顯示已有資料——避免 FinMind 資料延遲導致的殘餘缺洞。
  ② compute_metrics 新增「有股價但缺法人資料」偵測邏輯：
     - 該股該日缺洞 → 從個股 hist 移除該日（不寫假的 0）
     - 板塊 sec_hist 保留該日、標記 "missing": N（前端可提示）
     - 板塊成交值 to 仍用股價的 turnover 補計，維持基準正確
  ③ log 會列出近 7 天所有缺洞明細（前 15 筆），方便盯 FinMind 資料完整度。
  ④ 輸出 JSON 新增 top-level "missing_stock_dates" 統計。

執行：  python finmind_pipeline.py
相依：  pip install -r requirements.txt
"""

import os
import sys
import time
import json
import sqlite3
import logging
import datetime as dt
from pathlib import Path

import requests
import pandas as pd

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ─────────────────────────── 路徑設定（相對） ───────────────────────────
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("DATA_DIR", BASE_DIR / "data"))
CONFIG_PATH = Path(os.environ.get("SECTORS_CONFIG", BASE_DIR / "sectors_config.json"))
CACHE_DB = DATA_DIR / "cache.sqlite"
OUTPUT_JSON = DATA_DIR / "bubble_data.json"
LOG_FILE = Path(os.environ.get("LOG_FILE", BASE_DIR / "pipeline.log"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────── 參數（可用環境變數覆寫） ───────────────────────────
TOKEN = os.environ.get("FINMIND_TOKEN", "").strip()
BACKFILL_DAYS = int(os.environ.get("BACKFILL_DAYS", "120"))
OVERLAP_DAYS = int(os.environ.get("OVERLAP_DAYS", "10"))
FORCE_REFETCH_DAYS = int(os.environ.get("FORCE_REFETCH_DAYS", "15"))  # ★ 新增
REQUEST_SLEEP = float(os.environ.get("REQUEST_SLEEP", "0.4"))
MAX_RETRY = int(os.environ.get("MAX_RETRY", "4"))

API_URL = "https://api.finmindtrade.com/api/v4/data"
USER_INFO_URL = "https://api.web.finmindtrade.com/v2/user_info"

FOREIGN = {"Foreign_Investor", "Foreign_Dealer_Self"}
TRUST = {"Investment_Trust"}
DEALER = {"Dealer_self", "Dealer_Hedging"}
ALL_INV = FOREIGN | TRUST | DEALER

# ─────────────────────────── 日誌 ───────────────────────────
logger = logging.getLogger("pipeline")
logger.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", "%Y-%m-%d %H:%M:%S")
_fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
_fh.setFormatter(_fmt)
_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(_fmt)
if not logger.handlers:
    logger.addHandler(_fh)
    logger.addHandler(_sh)


class QuotaExceeded(Exception):
    """FinMind 額度用盡（HTTP/status 402）。"""


# ─────────────────────────── SQLite 快取 ───────────────────────────
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(CACHE_DB)
    conn.execute("""CREATE TABLE IF NOT EXISTS inst_raw(
        stock_id TEXT, date TEXT, name TEXT, buy REAL, sell REAL,
        PRIMARY KEY(stock_id, date, name))""")
    conn.execute("""CREATE TABLE IF NOT EXISTS price_raw(
        stock_id TEXT, date TEXT, close REAL, trading_money REAL, trading_volume REAL,
        PRIMARY KEY(stock_id, date))""")
    conn.commit()
    return conn


def last_cached_date(conn, table, stock_id):
    row = conn.execute(f"SELECT MAX(date) FROM {table} WHERE stock_id=?", (stock_id,)).fetchone()
    return row[0] if row and row[0] else None


# ─────────────────────────── FinMind 取數 ───────────────────────────
def _headers():
    return {"Authorization": f"Bearer {TOKEN}"} if TOKEN else {}


def check_quota():
    try:
        r = requests.get(USER_INFO_URL, headers=_headers(), timeout=15)
        j = r.json()
        used = j.get("user_count")
        limit = j.get("api_request_limit")
        logger.info(f"FinMind 額度：已用 {used} / 上限 {limit} （每小時）")
        return used, limit
    except Exception as e:
        logger.warning(f"查詢額度失敗（略過）：{e}")
        return None, None


def fetch_finmind(dataset, data_id, start_date, end_date):
    params = {"dataset": dataset, "data_id": data_id,
              "start_date": start_date, "end_date": end_date}
    for attempt in range(1, MAX_RETRY + 1):
        try:
            r = requests.get(API_URL, headers=_headers(), params=params, timeout=30)
            if r.status_code == 402:
                raise QuotaExceeded(f"{dataset}/{data_id} 回 HTTP 402")
            r.raise_for_status()
            j = r.json()
            if str(j.get("status")) == "402":
                raise QuotaExceeded(f"{dataset}/{data_id} 回 status 402：{j.get('msg')}")
            if j.get("status") not in (200, "200", None):
                logger.warning(f"{dataset}/{data_id} status={j.get('status')} msg={j.get('msg')}")
            return j.get("data", []) or []
        except QuotaExceeded:
            raise
        except Exception as e:
            wait = min(60, 2 ** attempt)
            logger.warning(f"{dataset}/{data_id} 第 {attempt} 次失敗：{e}；{wait}s 後重試")
            time.sleep(wait)
    logger.error(f"{dataset}/{data_id} 重試 {MAX_RETRY} 次仍失敗，跳過")
    return []


def upsert_inst(conn, rows):
    if not rows:
        return 0
    payload = [(r.get("stock_id"), r.get("date"), r.get("name"),
                r.get("buy") or 0, r.get("sell") or 0) for r in rows]
    conn.executemany("INSERT OR REPLACE INTO inst_raw VALUES(?,?,?,?,?)", payload)
    return len(payload)


def upsert_price(conn, rows):
    if not rows:
        return 0
    payload = [(r.get("stock_id"), r.get("date"), r.get("close"),
                r.get("Trading_money"), r.get("Trading_Volume")) for r in rows]
    conn.executemany("INSERT OR REPLACE INTO price_raw VALUES(?,?,?,?,?)", payload)
    return len(payload)


def _start_for(conn, table, code, today):
    """算增量抓取起點。
    ★ v1.5.1 改：取 (快取最後日 - OVERLAP) 和 (今天 - FORCE_REFETCH) 兩者較早者，
      確保即使快取顯示有資料，也會強制回抓最近 FORCE_REFETCH_DAYS 天，
      修復因 FinMind 資料延遲寫進來的缺洞。
    """
    force_start = today - dt.timedelta(days=FORCE_REFETCH_DAYS)
    last = last_cached_date(conn, table, code)
    if last is None:
        return (today - dt.timedelta(days=BACKFILL_DAYS)).isoformat()
    d = dt.date.fromisoformat(last) - dt.timedelta(days=OVERLAP_DAYS)
    return min(d, force_start).isoformat()


def update_universe(conn, codes, today):
    """對唯一母體逐檔增量抓 法人 + 股價。"""
    end = today.isoformat()
    n = len(codes)
    for i, code in enumerate(sorted(codes), 1):
        inst_start = _start_for(conn, "inst_raw", code, today)
        price_start = _start_for(conn, "price_raw", code, today)

        inst = fetch_finmind("TaiwanStockInstitutionalInvestorsBuySell", code, inst_start, end)
        time.sleep(REQUEST_SLEEP)
        price = fetch_finmind("TaiwanStockPrice", code, price_start, end)
        time.sleep(REQUEST_SLEEP)

        a = upsert_inst(conn, inst)
        b = upsert_price(conn, price)
        conn.commit()
        if i % 10 == 0 or i == n:
            logger.info(f"抓取進度 {i}/{n}（{code}：法人 {a} 筆、股價 {b} 筆）")


# ─────────────────────────── 指標計算 ───────────────────────────
def load_config():
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


def unique_codes(cfg):
    return sorted({m["code"] for s in cfg["sectors"] for m in s["members"] if m.get("code")})


def build_stock_daily(conn):
    """每檔每日：外資/投信/自營 各自的淨買賣超（股數）、金額（億）、張數，與成交值（億）。"""
    inst = pd.read_sql("SELECT * FROM inst_raw", conn)
    price = pd.read_sql("SELECT * FROM price_raw", conn)
    if inst.empty or price.empty:
        return pd.DataFrame()

    inst["net"] = inst["buy"].fillna(0) - inst["sell"].fillna(0)
    def gnet(names):
        return inst[inst["name"].isin(names)].groupby(["stock_id", "date"])["net"].sum()
    net = pd.concat([gnet(FOREIGN).rename("f_sh"),
                     gnet(TRUST).rename("t_sh"),
                     gnet(DEALER).rename("d_sh")], axis=1).reset_index().fillna(0)

    price = price.copy()
    vol = price["trading_volume"].replace(0, pd.NA)
    price["avg_price"] = (price["trading_money"] / vol).fillna(price["close"])
    price["turnover_100m"] = price["trading_money"].fillna(0) / 1e8

    df = net.merge(price[["stock_id", "date", "avg_price", "turnover_100m"]],
                   on=["stock_id", "date"], how="inner")
    for g in ["f", "t", "d"]:
        df[g + "_val"] = df[g + "_sh"] * df["avg_price"] / 1e8
        df[g + "_lots"] = df[g + "_sh"] / 1000.0
    return df


def compute_metrics(conn, cfg):
    """輸出每板塊、每個成分股的『每日 × 外資/投信/自營』金額(億)與張數，
    以及板塊每日各法人金額與成交值。近1/5/10/20日由前端加總。

    ★ v1.5.1 改：新增「有股價但缺法人資料」偵測邏輯。
    """
    daily = build_stock_daily(conn)
    if daily.empty:
        logger.error("快取無足夠資料，無法計算指標")
        return None

    # ★ 偵測「有活動股價 (turnover > 0) 但缺法人資料」的 (stock, date) 缺洞
    price_df = pd.read_sql("SELECT stock_id, date, trading_money FROM price_raw", conn)
    price_active = {(r.stock_id, r.date) for r in price_df.itertuples(index=False)
                    if r.trading_money and r.trading_money > 0}
    inst_covered = set(zip(daily["stock_id"], daily["date"]))
    missing_inst = price_active - inst_covered  # 缺洞：有股價 turnover 但無 inst

    out_days = int(cfg["config"].get("output_days", 30))
    # axis 用 price（更完整）而非 daily（inner join 後）決定，避免 as_of 掉一天
    all_dates_set = set(daily["date"].tolist()) | set(price_df["date"].tolist())
    all_dates = sorted(all_dates_set)
    axis = all_dates[-out_days:]
    as_of = axis[-1]

    code_name = {m["code"]: m["name"] for s in cfg["sectors"] for m in s["members"]}

    rec = {}
    for r in daily.itertuples(index=False):
        rec[(r.stock_id, r.date)] = r

    # 供板塊成交值補計用（缺洞股票 turnover 用 price 補）
    price_turnover = {}
    for r in price_df.itertuples(index=False):
        price_turnover[(r.stock_id, r.date)] = float(r.trading_money or 0) / 1e8

    have = set(daily["stock_id"].unique())
    sectors_out = []
    sector_missing_count = 0

    for s in cfg["sectors"]:
        codes = [m["code"] for m in s["members"] if m.get("code") and m["code"] in have]
        if not codes:
            continue
        sec = {dtt: {"f": 0.0, "t": 0.0, "dl": 0.0, "to": 0.0, "missing": 0} for dtt in axis}
        members = []
        for c in codes:
            hist = []
            for dtt in axis:
                r = rec.get((c, dtt))
                if r is None:
                    # 該股該日無 inst 資料
                    if (c, dtt) in missing_inst:
                        # ★ 缺洞：不寫入個股 hist（避免假的 0）
                        # 板塊 to 用 price turnover 補計，並標記 missing
                        sector_missing_count += 1
                        sec[dtt]["to"] += price_turnover.get((c, dtt), 0.0)
                        sec[dtt]["missing"] += 1
                    # 若非缺洞（該股當日無 price 資料，例如停牌/未上市）→ 同樣 skip
                    continue

                fv, tv, dv = float(r.f_val), float(r.t_val), float(r.d_val)
                fl, tl, dl = float(r.f_lots), float(r.t_lots), float(r.d_lots)
                to = float(r.turnover_100m)
                hist.append({"d": dtt,
                             "fv": round(fv, 4), "fl": round(fl),
                             "tv": round(tv, 4), "tl": round(tl),
                             "dv": round(dv, 4), "dl": round(dl)})
                sec[dtt]["f"] += fv
                sec[dtt]["t"] += tv
                sec[dtt]["dl"] += dv
                sec[dtt]["to"] += to
            members.append({"name": code_name.get(c, c), "code": c, "hist": hist})

        sec_hist = []
        for dtt in axis:
            item = {"d": dtt,
                    "f": round(sec[dtt]["f"], 4),
                    "t": round(sec[dtt]["t"], 4),
                    "dl": round(sec[dtt]["dl"], 4),
                    "to": round(sec[dtt]["to"], 3)}
            if sec[dtt]["missing"] > 0:
                item["missing"] = sec[dtt]["missing"]  # ★ 標記該日有幾支缺資料
            sec_hist.append(item)
        sectors_out.append({"name": s["name"], "members": members, "history": sec_hist})

    # ★ log 缺洞明細（近 7 天）
    if missing_inst:
        cutoff = (dt.date.today() - dt.timedelta(days=7)).isoformat()
        recent = sorted([k for k in missing_inst if k[1] >= cutoff], key=lambda x: (x[1], x[0]))
        logger.warning(f"⚠ 全部有 {len(missing_inst)} 個 (股票×日期) 缺法人資料（sector-level 累計影響 {sector_missing_count} 次）")
        if recent:
            logger.warning(f"⚠ 其中近 7 天有 {len(recent)} 個，明細（前 15 筆）：")
            for c, d in recent[:15]:
                logger.warning(f"    - {c} @ {d}")
            logger.warning(f"⚠ 下次跑管線會自動嘗試重抓（FORCE_REFETCH_DAYS={FORCE_REFETCH_DAYS}）")

    cfg["config"]["output_days"] = out_days
    out = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "as_of_date": as_of,
        "phase": 1.5,
        "schema": 2,
        "note": ("第一階段+：每日×外資/投信/自營 的金額(億)與張數，近1/5/10/20日由前端加總。"
                 "v1.5.1 起：缺法人資料的日子從個股 hist 移除（避免假 0）；"
                 "板塊 sec_hist 保留該日並標記 missing。集中度/家數差待第二階段分點資料。"),
        "config": cfg["config"],
        "dates": axis,
        "missing_stock_dates": len(missing_inst),  # ★ 新欄位：總缺洞數
        "sectors": sectors_out,
    }
    return out


# ─────────────────────────── 主流程 ───────────────────────────
def main():
    t0 = time.time()
    logger.info("=" * 56)
    logger.info("管線啟動（第一階段：法人資金）v1.5.1 缺洞感知版")

    if not TOKEN:
        logger.error("找不到 FINMIND_TOKEN（請設定環境變數或 .env）。中止。")
        return 1
    if not CONFIG_PATH.exists():
        logger.error(f"找不到設定檔：{CONFIG_PATH}。中止。")
        return 1

    try:
        cfg = load_config()
        codes = unique_codes(cfg)
        logger.info(f"設定檔：{len(cfg['sectors'])} 板塊，唯一母體 {len(codes)} 檔")
        logger.info(f"參數：BACKFILL={BACKFILL_DAYS} OVERLAP={OVERLAP_DAYS} FORCE_REFETCH={FORCE_REFETCH_DAYS}")

        check_quota()
        today = dt.date.today()
        conn = get_conn()

        logger.info("開始抓取 / 增量更新 ...")
        update_universe(conn, codes, today)

        logger.info("計算板塊指標 ...")
        out = compute_metrics(conn, cfg)
        if out is None:
            logger.error("指標計算失敗。中止。")
            return 1

        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        logger.info(f"已輸出：{OUTPUT_JSON}（資料日 {out['as_of_date']}，{len(out['sectors'])} 板塊，缺洞 {out['missing_stock_dates']}）")
        logger.info(f"完成，耗時 {time.time() - t0:.1f}s ✓")
        return 0

    except QuotaExceeded as e:
        logger.error(f"FinMind 額度用盡：{e}。已保存部分快取，稍後再跑即可續抓。")
        return 1
    except Exception as e:
        logger.exception(f"管線發生未預期錯誤：{e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
