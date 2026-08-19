#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
板塊泡泡系統 — 第一階段 FinMind 資料管線（法人資金主線）
v1.6.0（個股詳情資料集成版）

本次升級（vs v1.5.1）：
  ① Schema 版本 2 → 3。個股 hist 每筆新增 2 個欄位：
     - "cp"：當日收盤價（元）
     - "vl"：當日成交量（張）
     兩者用於前端個股 Modal 的股價走勢圖與成交量 bar。
  ② 前端未升級也不會壞（讀不到 cp/vl 就自動不畫股價圖）。

沿用 v1.5.1 的：
  · FORCE_REFETCH_DAYS 強制回抓機制
  · compute_metrics 缺洞偵測邏輯
  · 個股 hist 缺洞日子照樣 skip 避免假 0

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
FORCE_REFETCH_DAYS = int(os.environ.get("FORCE_REFETCH_DAYS", "15"))
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
    pass


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
    force_start = today - dt.timedelta(days=FORCE_REFETCH_DAYS)
    last = last_cached_date(conn, table, code)
    if last is None:
        return (today - dt.timedelta(days=BACKFILL_DAYS)).isoformat()
    d = dt.date.fromisoformat(last) - dt.timedelta(days=OVERLAP_DAYS)
    return min(d, force_start).isoformat()


def update_universe(conn, codes, today):
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
    """每檔每日：外資/投信/自營 各自的淨買賣超（股數）、金額（億）、張數，
    以及成交值（億）、【v1.6.0 新增】收盤價 close、【v1.6.0 新增】成交量張數 vol_lots。"""
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
    # ★ v1.6.0 新增：成交量（張）
    price["vol_lots"] = price["trading_volume"].fillna(0) / 1000.0

    df = net.merge(
        price[["stock_id", "date", "avg_price", "turnover_100m", "close", "vol_lots"]],
        on=["stock_id", "date"], how="inner")
    for g in ["f", "t", "d"]:
        df[g + "_val"] = df[g + "_sh"] * df["avg_price"] / 1e8
        df[g + "_lots"] = df[g + "_sh"] / 1000.0
    return df


def compute_metrics(conn, cfg):
    daily = build_stock_daily(conn)
    if daily.empty:
        logger.error("快取無足夠資料，無法計算指標")
        return None

    # 缺洞偵測（v1.5.1）
    price_df = pd.read_sql("SELECT stock_id, date, trading_money FROM price_raw", conn)
    price_active = {(r.stock_id, r.date) for r in price_df.itertuples(index=False)
                    if r.trading_money and r.trading_money > 0}
    inst_covered = set(zip(daily["stock_id"], daily["date"]))
    missing_inst = price_active - inst_covered

    out_days = int(cfg["config"].get("output_days", 30))
    all_dates_set = set(daily["date"].tolist()) | set(price_df["date"].tolist())
    all_dates = sorted(all_dates_set)
    axis = all_dates[-out_days:]
    as_of = axis[-1]

    code_name = {m["code"]: m["name"] for s in cfg["sectors"] for m in s["members"]}

    rec = {}
    for r in daily.itertuples(index=False):
        rec[(r.stock_id, r.date)] = r

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
                    if (c, dtt) in missing_inst:
                        sector_missing_count += 1
                        sec[dtt]["to"] += price_turnover.get((c, dtt), 0.0)
                        sec[dtt]["missing"] += 1
                    continue

                fv, tv, dv = float(r.f_val), float(r.t_val), float(r.d_val)
                fl, tl, dl = float(r.f_lots), float(r.t_lots), float(r.d_lots)
                to = float(r.turnover_100m)
                # ★ v1.6.0：新增 cp、vl
                cp = float(r.close) if r.close is not None else None
                vl = float(r.vol_lots) if r.vol_lots is not None else None
                item = {"d": dtt,
                        "fv": round(fv, 4), "fl": round(fl),
                        "tv": round(tv, 4), "tl": round(tl),
                        "dv": round(dv, 4), "dl": round(dl)}
                if cp is not None:
                    item["cp"] = round(cp, 2)
                if vl is not None:
                    item["vl"] = round(vl)
                hist.append(item)
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
                item["missing"] = sec[dtt]["missing"]
            sec_hist.append(item)
        sectors_out.append({"name": s["name"], "members": members, "history": sec_hist})

    if missing_inst:
        cutoff = (dt.date.today() - dt.timedelta(days=7)).isoformat()
        recent = sorted([k for k in missing_inst if k[1] >= cutoff], key=lambda x: (x[1], x[0]))
        logger.warning(f"⚠ 全部有 {len(missing_inst)} 個 (股票×日期) 缺法人資料（sector 累計影響 {sector_missing_count} 次）")
        if recent:
            logger.warning(f"⚠ 其中近 7 天有 {len(recent)} 個，明細（前 15 筆）：")
            for c, d in recent[:15]:
                logger.warning(f"    - {c} @ {d}")
            logger.warning(f"⚠ 下次跑會自動嘗試重抓（FORCE_REFETCH_DAYS={FORCE_REFETCH_DAYS}）")

    cfg["config"]["output_days"] = out_days
    out = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "as_of_date": as_of,
        "phase": 1.6,
        "schema": 3,  # ★ 版本 bump：hist 多了 cp / vl 欄位
        "note": ("第一階段+ v1.6：個股 hist 新增 cp（收盤價）、vl（成交量張）。"
                 "前端偵測有無 cp/vl 決定要不要畫股價圖。缺洞日子照樣 skip 避免假 0。"),
        "config": cfg["config"],
        "dates": axis,
        "missing_stock_dates": len(missing_inst),
        "sectors": sectors_out,
    }
    return out


# ─────────────────────────── 主流程 ───────────────────────────
def main():
    t0 = time.time()
    logger.info("=" * 56)
    logger.info("管線啟動（第一階段：法人資金）v1.6.0 個股詳情資料集成版")

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
