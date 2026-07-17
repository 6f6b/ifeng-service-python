"""同步四类资产元数据到 ifeng_research.instrument 表"""
from __future__ import annotations

import argparse
import logging
import sys
import time

import pandas as pd

from db_config import DB_CONFIG
from db_util import batch_upsert_df, get_connection
from kline_store import ts_code_to_inst_id
from tushare_config import get_pro_api

INSTRUMENT_COLUMNS = [
    "asset_type",
    "inst_id",
    "name",
    "exchange",
    "ts_code",
    "area",
    "industry",
    "market",
    "curr_type",
    "list_status",
    "list_date",
    "delist_date",
    "is_hs",
]
INSTRUMENT_UPDATE_COLUMNS = [
    "name",
    "exchange",
    "ts_code",
    "area",
    "industry",
    "market",
    "curr_type",
    "list_status",
    "list_date",
    "delist_date",
    "is_hs",
]

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)


def _exchange_from_ts_code(ts_code: str) -> str:
    if ts_code.endswith(".SH"):
        return "SSE"
    if ts_code.endswith(".BJ"):
        return "BSE"
    return "SZSE"


def _finalize_df(df: pd.DataFrame) -> pd.DataFrame:
    for col in INSTRUMENT_COLUMNS:
        if col not in df.columns:
            df[col] = None
    out = df[INSTRUMENT_COLUMNS].copy()
    out = out.dropna(subset=["asset_type", "inst_id"])
    out["inst_id"] = out["inst_id"].astype(str).str.strip()
    out["name"] = out["name"].fillna("").astype(str).str.strip()
    out = out[out["inst_id"] != ""]
    return out


def fetch_stock_df(pro) -> pd.DataFrame:
    parts = []
    for status in ["L", "D", "P"]:
        t0 = time.perf_counter()
        df = pro.stock_basic(
            exchange="",
            list_status=status,
            fields="ts_code,symbol,name,area,industry,market,exchange,curr_type,list_status,list_date,delist_date,is_hs",
        )
        logger.info(
            "stock_basic status=%s: %.2fs, %d 条", status, time.perf_counter() - t0, len(df)
        )
        parts.append(df)
    raw = pd.concat(parts, ignore_index=True)
    return _finalize_df(pd.DataFrame({
        "asset_type": "stock",
        "inst_id": raw["symbol"].astype(str),
        "name": raw["name"],
        "exchange": raw["exchange"],
        "ts_code": raw["ts_code"],
        "area": raw["area"],
        "industry": raw["industry"],
        "market": raw["market"],
        "curr_type": raw["curr_type"],
        "list_status": raw["list_status"],
        "list_date": raw["list_date"],
        "delist_date": raw["delist_date"],
        "is_hs": raw["is_hs"],
    }))


def fetch_index_df(pro) -> pd.DataFrame:
    t0 = time.perf_counter()
    raw = pro.index_basic(
        market="",
        fields="ts_code,name,market,publisher,category,list_date",
    )
    logger.info("index_basic: %.2fs, %d 条", time.perf_counter() - t0, len(raw))
    return _finalize_df(pd.DataFrame({
        "asset_type": "index",
        "inst_id": raw["ts_code"].map(ts_code_to_inst_id),
        "name": raw["name"],
        "exchange": raw["ts_code"].map(_exchange_from_ts_code),
        "ts_code": raw["ts_code"],
        "market": raw["market"],
        "industry": raw["category"],
        "list_date": raw["list_date"],
    }))


def fetch_etf_df(pro) -> pd.DataFrame:
    t0 = time.perf_counter()
    raw = pro.fund_basic(
        market="E",
        status="L",
        fields="ts_code,name,market,fund_type,list_date,delist_date",
    )
    logger.info("fund_basic(E): %.2fs, %d 条", time.perf_counter() - t0, len(raw))
    if raw.empty:
        return pd.DataFrame(columns=INSTRUMENT_COLUMNS)
    mask = raw["name"].astype(str).str.contains("ETF", case=False, na=False) | (
        raw["fund_type"].astype(str).str.contains("ETF", case=False, na=False)
    )
    raw = raw[mask].copy()
    return _finalize_df(pd.DataFrame({
        "asset_type": "etf",
        "inst_id": raw["ts_code"].map(ts_code_to_inst_id),
        "name": raw["name"],
        "exchange": raw["ts_code"].map(_exchange_from_ts_code),
        "ts_code": raw["ts_code"],
        "market": raw["market"],
        "list_status": "L",
        "list_date": raw["list_date"],
        "delist_date": raw["delist_date"],
    }))


def fetch_cb_df(pro) -> pd.DataFrame:
    t0 = time.perf_counter()
    raw = pro.cb_basic(
        fields="ts_code,bond_short_name,stk_code,stk_short_name,list_date,delist_date",
    )
    logger.info("cb_basic: %.2fs, %d 条", time.perf_counter() - t0, len(raw))
    return _finalize_df(pd.DataFrame({
        "asset_type": "cb",
        "inst_id": raw["ts_code"].map(ts_code_to_inst_id),
        "name": raw["bond_short_name"],
        "exchange": raw["ts_code"].map(_exchange_from_ts_code),
        "ts_code": raw["ts_code"],
        "list_date": raw["list_date"],
        "delist_date": raw["delist_date"],
        "list_status": "L",
    }))


FETCHERS = {
    "stock": fetch_stock_df,
    "index": fetch_index_df,
    "etf": fetch_etf_df,
    "cb": fetch_cb_df,
}


def upsert_df(cursor, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    return batch_upsert_df(
        cursor,
        "instrument",
        df,
        INSTRUMENT_COLUMNS,
        update_columns=INSTRUMENT_UPDATE_COLUMNS,
    )


def update_all(types: list[str] | None = None) -> int:
    pro = get_pro_api()
    selected = types or list(FETCHERS.keys())
    total = 0
    conn = get_connection()
    try:
        cursor = conn.cursor()
        logger.info(
            "写入 DB=%s:%s/%s, types=%s",
            DB_CONFIG["host"],
            DB_CONFIG["port"],
            DB_CONFIG["database"],
            ",".join(selected),
        )
        for asset_type in selected:
            fetcher = FETCHERS.get(asset_type)
            if fetcher is None:
                logger.warning("未知类型: %s", asset_type)
                continue
            df = fetcher(pro)
            rows = upsert_df(cursor, df)
            conn.commit()
            total += rows
            logger.info("%s 元数据入库 %d 条", asset_type, rows)
            time.sleep(0.3)
    finally:
        conn.close()
    return total


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="同步 instrument 元数据")
    parser.add_argument(
        "--types",
        default="stock,index,etf,cb",
        help="逗号分隔: stock,index,etf,cb",
    )
    args = parser.parse_args()
    types = [t.strip() for t in args.types.split(",") if t.strip()]
    count = update_all(types)
    logger.info("完成，共写入 %d 条", count)
