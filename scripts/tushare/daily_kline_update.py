"""daily_kline 历史回填（一次性脚本；日常更新由 stk Java 服务负责）

用法：
  python instrument_meta_update.py
  python daily_kline_update.py --reset --start 20240716
  python daily_kline_update.py --resume --start 20240716   # 断点续传
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd

from db_config import DB_CONFIG
from db_util import get_connection
from kline_store import (
    load_etf_ts_codes,
    tushare_daily_to_kline_rows,
    upsert_kline_rows,
)
from tushare_config import get_pro_api

ASSET_APIS = {
    "stock": "daily",
    "index": "index_daily",
    "cb": "cb_daily",
    "etf": "fund_daily",
}

# 判断某日是否已同步的最小条数
MIN_ROWS = {
    "stock": 4000,
    "index": 50,
    "etf": 100,
    "cb": 50,
}

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)

pro = get_pro_api()


def today_ymd() -> str:
    return datetime.now().strftime("%Y%m%d")


def ymd_to_iso(ymd: str) -> str:
    return f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}"


def fetch_trade_dates(start_date: str, end_date: str) -> list[str]:
    cal = pro.trade_cal(
        exchange="SSE",
        start_date=start_date,
        end_date=end_date,
        is_open="1",
        fields="cal_date,is_open",
    )
    if cal.empty:
        return []
    return cal["cal_date"].tolist()


def fetch_market_daily(asset_type: str, trade_date: str) -> pd.DataFrame:
    api_name = ASSET_APIS[asset_type]
    t0 = time.perf_counter()
    fetcher = getattr(pro, api_name)
    df = fetcher(trade_date=trade_date)
    logger.info(
        "%s %s API %.2fs, %d 条",
        asset_type,
        trade_date,
        time.perf_counter() - t0,
        len(df),
    )
    return df


def fetch_type_rows(asset_type: str, trade_date: str, etf_codes: set[str] | None) -> tuple[str, list[tuple]]:
    df = fetch_market_daily(asset_type, trade_date)
    ts_filter = etf_codes if asset_type == "etf" else None
    rows = tushare_daily_to_kline_rows(asset_type, df, ts_filter)
    return asset_type, rows


def date_already_synced(cursor, trade_date: str, types: list[str]) -> bool:
    iso = ymd_to_iso(trade_date)
    for asset_type in types:
        min_rows = MIN_ROWS.get(asset_type, 1)
        cursor.execute(
            "SELECT COUNT(*) FROM daily_kline WHERE asset_type=%s AND trade_date=%s",
            (asset_type, iso),
        )
        if cursor.fetchone()[0] < min_rows:
            return False
    return True


def update_daily_by_date(trade_date: str, types: list[str], workers: int = 4) -> int:
    conn = get_connection()
    total = 0
    try:
        cursor = conn.cursor()
        etf_codes = load_etf_ts_codes(cursor) if "etf" in types else set()
        if "etf" in types and not etf_codes:
            logger.warning("instrument 中无 ETF 元数据，请先运行 instrument_meta_update.py")

        fetched: dict[str, list[tuple]] = {}
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [
                pool.submit(fetch_type_rows, asset_type, trade_date, etf_codes)
                for asset_type in types
            ]
            for fut in as_completed(futures):
                asset_type, rows = fut.result()
                fetched[asset_type] = rows

        for asset_type in types:
            rows = fetched.get(asset_type, [])
            if not rows:
                continue
            try:
                n = upsert_kline_rows(cursor, rows)
                conn.commit()
                total += n
                logger.info("%s %s 写入 %d 条", asset_type, trade_date, n)
            except Exception as e:
                conn.rollback()
                logger.error("%s %s 写入失败: %s", asset_type, trade_date, e)
    finally:
        conn.close()
    return total


def reset_klines() -> None:
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE daily_kline")
        conn.commit()
        logger.info("已清空 daily_kline")
    finally:
        conn.close()


def backfill(
    start_date: str,
    end_date: str | None,
    types: list[str],
    resume: bool = False,
    workers: int = 4,
) -> None:
    if end_date is None:
        end_date = today_ymd()
    dates = fetch_trade_dates(start_date, end_date)
    if resume:
        conn = get_connection()
        try:
            cursor = conn.cursor()
            dates = [d for d in dates if not date_already_synced(cursor, d, types)]
        finally:
            conn.close()
        logger.info("断点续传，跳过已同步日期，剩余 %d 个交易日", len(dates))

    logger.info(
        "历史回填 %s ~ %s, %d 个交易日, types=%s, workers=%d, DB=%s/%s",
        start_date,
        end_date,
        len(dates),
        ",".join(types),
        workers,
        DB_CONFIG["host"],
        DB_CONFIG["database"],
    )
    if not dates:
        logger.info("无需回填")
        return

    t_all = time.perf_counter()
    ok = 0
    for i, d in enumerate(dates, 1):
        t_day = time.perf_counter()
        logger.info("===== [%d/%d] %s =====", i, len(dates), d)
        n = update_daily_by_date(d, types, workers=workers)
        if n > 0:
            ok += 1
        elapsed = time.perf_counter() - t_day
        avg = (time.perf_counter() - t_all) / i
        eta_min = avg * (len(dates) - i) / 60
        logger.info("本日耗时 %.1fs，预计剩余 %.0f 分钟", elapsed, eta_min)

    logger.info(
        "回填完成，有数据交易日 %d/%d，总耗时 %.1f 分钟",
        ok,
        len(dates),
        (time.perf_counter() - t_all) / 60,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="daily_kline 历史回填")
    parser.add_argument("--reset", action="store_true", help="回填前清空 daily_kline")
    parser.add_argument("--resume", action="store_true", help="跳过已同步日期")
    parser.add_argument("--start", default="20240716", help="回填开始 YYYYMMDD，默认近2年")
    parser.add_argument("--end", help="回填结束 YYYYMMDD")
    parser.add_argument("--workers", type=int, default=4, help="并行拉取类型数")
    parser.add_argument(
        "--types",
        default="stock,index,etf,cb",
        help="stock,index,etf,cb",
    )
    args = parser.parse_args()
    types = [t.strip() for t in args.types.split(",") if t.strip()]

    if args.reset:
        reset_klines()
    backfill(args.start, args.end, types, resume=args.resume, workers=args.workers)
