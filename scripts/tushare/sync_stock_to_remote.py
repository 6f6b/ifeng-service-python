"""将本地 stock 库同步到线上 RDS（按交易日批量 upsert）。"""
import argparse
import logging
import sys
import time

import pandas as pd
import pymysql

from db_config import DB_CONFIG as LOCAL_CFG
from db_util import batch_upsert_df

REMOTE_CFG = {
    "host": "rds.6f6b.cn",
    "port": 3306,
    "user": "root",
    "password": "FuckTheHaker@666",
    "database": "stock",
    "charset": "utf8mb4",
}

DAILY_COLUMNS = [
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change",
    "pct_chg",
    "vol",
    "amount",
]
DAILY_UPDATE_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change",
    "pct_chg",
    "vol",
    "amount",
]

BASIC_COLUMNS = [
    "ts_code",
    "symbol",
    "name",
    "area",
    "industry",
    "market",
    "exchange",
    "curr_type",
    "list_status",
    "list_date",
    "delist_date",
    "is_hs",
]
BASIC_UPDATE_COLUMNS = BASIC_COLUMNS[1:]

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)


def to_remote_date(ymd: str) -> str:
    return f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}"


def get_local_trade_dates() -> list[str]:
    conn = pymysql.connect(**LOCAL_CFG)
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT trade_date FROM stock_daily ORDER BY trade_date")
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def sync_stock_basic():
    t0 = time.perf_counter()
    local = pymysql.connect(**LOCAL_CFG)
    remote = pymysql.connect(**REMOTE_CFG)
    try:
        df = pd.read_sql("SELECT * FROM stock_basic", local)
        cur = remote.cursor()
        rows = batch_upsert_df(
            cur,
            "stock_basic",
            df,
            BASIC_COLUMNS,
            update_columns=BASIC_UPDATE_COLUMNS,
        )
        remote.commit()
        logger.info(f"stock_basic 同步完成: {rows} 条，{time.perf_counter() - t0:.2f}s")
    finally:
        local.close()
        remote.close()


def sync_stock_daily(start_date: str | None = None, end_date: str | None = None):
    dates = get_local_trade_dates()
    if start_date:
        dates = [d for d in dates if d >= start_date]
    if end_date:
        dates = [d for d in dates if d <= end_date]

    total = len(dates)
    logger.info(f"开始同步 stock_daily，共 {total} 个交易日")

    local = pymysql.connect(**LOCAL_CFG)
    remote = pymysql.connect(**REMOTE_CFG)
    t_all = time.perf_counter()
    synced_rows = 0

    try:
        rcur = remote.cursor()
        for i, ymd in enumerate(dates, 1):
            t0 = time.perf_counter()
            df = pd.read_sql(
                "SELECT * FROM stock_daily WHERE trade_date = %s",
                local,
                params=(ymd,),
            )
            if df.empty:
                logger.info(f"[{i}/{total}] {ymd} 本地无数据，跳过")
                continue

            df["trade_date"] = df["trade_date"].map(to_remote_date)
            rows = batch_upsert_df(
                rcur,
                "stock_daily",
                df,
                DAILY_COLUMNS,
                update_columns=DAILY_UPDATE_COLUMNS,
            )
            remote.commit()
            synced_rows += rows
            logger.info(
                f"[{i}/{total}] {ymd} -> {to_remote_date(ymd)}: {rows} 条, "
                f"{time.perf_counter() - t0:.2f}s, 累计 {synced_rows}"
            )
    finally:
        local.close()
        remote.close()

    logger.info(
        f"stock_daily 同步完成: {synced_rows} 条, {total} 天, "
        f"总计 {time.perf_counter() - t_all:.2f}s"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="本地 stock 库同步到线上 RDS")
    parser.add_argument("--tables", default="daily,basic", help="daily / basic / daily,basic")
    parser.add_argument("--start", help="起始交易日 YYYYMMDD")
    parser.add_argument("--end", help="结束交易日 YYYYMMDD")
    args = parser.parse_args()

    tables = {t.strip() for t in args.tables.split(",")}
    if "basic" in tables:
        sync_stock_basic()
    if "daily" in tables:
        sync_stock_daily(args.start, args.end)
