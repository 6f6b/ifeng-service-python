"""拉取 stock_basic 并批量写入本地 MySQL。"""
import argparse
import logging
import sys
import time

import pandas as pd

from db_config import DB_CONFIG
from db_util import batch_upsert_df, get_connection
from tushare_config import get_pro_api

"""
用法：
  python stock_basic_update.py
  python stock_basic_update.py --excel   # 额外导出 Excel

本地库：
  docker compose -f docker-compose.local.yml up -d
"""

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
BASIC_UPDATE_COLUMNS = [
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

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(console_handler)


def fetch_stock_basic() -> pd.DataFrame:
    pro = get_pro_api()
    df_list = []
    for status in ["L", "D", "P"]:
        t0 = time.perf_counter()
        df_temp = pro.stock_basic(
            exchange="",
            list_status=status,
            fields=",".join(BASIC_COLUMNS),
        )
        logger.info(
            f"[耗时] stock_basic API status={status}: {time.perf_counter() - t0:.2f}s，{len(df_temp)} 条"
        )
        df_list.append(df_temp)
    return pd.concat(df_list, ignore_index=True)


def update_stock_basic(export_excel: bool = False) -> int:
    t_total = time.perf_counter()
    df = fetch_stock_basic()
    logger.info(f"共获取 {len(df)} 条基础信息")

    if export_excel:
        df.to_excel("stock_basic_info.xlsx", index=False)
        logger.info("已导出 stock_basic_info.xlsx")

    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        logger.info(
            f"写入 DB={DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )

        t_db = time.perf_counter()
        rows = batch_upsert_df(
            cursor,
            "stock_basic",
            df,
            BASIC_COLUMNS,
            update_columns=BASIC_UPDATE_COLUMNS,
        )
        db_sec = time.perf_counter() - t_db
        conn.commit()
        logger.info(f"[耗时] 批量入库 stock_basic: {db_sec:.2f}s，{rows} 条")
        logger.info(f"完成，总计 {time.perf_counter() - t_total:.2f}s")
        return rows
    except Exception as e:
        logger.error(f"更新 stock_basic 失败: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="股票基础信息更新（本地批量入库）")
    parser.add_argument("--excel", action="store_true", help="同时导出 Excel")
    args = parser.parse_args()
    update_stock_basic(export_excel=args.excel)
