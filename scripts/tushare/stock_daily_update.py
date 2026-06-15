import argparse
import logging
import sys
import time
from datetime import datetime

import pandas as pd
import pymysql
from apscheduler.schedulers.blocking import BlockingScheduler

from db_config import DB_CONFIG
from db_util import batch_upsert_df, get_connection
from tushare_config import get_pro_api

"""
股票每日数据更新工具（默认写本地 MySQL，批量入库）

1. 定时任务：
   python stock_daily_update.py
   python stock_daily_update.py --mode schedule

2. 更新单个日期：
   python stock_daily_update.py --mode single --date 20240320

3. 更新日期范围：
   python stock_daily_update.py --mode range --start 20240301 --end 20240320

本地库启动：
   docker compose -f docker-compose.local.yml up -d

同步到线上时覆盖环境变量：
   DB_HOST=rds.6f6b.cn DB_PORT=3306 DB_PASSWORD=xxx python stock_daily_update.py --mode range --start 20260101
"""

pro = get_pro_api()

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

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(console_handler)


def get_today_date():
    return datetime.now().strftime("%Y%m%d")


def fetch_daily_df(trade_date: str) -> pd.DataFrame:
    t_api = time.perf_counter()
    df = pro.daily(
        trade_date=trade_date,
        fields=",".join(DAILY_COLUMNS),
    )
    api_sec = time.perf_counter() - t_api
    logger.info(f"[耗时] Tushare API: {api_sec:.2f}s，返回 {len(df)} 条")
    return df, api_sec


def update_daily_data_by_date(trade_date: str) -> bool:
    conn = None
    cursor = None
    t_total = time.perf_counter()

    try:
        t_conn = time.perf_counter()
        conn = get_connection()
        cursor = conn.cursor()
        conn_sec = time.perf_counter() - t_conn
        logger.info(
            f"开始更新 {trade_date}，DB={DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        logger.info(f"[耗时] 数据库连接: {conn_sec:.2f}s")

        df, api_sec = fetch_daily_df(trade_date)
        if df.empty:
            logger.info(f"{trade_date} 没有交易数据")
            return False

        t_db = time.perf_counter()
        rows = batch_upsert_df(
            cursor,
            "stock_daily",
            df,
            DAILY_COLUMNS,
            update_columns=DAILY_UPDATE_COLUMNS,
        )
        db_sec = time.perf_counter() - t_db
        per_row_ms = (db_sec / rows * 1000) if rows else 0
        logger.info(
            f"[耗时] 批量入库: {db_sec:.2f}s，{rows} 条，均 {per_row_ms:.2f}ms/条"
        )

        t_commit = time.perf_counter()
        conn.commit()
        commit_sec = time.perf_counter() - t_commit
        logger.info(f"[耗时] commit: {commit_sec:.2f}s")
        logger.info(
            f"{trade_date} 更新完成，总计 {time.perf_counter() - t_total:.2f}s "
            f"(API {api_sec:.2f}s + 入库 {db_sec:.2f}s + commit {commit_sec:.2f}s)"
        )
        return True

    except Exception as e:
        logger.error(f"更新 {trade_date} 出错: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def update_daily_data_range(start_date: str, end_date: str | None = None):
    if end_date is None:
        end_date = get_today_date()

    logger.info(f"开始更新从 {start_date} 到 {end_date} 的股票数据...")
    trade_cal = pro.trade_cal(start_date=start_date, end_date=end_date, is_open="1")
    trade_dates = trade_cal["cal_date"].tolist()

    success_count = 0
    total_dates = len(trade_dates)
    for i, date in enumerate(trade_dates, 1):
        logger.info(f"正在处理 {date} ({i}/{total_dates})")
        if update_daily_data_by_date(date):
            success_count += 1
        time.sleep(0.3)

    logger.info(f"数据更新完成！成功更新 {success_count}/{total_dates} 个交易日的数据。")


def run_scheduled_update():
    scheduler = BlockingScheduler()
    scheduler.add_job(
        lambda: update_daily_data_by_date(get_today_date()),
        trigger="cron",
        day_of_week="mon-fri",
        hour="17",
        minute="0-50/10",
        timezone="Asia/Shanghai",
    )
    logger.info("定时任务已启动...")
    scheduler.start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="股票日线数据更新（本地批量入库）")
    parser.add_argument(
        "--mode",
        choices=["schedule", "single", "range"],
        default="schedule",
        help="schedule / single / range",
    )
    parser.add_argument("--date", help="指定日期 YYYYMMDD")
    parser.add_argument("--start", help="开始日期 YYYYMMDD")
    parser.add_argument("--end", help="结束日期 YYYYMMDD")
    args = parser.parse_args()

    if args.mode == "schedule":
        run_scheduled_update()
    elif args.mode == "single":
        if not args.date:
            logger.error("需要指定 --date 参数")
            sys.exit(1)
        update_daily_data_by_date(args.date)
    elif args.mode == "range":
        if not args.start:
            logger.error("需要指定 --start 参数")
            sys.exit(1)
        update_daily_data_range(args.start, args.end)
