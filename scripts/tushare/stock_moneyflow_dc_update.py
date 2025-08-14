"""
东方财富个股资金流向数据更新脚本

功能：
    从Tushare获取东方财富个股资金流向数据并保存到MySQL数据库中。
    支持增量更新和指定日期范围更新。

使用方法：
    1. 增量更新（从最后更新日期到今天）：
        python stock_moneyflow_dc_update.py

    2. 更新指定开始日期到今天的数据：
        python stock_moneyflow_dc_update.py --start_date 2024-01-01

    3. 更新指定日期范围的数据：
        python stock_moneyflow_dc_update.py --start_date 2024-01-01 --end_date 2024-01-31

    4. 强制更新指定日期范围的数据（会先删除该范围的旧数据）：
        python stock_moneyflow_dc_update.py --start_date 2024-01-01 --end_date 2024-01-31 --force

参数说明：
    --start_date: 开始日期，支持YYYY-MM-DD或YYYYMMDD格式
    --end_date: 结束日期，支持YYYY-MM-DD或YYYYMMDD格式
    --force: 强制更新标志，会先删除指定日期范围的数据再重新获取

数据说明：
    小单：小于等于5万元
    中单：5万元至20万元
    大单：20万元至100万元
    特大单：大于等于100万元

依赖安装：
    pip install tushare pandas pymysql sqlalchemy python-dateutil
"""

import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
import pymysql
from sqlalchemy import create_engine
import time
import logging
import argparse
from dateutil.parser import parse
import os
import sys
from urllib.parse import quote_plus

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# 数据库配置
DB_CONFIG = {
    'host': 'rds.6f6b.cn',
    'port': 3306,
    'user': 'root',
    'password': 'FuckTheHaker@666',
    'database': 'stock',
    'charset': 'utf8mb4'
}

# Tushare配置
TUSHARE_TOKEN = 'gx03013e909f633ecb66722df66b360f070426613316ebf06ecd3482'
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

def parse_date(date_str):
    """解析日期字符串为datetime对象"""
    try:
        # 尝试解析多种格式的日期
        date = parse(date_str)
        return date.strftime('%Y%m%d')
    except:
        raise argparse.ArgumentTypeError(f'无效的日期格式: {date_str}')

def create_moneyflow_dc_table():
    """创建东方财富个股资金流向表"""
    conn = None
    cursor = None
    
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS stock_moneyflow_dc (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            ts_code VARCHAR(10) NOT NULL COMMENT '股票代码',
            trade_date DATE NOT NULL COMMENT '交易日期',
            name VARCHAR(50) COMMENT '股票名称',
            pct_change DECIMAL(8,4) COMMENT '涨跌幅(%)',
            close DECIMAL(10,4) COMMENT '最新价',
            net_amount DECIMAL(18,2) COMMENT '今日主力净流入额（万元）',
            net_amount_rate DECIMAL(8,4) COMMENT '今日主力净流入净占比（%）',
            buy_elg_amount DECIMAL(18,2) COMMENT '今日超大单净流入额（万元）',
            buy_elg_amount_rate DECIMAL(8,4) COMMENT '今日超大单净流入占比（%）',
            buy_lg_amount DECIMAL(18,2) COMMENT '今日大单净流入额（万元）',
            buy_lg_amount_rate DECIMAL(8,4) COMMENT '今日大单净流入占比（%）',
            buy_md_amount DECIMAL(18,2) COMMENT '今日中单净流入额（万元）',
            buy_md_amount_rate DECIMAL(8,4) COMMENT '今日中单净流入占比（%）',
            buy_sm_amount DECIMAL(18,2) COMMENT '今日小单净流入额（万元）',
            buy_sm_amount_rate DECIMAL(8,4) COMMENT '今日小单净流入占比（%）',
            created_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
            updated_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
            UNIQUE KEY uk_code_date (ts_code, trade_date),
            INDEX idx_trade_date (trade_date),
            INDEX idx_ts_code (ts_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='东方财富个股资金流向';
        """
        
        cursor.execute(create_table_sql)
        logging.info("成功创建东方财富个股资金流向表")
    except Exception as e:
        logging.error(f"创建表失败: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_last_trade_date():
    """获取最后一个交易日期"""
    conn = None
    cursor = None
    
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("SELECT MAX(trade_date) FROM stock_moneyflow_dc")
        last_date = cursor.fetchone()[0]
        return last_date
    except Exception as e:
        logging.error(f"获取最后交易日期失败: {str(e)}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_trade_dates(start_date, end_date):
    """获取交易日历"""
    try:
        df = pro.trade_cal(
            exchange='SSE',
            start_date=start_date,
            end_date=end_date,
            is_open='1'
        )
        return df['cal_date'].tolist()
    except Exception as e:
        logging.error(f"获取交易日历失败: {str(e)}")
        return []

def get_sqlalchemy_url():
    """生成正确的SQLAlchemy连接URL"""
    password = quote_plus(DB_CONFIG['password'])  # URL编码密码
    return f"mysql+pymysql://{DB_CONFIG['user']}:{password}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}?charset={DB_CONFIG['charset']}"

def update_moneyflow_dc_data(start_date=None, end_date=None, force_update=False):
    """更新东方财富个股资金流向数据
    
    Args:
        start_date: 开始日期，格式：YYYYMMDD
        end_date: 结束日期，格式：YYYYMMDD
        force_update: 是否强制更新（如果为True，则会覆盖已有数据）
    """
    if not start_date:
        # 获取最后更新日期
        last_date = get_last_trade_date()
        if last_date:
            start_date = (last_date + timedelta(days=1)).strftime('%Y%m%d')
        else:
            # 如果没有数据，默认从2023年9月11日开始（该接口数据开始日期）
            start_date = '20230911'
    
    if not end_date:
        end_date = datetime.now().strftime('%Y%m%d')
    
    logging.info(f"更新日期范围: {start_date} 至 {end_date}")
    
    # 获取交易日历
    trade_dates = get_trade_dates(start_date, end_date)
    if not trade_dates:
        logging.error("未获取到交易日期")
        return
    
    # 创建数据库连接
    engine = create_engine(get_sqlalchemy_url())
    
    total_dates = len(trade_dates)
    for date_idx, trade_date in enumerate(trade_dates, 1):
        try:
            logging.info(f"正在处理日期 ({date_idx}/{total_dates}): {trade_date}")
            
            if force_update:
                # 删除当天的数据
                conn = None
                cursor = None
                try:
                    conn = pymysql.connect(**DB_CONFIG)
                    cursor = conn.cursor()
                    cursor.execute(
                        "DELETE FROM stock_moneyflow_dc WHERE trade_date = %s",
                        (trade_date,)
                    )
                    conn.commit()
                    logging.info(f"已删除 {trade_date} 的历史数据")
                except Exception as e:
                    logging.error(f"删除历史数据失败: {str(e)}")
                    if conn:
                        conn.rollback()
                finally:
                    if cursor:
                        cursor.close()
                    if conn:
                        conn.close()
            
            # 获取当天所有股票的资金流向数据
            df = pro.moneyflow_dc(trade_date=trade_date)
            
            if not df.empty:
                # 处理数据格式
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                
                # 确保数值列的类型正确
                numeric_columns = [
                    'pct_change', 'close', 'net_amount', 'net_amount_rate',
                    'buy_elg_amount', 'buy_elg_amount_rate', 'buy_lg_amount', 'buy_lg_amount_rate',
                    'buy_md_amount', 'buy_md_amount_rate', 'buy_sm_amount', 'buy_sm_amount_rate'
                ]
                
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # 写入数据库
                df.to_sql(
                    'stock_moneyflow_dc',
                    engine,
                    if_exists='append',
                    index=False,
                    chunksize=1000
                )
                logging.info(f"{trade_date} 数据更新完成，共 {len(df)} 条记录")
            else:
                logging.warning(f"{trade_date} 没有数据")
            
            # 避免频繁调用接口
            time.sleep(0.5)
            
        except Exception as e:
            logging.error(f"处理日期 {trade_date} 时出错: {str(e)}")
            continue
    
    engine.dispose()
    logging.info("所有数据更新完成")

def update_single_stock_data(ts_code, start_date=None, end_date=None, force_update=False):
    """更新单个股票的资金流向数据
    
    Args:
        ts_code: 股票代码
        start_date: 开始日期，格式：YYYYMMDD
        end_date: 结束日期，格式：YYYYMMDD
        force_update: 是否强制更新
    """
    if not start_date:
        start_date = '20230911'  # 该接口数据开始日期
    
    if not end_date:
        end_date = datetime.now().strftime('%Y%m%d')
    
    logging.info(f"更新股票 {ts_code} 的资金流向数据，日期范围: {start_date} 至 {end_date}")
    
    try:
        # 获取单个股票的资金流向数据
        df = pro.moneyflow_dc(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date
        )
        
        if not df.empty:
            # 处理数据格式
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            
            # 确保数值列的类型正确
            numeric_columns = [
                'pct_change', 'close', 'net_amount', 'net_amount_rate',
                'buy_elg_amount', 'buy_elg_amount_rate', 'buy_lg_amount', 'buy_lg_amount_rate',
                'buy_md_amount', 'buy_md_amount_rate', 'buy_sm_amount', 'buy_sm_amount_rate'
            ]
            
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 创建数据库连接
            engine = create_engine(get_sqlalchemy_url())
            
            if force_update:
                # 删除指定日期范围的数据
                conn = None
                cursor = None
                try:
                    conn = pymysql.connect(**DB_CONFIG)
                    cursor = conn.cursor()
                    cursor.execute(
                        "DELETE FROM stock_moneyflow_dc WHERE ts_code = %s AND trade_date BETWEEN %s AND %s",
                        (ts_code, start_date, end_date)
                    )
                    conn.commit()
                    logging.info(f"已删除 {ts_code} 在 {start_date} 至 {end_date} 的历史数据")
                except Exception as e:
                    logging.error(f"删除历史数据失败: {str(e)}")
                    if conn:
                        conn.rollback()
                finally:
                    if cursor:
                        cursor.close()
                    if conn:
                        conn.close()
            
            # 写入数据库
            df.to_sql(
                'stock_moneyflow_dc',
                engine,
                if_exists='append',
                index=False,
                chunksize=1000
            )
            
            engine.dispose()
            logging.info(f"股票 {ts_code} 数据更新完成，共 {len(df)} 条记录")
        else:
            logging.warning(f"股票 {ts_code} 在指定日期范围内没有数据")
            
    except Exception as e:
        logging.error(f"更新股票 {ts_code} 数据时出错: {str(e)}")

def main():
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='更新东方财富个股资金流向数据')
    parser.add_argument('--start_date', type=parse_date, help='开始日期 (YYYY-MM-DD 或 YYYYMMDD)')
    parser.add_argument('--end_date', type=parse_date, help='结束日期 (YYYY-MM-DD 或 YYYYMMDD)')
    parser.add_argument('--force', action='store_true', help='强制更新（覆盖已有数据）')
    parser.add_argument('--ts_code', type=str, help='单个股票代码（如果指定，则只更新该股票的数据）')
    
    args = parser.parse_args()
    
    # 创建表（如果不存在）
    create_moneyflow_dc_table()
    
    if args.ts_code:
        # 更新单个股票的数据
        update_single_stock_data(
            ts_code=args.ts_code,
            start_date=args.start_date,
            end_date=args.end_date,
            force_update=args.force
        )
    else:
        # 更新所有股票的数据
        update_moneyflow_dc_data(
            start_date=args.start_date,
            end_date=args.end_date,
            force_update=args.force
        )

if __name__ == "__main__":
    main() 