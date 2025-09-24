"""
股票每日筹码及胜率数据更新脚本

功能：
    从Tushare获取股票每日筹码平均成本和胜率数据并保存到MySQL数据库中。
    支持增量更新和指定日期范围更新。

使用方法：
    1. 增量更新（从最后更新日期到今天）：
        python stock_cyq_update.py

    2. 更新指定开始日期到今天的数据：
        python stock_cyq_update.py --start_date 2024-01-01

    3. 更新指定日期范围的数据：
        python stock_cyq_update.py --start_date 2024-01-01 --end_date 2024-01-31

    4. 强制更新指定日期范围的数据（会先删除该范围的旧数据）：
        python stock_cyq_update.py --start_date 2024-01-01 --end_date 2024-01-31 --force

    5. 更新单个股票的数据：
        python stock_cyq_update.py --ts_code 600000.SH --start_date 2024-01-01

参数说明：
    --start_date: 开始日期，支持YYYY-MM-DD或YYYYMMDD格式
    --end_date: 结束日期，支持YYYY-MM-DD或YYYYMMDD格式
    --ts_code: 股票代码，如600000.SH
    --force: 强制更新标志，会先删除指定日期范围的数据再重新获取

数据说明：
    包含每日筹码平均成本和胜率情况，数据从2018年开始，每天17~18点左右更新
    积分要求：120积分可试用，5000积分每天20000次，10000积分每天200000次

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
import numpy as np

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

# Tushare API字段到数据库字段的映射
FIELD_MAPPINGS = {
    'ts_code': 'ts_code',           # 股票代码
    'trade_date': 'trade_date',     # 交易日期
    'his_low': 'his_low',           # 历史最低价
    'his_high': 'his_high',         # 历史最高价
    'cost_5pct': 'cost_5pct',       # 5分位成本
    'cost_15pct': 'cost_15pct',     # 15分位成本
    'cost_50pct': 'cost_50pct',     # 50分位成本
    'cost_85pct': 'cost_85pct',     # 85分位成本
    'cost_95pct': 'cost_95pct',     # 95分位成本
    'weight_avg': 'weight_avg',     # 加权平均成本
    'winner_rate': 'winner_rate',   # 胜率
}

def parse_date(date_str):
    """解析日期字符串为datetime对象"""
    try:
        # 尝试解析多种格式的日期
        date = parse(date_str)
        return date.strftime('%Y%m%d')
    except:
        raise argparse.ArgumentTypeError(f'无效的日期格式: {date_str}')

def create_stock_cyq_table():
    """创建股票筹码数据表"""
    conn = None
    cursor = None
    
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS `stock_cyq_perf` (
              `ts_code` varchar(10) NOT NULL COMMENT '股票代码',
              `trade_date` date NOT NULL COMMENT '交易日期',
              `his_low` decimal(10,4) DEFAULT NULL COMMENT '历史最低价',
              `his_high` decimal(10,4) DEFAULT NULL COMMENT '历史最高价',
              `cost_5pct` decimal(10,4) DEFAULT NULL COMMENT '5分位成本',
              `cost_15pct` decimal(10,4) DEFAULT NULL COMMENT '15分位成本',
              `cost_50pct` decimal(10,4) DEFAULT NULL COMMENT '50分位成本',
              `cost_85pct` decimal(10,4) DEFAULT NULL COMMENT '85分位成本',
              `cost_95pct` decimal(10,4) DEFAULT NULL COMMENT '95分位成本',
              `weight_avg` decimal(10,4) DEFAULT NULL COMMENT '加权平均成本',
              `winner_rate` decimal(10,4) DEFAULT NULL COMMENT '胜率(%)',
              `created_at` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
              `updated_at` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
              PRIMARY KEY (`ts_code`,`trade_date`),
              KEY `idx_trade_date` (`trade_date`),
              KEY `idx_ts_code` (`ts_code`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='股票每日筹码及胜率数据';
        """
        
        cursor.execute(create_table_sql)
        logging.info("成功创建股票筹码数据表")
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
        
        cursor.execute("SELECT MAX(trade_date) FROM stock_cyq_perf")
        result = cursor.fetchone()
        last_date = result[0] if result and result[0] else None
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

def update_stock_cyq_data(start_date=None, end_date=None, force_update=False):
    """更新股票筹码数据"""
    if not start_date:
        last_date = get_last_trade_date()
        if last_date:
            start_date = (last_date + timedelta(days=1)).strftime('%Y%m%d')
        else:
            # 筹码数据从2018年开始
            start_date = '20180101'
    
    if not end_date:
        end_date = datetime.now().strftime('%Y%m%d')
    
    logging.info(f"更新日期范围: {start_date} 至 {end_date}")
    
    # 获取交易日历
    trade_dates = get_trade_dates(start_date, end_date)
    if not trade_dates:
        logging.error("未获取到交易日期")
        return
    
    engine = create_engine(get_sqlalchemy_url())
    
    total_dates = len(trade_dates)
    for date_idx, trade_date in enumerate(trade_dates, 1):
        try:
            logging.info(f"正在处理日期 ({date_idx}/{total_dates}): {trade_date}")
            
            if force_update:
                with pymysql.connect(**DB_CONFIG) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            "DELETE FROM stock_cyq_perf WHERE trade_date = %s",
                            (trade_date,)
                        )
                        conn.commit()
                        logging.info(f"已删除 {trade_date} 的历史数据")
            
            # 获取当天所有股票的筹码数据
            df = pro.cyq_perf(trade_date=trade_date)
            
            if not df.empty:
                # 重命名列（根据映射关系）
                df = df.rename(columns=FIELD_MAPPINGS)
                
                # 处理日期格式
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                
                # 确保数值列的类型正确
                numeric_columns = ['his_low', 'his_high', 'cost_5pct', 'cost_15pct', 
                                 'cost_50pct', 'cost_85pct', 'cost_95pct', 'weight_avg', 'winner_rate']
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # 只保留映射中定义的列
                df = df[[col for col in FIELD_MAPPINGS.values() if col in df.columns]]
                
                # 写入数据库
                df.to_sql(
                    'stock_cyq_perf',
                    engine,
                    if_exists='append',
                    index=False,
                    chunksize=1000
                )
                logging.info(f"{trade_date} 数据更新完成，共 {len(df)} 条记录")
            else:
                logging.warning(f"{trade_date} 没有数据")
            
            time.sleep(0.5)  # 避免频繁调用接口
            
        except Exception as e:
            logging.error(f"处理日期 {trade_date} 时出错: {str(e)}")
            continue
    
    engine.dispose()
    logging.info("所有数据更新完成")

def update_single_stock_cyq_data(ts_code, start_date=None, end_date=None, force_update=False):
    """更新单个股票的筹码数据
    
    Args:
        ts_code: 股票代码
        start_date: 开始日期，格式：YYYYMMDD
        end_date: 结束日期，格式：YYYYMMDD
        force_update: 是否强制更新
    """
    if not start_date:
        start_date = '20180101'  # 筹码数据从2018年开始
    
    if not end_date:
        end_date = datetime.now().strftime('%Y%m%d')
    
    logging.info(f"更新股票 {ts_code} 的筹码数据，日期范围: {start_date} 至 {end_date}")
    
    try:
        # 获取单个股票的筹码数据
        df = pro.cyq_perf(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date
        )
        
        if not df.empty:
            # 重命名列（根据映射关系）
            df = df.rename(columns=FIELD_MAPPINGS)
            
            # 处理日期格式
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            
            # 确保数值列的类型正确
            numeric_columns = ['his_low', 'his_high', 'cost_5pct', 'cost_15pct', 
                             'cost_50pct', 'cost_85pct', 'cost_95pct', 'weight_avg', 'winner_rate']
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
                        "DELETE FROM stock_cyq_perf WHERE ts_code = %s AND trade_date BETWEEN %s AND %s",
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
                'stock_cyq_perf',
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
    parser = argparse.ArgumentParser(description='更新股票每日筹码及胜率数据')
    parser.add_argument('--start_date', type=parse_date, help='开始日期 (YYYY-MM-DD 或 YYYYMMDD)')
    parser.add_argument('--end_date', type=parse_date, help='结束日期 (YYYY-MM-DD 或 YYYYMMDD)')
    parser.add_argument('--force', action='store_true', help='强制更新（覆盖已有数据）')
    parser.add_argument('--ts_code', type=str, help='单个股票代码（如果指定，则只更新该股票的数据）')
    
    args = parser.parse_args()
    
    # 创建表（如果不存在）
    create_stock_cyq_table()
    
    if args.ts_code:
        # 更新单个股票的数据
        update_single_stock_cyq_data(
            ts_code=args.ts_code,
            start_date=args.start_date,
            end_date=args.end_date,
            force_update=args.force
        )
    else:
        # 更新所有股票的数据
        update_stock_cyq_data(
            start_date=args.start_date,
            end_date=args.end_date,
            force_update=args.force
        )

if __name__ == "__main__":
    main() 