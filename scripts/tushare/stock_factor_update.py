"""
股票技术因子数据更新脚本

功能：
    从Tushare获取股票技术因子数据并保存到MySQL数据库中。
    包括MACD、KDJ、RSI、BOLL等技术指标。
    支持增量更新和指定日期范围更新。

使用方法：
    1. 增量更新（从最后更新日期到今天）：
        python stock_factor_update.py

    2. 更新指定开始日期到今天的数据：
        python stock_factor_update.py --start_date 2024-01-01

    3. 更新指定日期范围的数据：
        python stock_factor_update.py --start_date 2024-01-01 --end_date 2024-01-31

    4. 强制更新指定日期范围的数据（会先删除该范围的旧数据）：
        python stock_factor_update.py --start_date 2024-01-01 --end_date 2024-01-31 --force

参数说明：
    --start_date: 开始日期，支持YYYY-MM-DD或YYYYMMDD格式
    --end_date: 结束日期，支持YYYY-MM-DD或YYYYMMDD格式
    --force: 强制更新标志，会先删除指定日期范围的数据再重新获取

数据说明：
    MACD: 平滑异同移动平均线
    KDJ: 随机指标
    RSI: 相对强弱指标
    BOLL: 布林线

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
        date = parse(date_str)
        return date.strftime('%Y%m%d')
    except:
        raise argparse.ArgumentTypeError(f'无效的日期格式: {date_str}')

def create_factor_table():
    """创建股票技术因子表"""
    conn = None
    cursor = None
    
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS stock_factor (
            ts_code VARCHAR(10) NOT NULL COMMENT '股票代码（格式：000001.SZ）',
            trade_date DATE NOT NULL COMMENT '交易日期',
            close DECIMAL(10,4) COMMENT '当日收盘价',
            open DECIMAL(10,4) COMMENT '当日开盘价',
            high DECIMAL(10,4) COMMENT '当日最高价',
            low DECIMAL(10,4) COMMENT '当日最低价',
            pre_close DECIMAL(10,4) COMMENT '昨日收盘价',
            `change` DECIMAL(10,4) COMMENT '涨跌额（当日收盘价-昨日收盘价）',
            pct_change DECIMAL(10,4) COMMENT '涨跌幅（未复权，单位：%）',
            vol DECIMAL(18,2) COMMENT '成交量（手）',
            amount DECIMAL(18,2) COMMENT '成交额（千元）',
            adj_factor DECIMAL(10,4) COMMENT '复权因子',
            open_hfq DOUBLE COMMENT '开盘价（后复权）',
            open_qfq DOUBLE COMMENT '开盘价（前复权）',
            close_hfq DOUBLE COMMENT '收盘价（后复权）',
            close_qfq DOUBLE COMMENT '收盘价（前复权）',
            high_hfq DOUBLE COMMENT '最高价（后复权）',
            high_qfq DOUBLE COMMENT '最高价（前复权）',
            low_hfq DOUBLE COMMENT '最低价（后复权）',
            low_qfq DOUBLE COMMENT '最低价（前复权）',
            pre_close_hfq DOUBLE COMMENT '昨收价（后复权）',
            pre_close_qfq DOUBLE COMMENT '昨收价（前复权）',
            macd_dif DECIMAL(10,4) COMMENT 'MACD的DIF值（基于前复权价格计算）',
            macd_dea DECIMAL(10,4) COMMENT 'MACD的DEA值（也称MACD值，基于前复权价格计算）',
            macd DECIMAL(10,4) COMMENT 'MACD指标，即MACD柱（基于前复权价格计算）',
            kdj_k DECIMAL(10,4) COMMENT 'KDJ的K值，默认参数9,3,3',
            kdj_d DECIMAL(10,4) COMMENT 'KDJ的D值，默认参数9,3,3',
            kdj_j DECIMAL(10,4) COMMENT 'KDJ的J值，默认参数9,3,3',
            rsi_6 DECIMAL(10,4) COMMENT 'RSI指标，周期为6日',
            rsi_12 DECIMAL(10,4) COMMENT 'RSI指标，周期为12日',
            rsi_24 DECIMAL(10,4) COMMENT 'RSI指标，周期为24日',
            boll_upper DECIMAL(10,4) COMMENT '布林线上轨，默认参数20,2',
            boll_mid DECIMAL(10,4) COMMENT '布林线中轨，即20日移动平均线',
            boll_lower DECIMAL(10,4) COMMENT '布林线下轨，默认参数20,2',
            cci DOUBLE COMMENT 'CCI指标（顺势指标），默认参数14日',
            UNIQUE KEY uk_code_date (ts_code, trade_date) COMMENT '股票代码和交易日期的唯一索引',
            INDEX idx_trade_date (trade_date) COMMENT '交易日期索引',
            INDEX idx_ts_code (ts_code) COMMENT '股票代码索引'
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='股票技术因子表，包含MACD、KDJ、RSI、BOLL等技术指标';
        """
        
        cursor.execute(create_table_sql)
        logging.info("成功创建股票技术因子表")
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
        
        cursor.execute("SELECT MAX(trade_date) FROM stock_factor")
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
    """生成SQLAlchemy连接URL"""
    password = quote_plus(DB_CONFIG['password'])
    return f"mysql+pymysql://{DB_CONFIG['user']}:{password}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}?charset={DB_CONFIG['charset']}"

def update_factor_data(start_date=None, end_date=None, force_update=False):
    """更新股票技术因子数据"""
    if not start_date:
        last_date = get_last_trade_date()
        if last_date:
            start_date = (last_date + timedelta(days=1)).strftime('%Y%m%d')
        else:
            start_date = '20100101'  # 默认从2010年开始
    
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
                        "DELETE FROM stock_factor WHERE trade_date = %s",
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
            
            # 获取当天所有股票的技术因子数据
            df = pro.stk_factor(trade_date=trade_date)
            
            if not df.empty:
                # 处理数据格式
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                
                # 写入数据库
                df.to_sql(
                    'stock_factor',
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

def main():
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='更新股票技术因子数据')
    parser.add_argument('--start_date', type=parse_date, help='开始日期 (YYYY-MM-DD 或 YYYYMMDD)')
    parser.add_argument('--end_date', type=parse_date, help='结束日期 (YYYY-MM-DD 或 YYYYMMDD)')
    parser.add_argument('--force', action='store_true', help='强制更新（覆盖已有数据）')
    
    args = parser.parse_args()
    
    # 创建表（如果不存在）
    create_factor_table()
    
    # 更新数据
    update_factor_data(
        start_date=args.start_date,
        end_date=args.end_date,
        force_update=args.force
    )

if __name__ == "__main__":
    main() 