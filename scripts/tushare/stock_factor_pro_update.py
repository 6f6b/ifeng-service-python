"""
股票技术面因子(专业版)数据更新脚本

功能：
    从Tushare获取股票技术面因子(专业版)数据并保存到MySQL数据库中。
    支持增量更新和指定日期范围更新。

使用方法：
    1. 增量更新（从最后更新日期到今天）：
        python stock_factor_pro_update.py

    2. 更新指定开始日期到今天的数据：
        python stock_factor_pro_update.py --start_date 2024-01-01

    3. 更新指定日期范围的数据：
        python stock_factor_pro_update.py --start_date 2024-01-01 --end_date 2024-01-31

    4. 强制更新指定日期范围的数据（会先删除该范围的旧数据）：
        python stock_factor_pro_update.py --start_date 2024-01-01 --end_date 2024-01-31 --force

参数说明：
    --start_date: 开始日期，支持YYYY-MM-DD或YYYYMMDD格式
    --end_date: 结束日期，支持YYYY-MM-DD或YYYYMMDD格式
    --force: 强制更新标志，会先删除指定日期范围的数据再重新获取

数据说明：
    包含多种技术指标，如MACD、KDJ、RSI、BOLL等，支持前复权、后复权、不复权三种模式

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

def parse_date(date_str):
    """解析日期字符串为datetime对象"""
    try:
        # 尝试解析多种格式的日期
        date = parse(date_str)
        return date.strftime('%Y%m%d')
    except:
        raise argparse.ArgumentTypeError(f'无效的日期格式: {date_str}')

def create_stock_factor_pro_table():
    """创建股票技术面因子(专业版)表"""
    conn = None
    cursor = None
    
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS stock_factor_pro (
            ts_code VARCHAR(10) NOT NULL COMMENT '股票代码',
            trade_date DATE NOT NULL COMMENT '交易日期',
            open DECIMAL(10,4) COMMENT '开盘价',
            high DECIMAL(10,4) COMMENT '最高价',
            low DECIMAL(10,4) COMMENT '最低价',
            close DECIMAL(10,4) COMMENT '收盘价',
            pre_close DECIMAL(10,4) COMMENT '昨收价',
            `change` DECIMAL(10,4) COMMENT '涨跌额',
            pct_chg DECIMAL(10,4) COMMENT '涨跌幅',
            vol DECIMAL(20,4) COMMENT '成交量（手）',
            amount DECIMAL(20,4) COMMENT '成交额（千元）',
            turnover_rate DECIMAL(10,4) COMMENT '换手率（%）',
            turnover_rate_f DECIMAL(10,4) COMMENT '换手率（自由流通股）',
            volume_ratio DECIMAL(10,4) COMMENT '量比',
            pe DECIMAL(10,4) COMMENT '市盈率',
            pe_ttm DECIMAL(10,4) COMMENT '市盈率TTM',
            pb DECIMAL(10,4) COMMENT '市净率',
            total_mv DECIMAL(20,4) COMMENT '总市值（万元）',
            circ_mv DECIMAL(20,4) COMMENT '流通市值（万元）',
            
            /* MACD指标 */
            macd_dif DECIMAL(10,4) COMMENT 'MACD DIF值',
            macd_dea DECIMAL(10,4) COMMENT 'MACD DEA值',
            macd DECIMAL(10,4) COMMENT 'MACD柱',
            
            /* KDJ指标 */
            kdj_k DECIMAL(10,4) COMMENT 'KDJ K值',
            kdj_d DECIMAL(10,4) COMMENT 'KDJ D值',
            kdj_j DECIMAL(10,4) COMMENT 'KDJ J值',
            
            /* RSI指标 */
            rsi_6 DECIMAL(10,4) COMMENT 'RSI-6',
            rsi_12 DECIMAL(10,4) COMMENT 'RSI-12',
            rsi_24 DECIMAL(10,4) COMMENT 'RSI-24',
            
            /* BOLL指标 */
            boll_upper DECIMAL(10,4) COMMENT 'BOLL上轨',
            boll_mid DECIMAL(10,4) COMMENT 'BOLL中轨',
            boll_lower DECIMAL(10,4) COMMENT 'BOLL下轨',
            
            /* 均线指标 */
            ma_5 DECIMAL(10,4) COMMENT '5日均线',
            ma_10 DECIMAL(10,4) COMMENT '10日均线',
            ma_20 DECIMAL(10,4) COMMENT '20日均线',
            ma_30 DECIMAL(10,4) COMMENT '30日均线',
            ma_60 DECIMAL(10,4) COMMENT '60日均线',
            
            /* 趋势指标 */
            bias1 DECIMAL(10,4) COMMENT '6日BIAS',
            bias2 DECIMAL(10,4) COMMENT '12日BIAS',
            bias3 DECIMAL(10,4) COMMENT '24日BIAS',
            cci DECIMAL(10,4) COMMENT 'CCI顺势指标',
            
            /* 成交量指标 */
            vr DECIMAL(10,4) COMMENT 'VR容量比率',
            
            /* 其他技术指标 */
            dmi_pdi DECIMAL(10,4) COMMENT 'DMI上升动向值',
            dmi_mdi DECIMAL(10,4) COMMENT 'DMI下降动向值',
            dmi_adx DECIMAL(10,4) COMMENT 'DMI平均动向值',
            dmi_adxr DECIMAL(10,4) COMMENT 'DMI评估动向值',
            
            /* 连续涨跌统计 */
            updays DECIMAL(10,4) COMMENT '连涨天数',
            downdays DECIMAL(10,4) COMMENT '连跌天数',
            PRIMARY KEY (ts_code, trade_date),
            INDEX idx_trade_date (trade_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票技术面因子(专业版)';
        """
        
        cursor.execute(create_table_sql)
        logging.info("成功创建股票技术面因子(专业版)表")
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
        
        cursor.execute("SELECT MAX(trade_date) FROM stock_factor_pro")
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

def update_stock_factor_pro_data(start_date=None, end_date=None, force_update=False):
    """更新股票技术面因子(专业版)数据
    
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
            # 如果没有数据，默认从30天前开始
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
    
    if not end_date:
        end_date = datetime.now().strftime('%Y%m%d')
    
    logging.info(f"更新日期范围: {start_date} 至 {end_date}")
    
    # 获取交易日历
    trade_dates = get_trade_dates(start_date, end_date)
    if not trade_dates:
        logging.error("未获取到交易日期")
        return
    
    # 获取所有股票列表
    try:
        stocks = pro.stock_basic(exchange='', list_status='L')
        stock_list = stocks['ts_code'].tolist()
    except Exception as e:
        logging.error(f"获取股票列表失败: {str(e)}")
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
                        "DELETE FROM stock_factor_pro WHERE trade_date = %s",
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
            
            # 获取当天所有股票的技术面因子数据
            df = pro.stk_factor_pro(trade_date=trade_date)
            
            if not df.empty:
                # 处理数据格式
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                
                # 获取数据库表的所有字段
                conn = pymysql.connect(**DB_CONFIG)
                cursor = conn.cursor()
                cursor.execute("DESC stock_factor_pro")
                db_columns = [row[0] for row in cursor.fetchall()]
                cursor.close()
                conn.close()
                
                # 只保留数据库中存在的字段
                existing_columns = [col for col in df.columns if col in db_columns]
                df = df[existing_columns]
                
                # 确保数值列的类型正确
                numeric_columns = df.select_dtypes(include=[np.number]).columns
                for col in numeric_columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # 写入数据库
                df.to_sql(
                    'stock_factor_pro',
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
    """更新单个股票的技术面因子数据
    
    Args:
        ts_code: 股票代码
        start_date: 开始日期，格式：YYYYMMDD
        end_date: 结束日期，格式：YYYYMMDD
        force_update: 是否强制更新
    """
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
    
    if not end_date:
        end_date = datetime.now().strftime('%Y%m%d')
    
    logging.info(f"更新股票 {ts_code} 的技术面因子数据，日期范围: {start_date} 至 {end_date}")
    
    try:
        # 获取单个股票的技术面因子数据
        df = pro.stk_factor_pro(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date
        )
        
        if not df.empty:
            # 处理数据格式
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            
            # 获取数据库表的所有字段
            conn = pymysql.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute("DESC stock_factor_pro")
            db_columns = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            
            # 只保留数据库中存在的字段
            existing_columns = [col for col in df.columns if col in db_columns]
            df = df[existing_columns]
            
            # 确保数值列的类型正确
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            for col in numeric_columns:
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
                        "DELETE FROM stock_factor_pro WHERE ts_code = %s AND trade_date BETWEEN %s AND %s",
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
                'stock_factor_pro',
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
    parser = argparse.ArgumentParser(description='更新股票技术面因子(专业版)数据')
    parser.add_argument('--start_date', type=parse_date, help='开始日期 (YYYY-MM-DD 或 YYYYMMDD)')
    parser.add_argument('--end_date', type=parse_date, help='结束日期 (YYYY-MM-DD 或 YYYYMMDD)')
    parser.add_argument('--force', action='store_true', help='强制更新（覆盖已有数据）')
    parser.add_argument('--ts_code', type=str, help='单个股票代码（如果指定，则只更新该股票的数据）')
    
    args = parser.parse_args()
    
    # 创建表（如果不存在）
    create_stock_factor_pro_table()
    
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
        update_stock_factor_pro_data(
            start_date=args.start_date,
            end_date=args.end_date,
            force_update=args.force
        )

if __name__ == "__main__":
    main() 