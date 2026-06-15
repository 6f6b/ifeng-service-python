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

from tushare_config import get_pro_api

pro = get_pro_api()

# 在文件开头添加字段映射关系
# Tushare API字段到数据库字段的映射
FIELD_MAPPINGS = {
    # 基础数据
    'ts_code': 'ts_code',           # 股票代码
    'trade_date': 'trade_date',     # 交易日期
    'open': 'open',                 # 开盘价
    'high': 'high',                 # 最高价
    'low': 'low',                   # 最低价
    'close': 'close',               # 收盘价
    'pre_close': 'pre_close',       # 昨收价
    'change': 'change',             # 涨跌额
    'pct_chg': 'pct_chg',          # 涨跌幅
    'vol': 'vol',                   # 成交量（手）
    'amount': 'amount',             # 成交额（千元）
    
    # 换手率指标
    'turnover_rate': 'turnover_rate',         # 换手率
    'turnover_rate_f': 'turnover_rate_f',     # 换手率(自由流通股)
    'volume_ratio': 'volume_ratio',           # 量比
    
    # 估值指标
    'pe': 'pe',                     # 市盈率
    'pe_ttm': 'pe_ttm',             # 市盈率TTM
    'pb': 'pb',                     # 市净率
    'total_mv': 'total_mv',         # 总市值（万元）
    'circ_mv': 'circ_mv',           # 流通市值（万元）
    
    # MACD指标
    'macd_bfq': 'macd',                 # MACD指标
    'macd_dif_bfq': 'macd_dif',         # MACD DIF值
    'macd_dea_bfq': 'macd_dea',         # MACD DEA值
    
    # KDJ指标
    'kdj_k_bfq': 'kdj_k',               # KDJ K值
    'kdj_d_bfq': 'kdj_d',               # KDJ D值
    'kdj_bfq': 'kdj_j',               # KDJ J值
    
    # RSI指标
    'rsi_bfq_6': 'rsi_6',               # RSI-6值
    'rsi_bfq_12': 'rsi_12',             # RSI-12值
    'rsi_bfq_24': 'rsi_24',             # RSI-24值
    
    # BOLL指标
    'boll_upper_bfq': 'boll_upper',     # BOLL上轨
    'boll_mid_bfq': 'boll_mid',         # BOLL中轨
    'boll_lower_bfq': 'boll_lower',     # BOLL下轨
    
    # 均线指标
    'ema_bfq_5': 'ma_5',                 # 5日均线
    'ema_bfq_10': 'ma_10',               # 10日均线
    'ema_bfq_20': 'ma_20',               # 20日均线
    'ema_bfq_30': 'ma_30',               # 30日均线
    'ema_bfq_60': 'ma_60',               # 60日均线
    'ema_bfq_90': 'ma_90',               # 60日均线
    'ema_bfq_250': 'ma_250',               # 60日均线

    # BIAS指标
    'bias1_bfq': 'bias1',               # 6日BIAS
    'bias2_bfq': 'bias2',               # 12日BIAS
    'bias3_bfq': 'bias3',               # 24日BIAS
    
    # DMI指标
    'dmi_pdi_bfq': 'dmi_pdi',           # DMI上升动向值
    'dmi_mdi_bfq': 'dmi_mdi',           # DMI下降动向值
    'dmi_adx_bfq': 'dmi_adx',           # DMI平均动向值
    'dmi_adxr_bfq': 'dmi_adxr',         # DMI评估动向值
        
    # CCI指标
    'cci_bfq': 'cci',                   # CCI顺势指标
    
    # VR指标
    'vr_bfq': 'vr',                     # VR容量比率
    
    # 统计指标
    'updays': 'updays',             # 连涨天数
    'downdays': 'downdays',         # 连跌天数
}

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
            CREATE TABLE `stock_factor_pro` (
              `ts_code` varchar(10) NOT NULL COMMENT '股票代码',
              `trade_date` date NOT NULL COMMENT '交易日期',
              `open` decimal(10,4) DEFAULT NULL COMMENT '开盘价',
              `high` decimal(10,4) DEFAULT NULL COMMENT '最高价',
              `low` decimal(10,4) DEFAULT NULL COMMENT '最低价',
              `close` decimal(10,4) DEFAULT NULL COMMENT '收盘价',
              `pre_close` decimal(10,4) DEFAULT NULL COMMENT '昨收价',
              `change` decimal(10,4) DEFAULT NULL COMMENT '涨跌额',
              `pct_chg` decimal(10,4) DEFAULT NULL COMMENT '涨跌幅',
              `vol` decimal(20,4) DEFAULT NULL COMMENT '成交量（手）',
              `amount` decimal(20,4) DEFAULT NULL COMMENT '成交额（千元）',
              `turnover_rate` decimal(10,4) DEFAULT NULL COMMENT '换手率（%）',
              `turnover_rate_f` decimal(10,4) DEFAULT NULL COMMENT '换手率（自由流通股）',
              `volume_ratio` decimal(10,4) DEFAULT NULL COMMENT '量比',
              `pe` decimal(10,4) DEFAULT NULL COMMENT '市盈率',
              `pe_ttm` decimal(10,4) DEFAULT NULL COMMENT '市盈率TTM',
              `pb` decimal(10,4) DEFAULT NULL COMMENT '市净率',
              `ps` decimal(10,4) DEFAULT NULL COMMENT '市销率',
              `ps_ttm` decimal(10,4) DEFAULT NULL COMMENT '市销率（TTM）',
              `dv_ratio` decimal(10,4) DEFAULT NULL COMMENT '股息率（%）',
              `dv_ttm` decimal(10,4) DEFAULT NULL COMMENT '股息率（TTM）（%）',
              `total_share` decimal(20,4) DEFAULT NULL COMMENT '总股本（万股）',
              `float_share` decimal(20,4) DEFAULT NULL COMMENT '流通股本（万股）',
              `free_share` decimal(20,4) DEFAULT NULL COMMENT '自由流通股本（万）',
              `total_mv` decimal(20,4) DEFAULT NULL COMMENT '总市值（万元）',
              `circ_mv` decimal(20,4) DEFAULT NULL COMMENT '流通市值（万元）',
              `adj_factor` decimal(10,4) DEFAULT NULL COMMENT '复权因子',

              -- 趋势指标
              `ma_5` decimal(10,4) DEFAULT NULL COMMENT '5日均线',
              `ma_10` decimal(10,4) DEFAULT NULL COMMENT '10日均线',
              `ma_20` decimal(10,4) DEFAULT NULL COMMENT '20日均线',
              `ma_30` decimal(10,4) DEFAULT NULL COMMENT '30日均线',
              `ma_60` decimal(10,4) DEFAULT NULL COMMENT '60日均线',
              `ma_90` decimal(10,4) DEFAULT NULL COMMENT '90日均线',
              `ma_250` decimal(10,4) DEFAULT NULL COMMENT '250日均线',

              -- MACD指标
              `macd` decimal(10,4) DEFAULT NULL COMMENT 'MACD指标',
              `macd_dif` decimal(10,4) DEFAULT NULL COMMENT 'MACD DIF值',
              `macd_dea` decimal(10,4) DEFAULT NULL COMMENT 'MACD DEA值',

              -- KDJ指标
              `kdj_k` decimal(10,4) DEFAULT NULL COMMENT 'KDJ K值',
              `kdj_d` decimal(10,4) DEFAULT NULL COMMENT 'KDJ D值',
              `kdj_j` decimal(10,4) DEFAULT NULL COMMENT 'KDJ J值',

              -- RSI指标
              `rsi_6` decimal(10,4) DEFAULT NULL COMMENT 'RSI-6值',
              `rsi_12` decimal(10,4) DEFAULT NULL COMMENT 'RSI-12值',
              `rsi_24` decimal(10,4) DEFAULT NULL COMMENT 'RSI-24值',

              -- BOLL指标
              `boll_upper` decimal(10,4) DEFAULT NULL COMMENT 'BOLL上轨',
              `boll_mid` decimal(10,4) DEFAULT NULL COMMENT 'BOLL中轨',
              `boll_lower` decimal(10,4) DEFAULT NULL COMMENT 'BOLL下轨',

              -- DMI指标
              `dmi_pdi` decimal(10,4) DEFAULT NULL COMMENT 'DMI上升动向值',
              `dmi_mdi` decimal(10,4) DEFAULT NULL COMMENT 'DMI下降动向值',
              `dmi_adx` decimal(10,4) DEFAULT NULL COMMENT 'DMI平均动向值',
              `dmi_adxr` decimal(10,4) DEFAULT NULL COMMENT 'DMI评估动向值',

              -- CCI指标
              `cci` decimal(10,4) DEFAULT NULL COMMENT 'CCI顺势指标',

              -- 其他技术指标
              `bias1` decimal(10,4) DEFAULT NULL COMMENT '6日BIAS',
              `bias2` decimal(10,4) DEFAULT NULL COMMENT '12日BIAS',
              `bias3` decimal(10,4) DEFAULT NULL COMMENT '24日BIAS',
              `vr` decimal(10,4) DEFAULT NULL COMMENT 'VR容量比率',
              `atr` decimal(10,4) DEFAULT NULL COMMENT '真实波动幅度均值',

              -- 连续涨跌统计
              `updays` int DEFAULT NULL COMMENT '连涨天数',
              `downdays` int DEFAULT NULL COMMENT '连跌天数',
              `topdays` int DEFAULT NULL COMMENT '近期最高价天数',
              `lowdays` int DEFAULT NULL COMMENT '近期最低价天数',

              PRIMARY KEY (`ts_code`,`trade_date`),
              KEY `idx_trade_date` (`trade_date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='股票技术指标数据';
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
    """更新股票技术面因子(专业版)数据"""
    if not start_date:
        last_date = get_last_trade_date()
        if last_date:
            start_date = (last_date + timedelta(days=1)).strftime('%Y%m%d')
        else:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
    
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
            
            # 先删除该日期的旧数据，避免重复
            with pymysql.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "DELETE FROM stock_factor_pro WHERE trade_date = %s",
                        (trade_date,)
                    )
                    conn.commit()
                    deleted_count = cursor.rowcount
                    if deleted_count > 0:
                        logging.info(f"已删除 {trade_date} 的 {deleted_count} 条历史数据")
            
            # 获取当天所有股票的技术面因子数据
            df = pro.stk_factor_pro(trade_date=trade_date)
            
            if not df.empty:
                # 重命名列（根据映射关系）
                df = df.rename(columns=FIELD_MAPPINGS)
                
                # 处理日期格式
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                
                # 确保数值列的类型正确
                numeric_columns = df.select_dtypes(include=[np.number]).columns
                for col in numeric_columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # 只保留映射中定义的列
                df = df[[col for col in FIELD_MAPPINGS.values() if col in df.columns]]
                
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
            
            time.sleep(0.5)  # 避免频繁调用接口
            
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