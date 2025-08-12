"""
股票每日数据更新工具

使用说明：
1. 定时任务模式（默认）：
   每个工作日15:00-15:50每10分钟自动更新当天数据
   python stock_daily_update.py
   或
   python stock_daily_update.py --mode schedule

2. 更新单个指定日期数据：
   python stock_daily_update.py --mode single --date YYYYMMDD
   例如：
   python stock_daily_update.py --mode single --date 20240320

3. 更新日期范围数据：
   指定开始日期到今天：
   python stock_daily_update.py --mode range --start YYYYMMDD
   
   指定开始日期和结束日期：
   python stock_daily_update.py --mode range --start YYYYMMDD --end YYYYMMDD
   例如：
   python stock_daily_update.py --mode range --start 20240301 --end 20240320

参数说明：
--mode: 运行模式
       schedule - 定时运行（默认）
       single - 更新单个日期
       range - 更新日期范围
--date: 指定单个日期（YYYYMMDD格式）
--start: 指定开始日期（YYYYMMDD格式）
--end: 指定结束日期（YYYYMMDD格式，可选，默认为今天）

Docker环境使用：
docker exec stock-daily python stock_daily_update.py [参数]
"""

import tushare as ts
import pandas as pd
import time
import pymysql
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import os
import sys
import argparse

# 设置token
ts.set_token('gx03013e909f633ecb66722df66b360f070426613316ebf06ecd3482')
pro = ts.pro_api()

# 数据库配置
DB_CONFIG = {
    'host': 'rds.6f6b.cn',  # 改为docker-compose中的服务名
    'port': 3306,
    'user': 'root',
    'password': 'FuckTheHaker@666',
    'database': 'stock',
    'charset': 'utf8mb4'
}

# 确保logs目录存在
# os.makedirs('logs', exist_ok=True)

# 创建日志格式器
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# 创建控制台处理器
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

# 配置根日志记录器
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(console_handler)

# 移除所有已存在的处理器（避免重复）
for handler in logger.handlers[:]:
    if isinstance(handler, logging.FileHandler):
        logger.removeHandler(handler)

def get_today_date():
    """获取当前交易日期"""
    return datetime.now().strftime('%Y%m%d')

def update_daily_data(update_db=False):
    """更新每日股票数据
    Args:
        update_db (bool): 是否更新数据库，True则连接数据库更新数据，False则只打印SQL语句
    """
    conn = None
    cursor = None
    
    try:
        # 仅在需要更新数据库时建立连接
        if update_db:
            print(DB_CONFIG)
            conn = pymysql.connect(**DB_CONFIG)
            cursor = conn.cursor()
        
        # 获取当天日期
        trade_date = get_today_date()
        logger.info(f"开始更新 {trade_date} 的股票数据...")

        # 直接获取当天所有股票的交易数据
        try:
            df = pro.daily(trade_date=trade_date,
                          fields='ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount')
            
            if not df.empty:
                logger.info(f"获取到 {len(df)} 条交易数据")
                # 批量生成插入语句
                for _, row in df.iterrows():
                    values = []
                    for val in row:
                        if pd.isna(val):
                            values.append('NULL')
                        elif isinstance(val, (int, float)):
                            values.append(str(val))
                        else:
                            values.append(f"'{str(val)}'")
                    
                    # 使用REPLACE INTO来处理可能的重复数据
                    insert_sql = f"""REPLACE INTO stock_daily 
                        (ts_code, trade_date, `open`, high, low, `close`, pre_close, 
                        `change`, pct_chg, vol, amount) 
                        VALUES ({', '.join(values)});"""
                    
                    if update_db:
                        cursor.execute(insert_sql)
                
                if update_db:
                    # 提交事务
                    conn.commit()
                    logger.info(f"{trade_date} 的股票数据已更新到数据库！")
            else:
                logger.info(f"{trade_date} 没有交易数据")
                
        except Exception as e:
            error_msg = f"获取数据时出错: {str(e)}"
            logger.error(error_msg)
            if update_db and conn:
                conn.rollback()
            
    except Exception as e:
        error_msg = f"更新过程中出现错误: {str(e)}"
        logger.error(error_msg)
        if update_db and conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def main():
    """主函数，设置定时任务"""
    scheduler = BlockingScheduler()
    
    # 设置在每个工作日下午3点到4点之间每10分钟执行一次
    scheduler.add_job(
        lambda: update_daily_data(True),  # 定时任务时更新数据库
        trigger='cron',
        day_of_week='mon-fri',
        hour='17',
        minute='0-50/1',  # 在0-50分钟之间每10分钟执行一次
        timezone='Asia/Shanghai'
    )
    
    logger.info("定时任务已启动...")
    print("定时任务已启动...")
    scheduler.start()

def update_daily_data_by_date(trade_date):
    """更新指定日期的股票数据
    Args:
        trade_date (str): 交易日期，格式：YYYYMMDD
    """
    conn = None
    cursor = None
    
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        logger.info(f"开始更新 {trade_date} 的股票数据...")

        try:
            df = pro.daily(trade_date=trade_date,
                          fields='ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount')
            
            if not df.empty:
                logger.info(f"获取到 {len(df)} 条交易数据")
                # 批量生成插入语句
                for _, row in df.iterrows():
                    values = []
                    for val in row:
                        if pd.isna(val):
                            values.append('NULL')
                        elif isinstance(val, (int, float)):
                            values.append(str(val))
                        else:
                            values.append(f"'{str(val)}'")
                    
                    insert_sql = f"""REPLACE INTO stock_daily 
                        (ts_code, trade_date, `open`, high, low, `close`, pre_close, 
                        `change`, pct_chg, vol, amount) 
                        VALUES ({', '.join(values)});"""
                    
                    cursor.execute(insert_sql)
                
                conn.commit()
                logger.info(f"{trade_date} 的股票数据已更新到数据库！")
                return True
            else:
                logger.info(f"{trade_date} 没有交易数据")
                return False
                
        except Exception as e:
            error_msg = f"获取数据时出错: {str(e)}"
            logger.error(error_msg)
            if conn:
                conn.rollback()
            return False
            
    except Exception as e:
        error_msg = f"更新过程中出现错误: {str(e)}"
        logger.error(error_msg)
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def update_daily_data_range(start_date, end_date=None):
    """更新指定日期范围的股票数据
    Args:
        start_date (str): 开始日期，格式：YYYYMMDD
        end_date (str): 结束日期，格式：YYYYMMDD，默认为今天
    """
    if end_date is None:
        end_date = get_today_date()
    
    logger.info(f"开始更新从 {start_date} 到 {end_date} 的股票数据...")
    
    # 获取交易日历
    trade_cal = pro.trade_cal(start_date=start_date, end_date=end_date, is_open='1')
    trade_dates = trade_cal['cal_date'].tolist()
    
    success_count = 0
    total_dates = len(trade_dates)
    
    for i, date in enumerate(trade_dates, 1):
        logger.info(f"正在处理 {date} ({i}/{total_dates})")
        if update_daily_data_by_date(date):
            success_count += 1
        time.sleep(0.5)  # 避免频繁调用API
    
    logger.info(f"数据更新完成！成功更新 {success_count}/{total_dates} 个交易日的数据。")

def run_scheduled_update():
    """运行定时更新任务"""
    scheduler = BlockingScheduler()
    
    scheduler.add_job(
        lambda: update_daily_data_by_date(get_today_date()),
        trigger='cron',
        day_of_week='mon-fri',
        hour='15',
        minute='0-50/10',
        timezone='Asia/Shanghai'
    )
    
    logger.info("定时任务已启动...")
    scheduler.start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='股票数据更新工具')
    parser.add_argument('--mode', choices=['schedule', 'single', 'range'], default='schedule',
                      help='运行模式：schedule-定时运行，single-更新单个日期，range-更新日期范围')
    parser.add_argument('--date', help='指定日期 (YYYYMMDD格式)')
    parser.add_argument('--start', help='开始日期 (YYYYMMDD格式)')
    parser.add_argument('--end', help='结束日期 (YYYYMMDD格式)')
    
    args = parser.parse_args()
    
    if args.mode == 'schedule':
        run_scheduled_update()
    elif args.mode == 'single':
        if not args.date:
            logger.error("需要指定 --date 参数")
            sys.exit(1)
        update_daily_data_by_date(args.date)
    elif args.mode == 'range':
        if not args.start:
            logger.error("需要指定 --start 参数")
            sys.exit(1)
        update_daily_data_range(args.start, args.end)