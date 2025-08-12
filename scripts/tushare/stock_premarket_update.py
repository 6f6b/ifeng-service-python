import tushare as ts
import pandas as pd
import pymysql
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import os
import sys

# 设置token
ts.set_token('gx03013e909f633ecb66722df66b360f070426613316ebf06ecd3482')
pro = ts.pro_api()

# 数据库配置
DB_CONFIG = {
    'host': '192.168.1.6',
    'port': 3306,
    'user': 'root',
    'password': 'lf123456',
    'database': 'stock',
    'charset': 'utf8mb4'
}

# 确保logs目录存在
os.makedirs('logs', exist_ok=True)

# 配置日志
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

file_handler = logging.FileHandler('logs/stock_premarket.log')
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

def get_today_date():
    """获取当前交易日期"""
    return datetime.now().strftime('%Y%m%d')

def update_premarket_data(update_db=False):
    """更新盘前股本数据
    Args:
        update_db (bool): 是否更新数据库，True则连接数据库更新数据，False则只打印SQL语句
    """
    conn = None
    cursor = None
    
    try:
        # 仅在需要更新数据库时建立连接
        if update_db:
            conn = pymysql.connect(**DB_CONFIG)
            cursor = conn.cursor()
            
            # 创建表（如果不存在）
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS stock_premarket (
                trade_date VARCHAR(8) COMMENT '交易日期',
                ts_code VARCHAR(10) COMMENT 'TS股票代码',
                total_share DECIMAL(20,4) COMMENT '总股本（万股）',
                float_share DECIMAL(20,4) COMMENT '流通股本（万股）',
                pre_close DECIMAL(20,4) COMMENT '昨日收盘价',
                up_limit DECIMAL(20,4) COMMENT '今日涨停价',
                down_limit DECIMAL(20,4) COMMENT '今日跌停价',
                PRIMARY KEY (ts_code, trade_date),
                INDEX idx_trade_date (trade_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '股票盘前数据表';
            """
            cursor.execute(create_table_sql)
            conn.commit()
        
        # 获取当天日期
        trade_date = get_today_date()
        logger.info(f"开始获取 {trade_date} 的盘前数据...")

        try:
            # 获取盘前数据
            df = pro.stk_premarket(trade_date=trade_date)
            
            if not df.empty:
                logger.info(f"获取到 {len(df)} 条盘前数据")
                
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
                    insert_sql = f"""REPLACE INTO stock_premarket 
                        (trade_date, ts_code, total_share, float_share, 
                        pre_close, up_limit, down_limit) 
                        VALUES ({', '.join(values)});"""
                    
                    logger.info(insert_sql)
                    if update_db:
                        cursor.execute(insert_sql)
                
                if update_db:
                    conn.commit()
                    logger.info(f"{trade_date} 的盘前数据已更新到数据库！")
            else:
                logger.info(f"{trade_date} 没有盘前数据")
                
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
    
    # 设置在每个工作日早上8:30执行
    scheduler.add_job(
        lambda: update_premarket_data(True),
        trigger='cron',
        day_of_week='mon-fri',
        hour=8,
        minute=30,
        timezone='Asia/Shanghai'
    )
    
    logger.info("定时任务已启动...")
    scheduler.start()

if __name__ == "__main__":
    # main()  # 启动定时任务
    update_premarket_data(True)  # 直接执行一次更新 