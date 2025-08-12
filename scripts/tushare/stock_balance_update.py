import tushare as ts
import pandas as pd
import pymysql
from datetime import datetime, timedelta
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

file_handler = logging.FileHandler('logs/stock_balance.log')
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

def get_last_quarter_date():
    """获取上一个季度末的日期"""
    today = datetime.now()
    month = today.month
    year = today.year
    
    # 确定上一个季度的月份
    if month <= 3:
        year -= 1
        month = 12
    elif month <= 6:
        month = 3
    elif month <= 9:
        month = 6
    else:
        month = 9
    
    # 返回上一个季度末的日期
    return f"{year}{month:02d}31"

def create_balance_table(cursor):
    """创建资产负债表"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS stock_balance (
        ts_code VARCHAR(10) COMMENT 'TS股票代码',
        ann_date VARCHAR(8) COMMENT '公告日期',
        f_ann_date VARCHAR(8) COMMENT '实际公告日期',
        end_date VARCHAR(8) COMMENT '报告期',
        report_type VARCHAR(2) COMMENT '报表类型',
        comp_type VARCHAR(2) COMMENT '公司类型',
        total_share DECIMAL(20,4) COMMENT '期末总股本',
        cap_rese DECIMAL(20,4) COMMENT '资本公积金',
        undistr_porfit DECIMAL(20,4) COMMENT '未分配利润',
        surplus_rese DECIMAL(20,4) COMMENT '盈余公积金',
        special_rese DECIMAL(20,4) COMMENT '专项储备',
        money_cap DECIMAL(20,4) COMMENT '货币资金',
        trad_asset DECIMAL(20,4) COMMENT '交易性金融资产',
        notes_receiv DECIMAL(20,4) COMMENT '应收票据',
        accounts_receiv DECIMAL(20,4) COMMENT '应收账款',
        total_cur_assets DECIMAL(20,4) COMMENT '流动资产合计',
        total_assets DECIMAL(20,4) COMMENT '资产总计',
        total_cur_liab DECIMAL(20,4) COMMENT '流动负债合计',
        total_liab DECIMAL(20,4) COMMENT '负债合计',
        total_hldr_eqy_exc_min_int DECIMAL(20,4) COMMENT '股东权益合计(不含少数股东权益)',
        update_flag VARCHAR(1) COMMENT '更新标识',
        PRIMARY KEY (ts_code, end_date, report_type),
        INDEX idx_ann_date (ann_date),
        INDEX idx_end_date (end_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '公司资产负债表';
    """
    cursor.execute(create_table_sql)

def update_balance_data(update_db=False):
    """更新资产负债表数据
    Args:
        update_db (bool): 是否更新数据库，True则连接数据库更新数据，False则只打印SQL语句
    """
    conn = None
    cursor = None
    
    try:
        # 获取所有股票列表
        stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code')
        end_date = get_last_quarter_date()
        
        if update_db:
            conn = pymysql.connect(**DB_CONFIG)
            cursor = conn.cursor()
            create_balance_table(cursor)
            conn.commit()
        
        logger.info(f"开始获取 {end_date} 的资产负债表数据...")
        total_stocks = len(stocks)
        
        for index, row in stocks.iterrows():
            ts_code = row['ts_code']
            logger.info(f"处理进度: {index + 1}/{total_stocks}, 股票代码: {ts_code}")
            
            try:
                # 获取单个股票的资产负债表数据
                df = pro.balancesheet(ts_code=ts_code, period=end_date,
                                    fields='ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,'
                                          'total_share,cap_rese,undistr_porfit,surplus_rese,special_rese,'
                                          'money_cap,trad_asset,notes_receiv,accounts_receiv,'
                                          'total_cur_assets,total_assets,total_cur_liab,total_liab,'
                                          'total_hldr_eqy_exc_min_int,update_flag')
                
                if not df.empty:
                    for _, data_row in df.iterrows():
                        values = []
                        for val in data_row:
                            if pd.isna(val):
                                values.append('NULL')
                            elif isinstance(val, (int, float)):
                                values.append(str(val))
                            else:
                                values.append(f"'{str(val)}'")
                        
                        insert_sql = f"""REPLACE INTO stock_balance 
                            (ts_code, ann_date, f_ann_date, end_date, report_type, comp_type,
                            total_share, cap_rese, undistr_porfit, surplus_rese, special_rese,
                            money_cap, trad_asset, notes_receiv, accounts_receiv,
                            total_cur_assets, total_assets, total_cur_liab, total_liab,
                            total_hldr_eqy_exc_min_int, update_flag)
                            VALUES ({', '.join(values)});"""
                        
                        logger.info(insert_sql)
                        if update_db:
                            cursor.execute(insert_sql)
                            conn.commit()
                
            except Exception as e:
                error_msg = f"处理股票 {ts_code} 时出错: {str(e)}"
                logger.error(error_msg)
                continue
            
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
    
    # 设置在每个月第一个工作日的早上9点执行
    scheduler.add_job(
        lambda: update_balance_data(True),
        trigger='cron',
        day='1',
        hour=9,
        minute=0,
        timezone='Asia/Shanghai'
    )
    
    logger.info("定时任务已启动...")
    scheduler.start()

if __name__ == "__main__":
    # main()  # 启动定时任务
    update_balance_data(True)  # 直接执行一次更新 