from datetime import datetime, timedelta
import pandas as pd
from Ashare import *

def fetch_stock_data(stock_code, data_type='daily', days=5, end_date=None):
    """
    获取股票数据的函数
    
    参数:
        stock_code (str): 股票代码 (例如: 'sh000001', 'sh600519')
        data_type (str): 数据类型 ('daily', 'weekly', 'monthly', '1m', '5m', '15m', '30m', '60m')
        days (int): 获取多少天的数据
        end_date (str): 结束日期，格式：'YYYY-MM-DD'，默认为None（当前日期）
    
    返回:
        pandas.DataFrame: 包含股票数据的DataFrame
    """
    # 转换数据类型到frequency参数
    frequency_map = {
        'daily': '1d',
        'weekly': '1w',
        'monthly': '1M',
        '1m': '1m',
        '5m': '5m',
        '15m': '15m',
        '30m': '30m',
        '60m': '60m'
    }
    
    frequency = frequency_map.get(data_type, '1d')
    
    try:
        # 获取股票数据
        df = get_price(stock_code, 
                      frequency=frequency,
                      count=days,
                      end_date=end_date)
        
        return df
    except Exception as e:
        print(f"获取数据时出错: {e}")
        return None

def save_to_excel(df, filename):
    """
    将数据保存到Excel文件
    
    参数:
        df (pandas.DataFrame): 要保存的数据
        filename (str): 文件名
    """
    try:
        df.to_excel(filename, index=True)
        print(f"数据已保存到 {filename}")
    except Exception as e:
        print(f"保存数据时出错: {e}")

if __name__ == "__main__":
    # 示例：获取上证指数数据
    print("获取上证指数日线数据...")
    df_daily = fetch_stock_data('sh000001', 'daily', days=5)
    print("\n上证指数日线数据:")
    print(df_daily)
    
    # 获取贵州茅台分钟数据
    print("\n获取贵州茅台15分钟线数据...")
    df_minutes = fetch_stock_data('sh600519', '15m', days=5)
    print("\n贵州茅台15分钟线数据:")
    print(df_minutes)
    
    # 保存数据到Excel
    if df_daily is not None:
        save_to_excel(df_daily, 'shanghai_index_daily.xlsx')
    if df_minutes is not None:
        save_to_excel(df_minutes, 'maotai_15min.xlsx') 