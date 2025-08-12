import requests
import pandas as pd
import time
import pymysql
from pymysql.cursors import DictCursor

# 数据库配置
DB_CONFIG = {
    'host': 'rds.6f6b.cn',
    'port': 3306,
    'user': 'root',
    'password': 'FuckTheHaker@666',
    'database': 'stock',
    'charset': 'utf8mb4'
}

def get_db_connection():
    """获取数据库连接"""
    return pymysql.connect(**DB_CONFIG, cursorclass=DictCursor)

def save_to_db(df):
    """保存数据到数据库，使用REPLACE INTO确保每天每支股票只有一条记录"""
    if df.empty:
        return
    
    # 构建SQL语句
    columns = [
        'trade_date', 'stock_code', 'market_type', 'stock_name', 'latest_price',
        'change_percent', 'price_change', 'volume', 'turnover', 'amplitude',
        'turnover_rate', 'open_price', 'high_price', 'low_price', 'prev_close',
        'total_market_cap', 'circulating_market_cap', 'pe_static', 'pe_ttm',
        'main_force_inflow', 'super_large_inflow', 'super_large_inflow_pct',
        'large_inflow', 'large_inflow_pct', 'medium_inflow', 'medium_inflow_pct',
        'small_inflow', 'small_inflow_pct', 'main_force_inflow_pct', 'update_time'
    ]
    
    # 构建REPLACE INTO语句
    sql = f"""REPLACE INTO stock_money_flow ({','.join(columns)}) 
             VALUES ({','.join(['%s'] * len(columns))})"""
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 将DataFrame转换为值列表
            values = df.rename(columns={
                '交易日期': 'trade_date',
                '股票代码': 'stock_code',
                '市场类型': 'market_type',
                '股票名称': 'stock_name',
                '最新价': 'latest_price',
                '涨跌幅(%)': 'change_percent',
                '涨跌额': 'price_change',
                '成交量': 'volume',
                '成交额(元)': 'turnover',
                '振幅(%)': 'amplitude',
                '换手率(%)': 'turnover_rate',
                '今开价': 'open_price',
                '最高价': 'high_price',
                '最低价': 'low_price',
                '昨收价': 'prev_close',
                '总市值(元)': 'total_market_cap',
                '流通市值(元)': 'circulating_market_cap',
                '市盈率(静态)': 'pe_static',
                '市盈率(TTM)': 'pe_ttm',
                '主力净流入(元)': 'main_force_inflow',
                '超大单净流入(元)': 'super_large_inflow',
                '超大单净流入占比(%)': 'super_large_inflow_pct',
                '大单净流入(元)': 'large_inflow',
                '大单净流入占比(%)': 'large_inflow_pct',
                '中单净流入(元)': 'medium_inflow',
                '中单净流入占比(%)': 'medium_inflow_pct',
                '小单净流入(元)': 'small_inflow',
                '小单净流入占比(%)': 'small_inflow_pct',
                '主力净流入占比(%)': 'main_force_inflow_pct',
                '数据更新时间': 'update_time'
            })[columns].values.tolist()
            
            # 批量插入数据
            cursor.executemany(sql, values)
        conn.commit()
    except Exception as e:
        print(f"数据库操作错误: {e}")
        conn.rollback()
    finally:
        conn.close()

def fetch_board_moneyflow(page=1, page_size=100):
    """获取主板资金流向数据"""
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": page,
        "pz": page_size,
        "po": "1",
        "np": "1",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:1 t:2,m:0 t:6",  # 沪市主板 + 深市主板
        "fields": "f2,f3,f4,f5,f6,f7,f8,f12,f13,f14,f15,f16,f17,f18,f20,f21,f62,f66,f69,f72,f75,f78,f81,f84,f87,f114,f115,f124,f184,f297",
    }
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    try:
        resp = requests.get(url, params=params, headers=headers)
        data = resp.json()
        if data.get("data") == None:
            print("API返回数据为None")
            return pd.DataFrame()
        items = data.get("data", {}).get("diff", [])
        if not items:
            print(f"API返回数据为空或异常，完整返回: {data}")
            return pd.DataFrame()

        records = []
        for item in items:
            try:
                records.append({
                    "交易日期": str(item.get("f297", "")),
                    "股票代码": str(item.get("f12", "")),
                    "市场类型": "SZ" if item.get("f13", 1) == 0 else "SH",
                    "股票名称": str(item.get("f14", "")),
                    "最新价": str(item.get("f2", "")),
                    "涨跌幅(%)": str(item.get("f3", "")),
                    "涨跌额": str(item.get("f4", "")),
                    "成交量": str(item.get("f5", "")),
                    "成交额(元)": str(item.get("f6", "")),
                    "振幅(%)": str(item.get("f7", "")),
                    "换手率(%)": str(item.get("f8", "")),
                    "今开价": str(item.get("f17", "")),
                    "最高价": str(item.get("f15", "")),
                    "最低价": str(item.get("f16", "")),
                    "昨收价": str(item.get("f18", "")),
                    "总市值(元)": str(item.get("f20", "")),
                    "流通市值(元)": str(item.get("f21", "")),
                    "市盈率(静态)": str(item.get("f114", "")),
                    "市盈率(TTM)": str(item.get("f115", "")),
                    "主力净流入(元)": str(item.get("f62", "")),
                    "超大单净流入(元)": str(item.get("f66", "")),
                    "超大单净流入占比(%)": str(item.get("f69", "")),
                    "大单净流入(元)": str(item.get("f72", "")),
                    "大单净流入占比(%)": str(item.get("f75", "")),
                    "中单净流入(元)": str(item.get("f78", "")),
                    "中单净流入占比(%)": str(item.get("f81", "")),
                    "小单净流入(元)": str(item.get("f84", "")),
                    "小单净流入占比(%)": str(item.get("f87", "")),
                    "主力净流入占比(%)": str(item.get("f184", "")),
                    "数据更新时间": str(item.get("f124", "")),
                })
            except Exception as e:
                print(f"处理数据时出错: {e}")
                print(f"出错的数据项: {item}")
                continue
        return pd.DataFrame(records)
    except Exception as e:
        print(f"获取数据错误: {e}")
        return pd.DataFrame()

def main():
    """主函数"""
    try:
        total_pages = 60
        print(f"开始获取数据，共{total_pages}页...")
        for page in range(1, total_pages):
            print(f"正在获取第{page}/{total_pages-1}页数据...")
            df = fetch_board_moneyflow(page=page)
            if not df.empty:
                print(f"第{page}页获取成功，正在写入数据库...")
                save_to_db(df)
                print(f"第{page}页数据写入完成")
            else:
                print(f"第{page}页数据为空，跳过")
            time.sleep(0.5)  # 避免过快被封
        print("所有数据更新完成!")
    except Exception as e:
        print(f"程序执行错误: {e}")

if __name__ == "__main__":
    main()
