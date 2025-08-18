import pandas as pd
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

def get_data():
    conn = pymysql.connect(**DB_CONFIG, cursorclass=DictCursor)
    try:
        sql = """
        SELECT 
            m.ts_code, m.trade_date, m.name,
            f.pre_close,  -- 前一天收盘价
            f.high,       -- 当天最高价
            f.low,        -- 当天最低价
            f.close,      -- 当天收盘价
            m.net_amount,        -- 主力资金净流入额（万元）
            m.net_amount_rate,   -- 主力资金净流入占比（%）
            f.total_mv / 10000 as total_mv  -- 总市值（亿元）
        FROM stock_moneyflow_dc m
        JOIN stock_factor_pro f ON m.ts_code = f.ts_code AND m.trade_date = f.trade_date
        where f.trade_date > '20220515' and f.trade_date < '20250815' AND (m.ts_code like "%.S%")
        ORDER BY m.ts_code, m.trade_date
        """
        
        with conn.cursor() as cursor:
            cursor.execute(sql)
            results = cursor.fetchall()
            
        df = pd.DataFrame(results)
        return df
    except Exception as e:
        print(f"发生错误: {str(e)}")
        raise
    finally:
        conn.close()

def analyze_rebound(df):
    # 计算每天最高价、最低价和收盘价相对前一天收盘价的涨跌幅
    df['high_change'] = (df['high'] - df['pre_close']) / df['pre_close'] * 100
    df['low_change'] = (df['low'] - df['pre_close']) / df['pre_close'] * 100
    df['close_change'] = (df['close'] - df['pre_close']) / df['pre_close'] * 100
    
    all_next_days = []
    stock_specific_results = []
    
    for ts_code, group in df.groupby('ts_code'):
        group = group.reset_index(drop=True)
        for i in range(len(group) - 4):  # 需要往后看4天（2天信号+2天观察）
            # 获取连续两天的数据
            signal_day1 = group.loc[i]
            signal_day2 = group.loc[i+1]
            
            # 获取后续两天的数据
            next_days = [group.loc[i+j] for j in range(2, 4)]  # 从第3天开始，获取2天数据
            
            # 计算后续两天的最高价和收盘价涨跌幅
            high_changes = []
            close_changes = []
            for day in next_days:
                high_change = (day['high'] - day['pre_close']) / day['pre_close'] * 100
                close_change = (day['close'] - day['pre_close']) / day['pre_close'] * 100
                high_changes.append(high_change)
                close_changes.append(close_change)
            
            stock_specific_results.append({
                'ts_code': ts_code,
                'name': signal_day1['name'],            # 股票名称
                'start_date': signal_day1['trade_date'],
                'total_mv': signal_day1['total_mv'],    # 公司市值（亿元）
                'signal_day1_high': signal_day1['high_change'],      # 信号日1最高价涨幅
                'signal_day1_low': signal_day1['low_change'],        # 信号日1最低价涨幅
                'signal_day1_close': signal_day1['close_change'],    # 信号日1收盘价涨幅
                'signal_day1_amount': signal_day1['net_amount'],     # 信号日1主力净流入额（万元）
                'signal_day1_amount_rate': signal_day1['net_amount_rate'],  # 信号日1主力净流入占比
                'signal_day2_high': signal_day2['high_change'],      # 信号日2最高价涨幅
                'signal_day2_low': signal_day2['low_change'],        # 信号日2最低价涨幅
                'signal_day2_close': signal_day2['close_change'],    # 信号日2收盘价涨幅
                'signal_day2_amount': signal_day2['net_amount'],     # 信号日2主力净流入额（万元）
                'signal_day2_amount_rate': signal_day2['net_amount_rate'],  # 信号日2主力净流入占比
                'next_day1_high': high_changes[0],     # 后续第一天最高价涨幅
                'next_day1_close': close_changes[0],   # 后续第一天收盘价涨幅
                'next_day2_high': high_changes[1],     # 后续第二天最高价涨幅
                'next_day2_close': close_changes[1]    # 后续第二天收盘价涨幅
            })
    
    # 转换为DataFrame便于统计
    results_df = pd.DataFrame(stock_specific_results)
    
    # 重命名列以便更好理解
    column_renames = {
        'ts_code': '股票代码',
        'name': '股票名称',
        'start_date': '开始日期',
        'total_mv': '总市值(亿)',
        'signal_day1_high': '信号日1最高价涨幅',
        'signal_day1_low': '信号日1最低价涨幅',
        'signal_day1_close': '信号日1收盘价涨幅',
        'signal_day1_amount': '信号日1主力净流入(万)',
        'signal_day1_amount_rate': '信号日1主力净占比(%)',
        'signal_day2_high': '信号日2最高价涨幅',
        'signal_day2_low': '信号日2最低价涨幅',
        'signal_day2_close': '信号日2收盘价涨幅',
        'signal_day2_amount': '信号日2主力净流入(万)',
        'signal_day2_amount_rate': '信号日2主力净占比(%)',
        'next_day1_high': '后续第1天最高价涨幅',
        'next_day1_close': '后续第1天收盘价涨幅',
        'next_day2_high': '后续第2天最高价涨幅',
        'next_day2_close': '后续第2天收盘价涨幅'
    }
    
    results_df = results_df.rename(columns=column_renames)
    
    # 设置列的顺序
    column_order = [
        '股票代码', '股票名称', '开始日期', '总市值(亿)',
        '信号日1最高价涨幅', '信号日1最低价涨幅', '信号日1收盘价涨幅', '信号日1主力净流入(万)', '信号日1主力净占比(%)',
        '信号日2最高价涨幅', '信号日2最低价涨幅', '信号日2收盘价涨幅', '信号日2主力净流入(万)', '信号日2主力净占比(%)',
        '后续第1天最高价涨幅', '后续第1天收盘价涨幅',
        '后续第2天最高价涨幅', '后续第2天收盘价涨幅'
    ]
    
    # 按日期降序排序
    results_df = results_df.sort_values('开始日期', ascending=False)
    
    # 按指定列顺序重排列
    results_df = results_df[column_order]
    
    # 保存为CSV文件
    output_file = 'signal_analysis.csv'
    results_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n个股明细数据已保存至: {output_file}")
    
    return results_df

if __name__ == "__main__":
    df = get_data()
    results = analyze_rebound(df)
    if not results.empty:
        print("\n=== 最近的几条个股明细数据 ===")
        print(results.head()) 