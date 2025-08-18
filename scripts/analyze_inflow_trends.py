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
            m.ts_code, m.trade_date, m.net_amount, m.net_amount_rate,
            m.buy_elg_amount, m.buy_elg_amount_rate,
            m.buy_lg_amount, m.buy_lg_amount_rate,
            m.buy_md_amount, m.buy_md_amount_rate,
            m.buy_sm_amount, m.buy_sm_amount_rate,
            f.pre_close,  -- 前一天收盘价
            f.high        -- 当天最高价
        FROM stock_moneyflow_dc m
        JOIN stock_factor_pro f ON m.ts_code = f.ts_code AND m.trade_date = f.trade_date
        ORDER BY m.ts_code, m.trade_date limit 1000000
        """
        
        # 使用cursor执行查询
        with conn.cursor() as cursor:
            cursor.execute(sql)
            results = cursor.fetchall()
            
        # 转换为DataFrame
        df = pd.DataFrame(results)
        df['net_amount'] = pd.to_numeric(df['net_amount'], errors='coerce')
        return df
    except Exception as e:
        print(f"发生错误: {str(e)}")
        raise
    finally:
        conn.close()

def analyze_trends(df):
    # 使用net_amount_rate判断资金净流入是否超过市值2%
    df['net_inflow'] = df['net_amount'] > 3000
    # 改为连续3天
    df['consecutive_inflow'] = df['net_inflow'].rolling(window=3).sum() == 3

    all_next_days = []
    stock_specific_results = []
    print(df.head(50))
    
    for ts_code, group in df.groupby('ts_code'):
        group = group.reset_index(drop=True)
        for i in range(len(group) - 7):  # 改为-7以获取连续3天后的5天数据
            if group.loc[i, 'consecutive_inflow']:
                # 获取后续五天的数据（从第4天开始）
                next_days = [group.loc[i+j] for j in range(3, 8)]  # 获取第1-5天数据
                
                # 计算每天最高价相对前一天收盘价的涨跌幅
                high_changes = []
                for day in next_days:
                    high_change = (day['high'] - day['pre_close']) / day['pre_close'] * 100
                    high_changes.append(high_change)
                
                all_next_days.append({
                    'day_1_high_change': high_changes[0],
                    'day_2_high_change': high_changes[1],
                    'day_3_high_change': high_changes[2],
                    'day_4_high_change': high_changes[3],
                    'day_5_high_change': high_changes[4]
                })
                
                stock_specific_results.append({
                    'ts_code': ts_code,
                    'start_date': group.loc[i, 'trade_date'],
                    'net_amount_rate_1': group.loc[i, 'net_amount_rate'],      # 第一天净流入率
                    'net_amount_rate_2': group.loc[i+1, 'net_amount_rate'],    # 第二天净流入率
                    'net_amount_rate_3': group.loc[i+2, 'net_amount_rate'],    # 第三天净流入率
                    'day_1_high_change': high_changes[0],
                    'day_2_high_change': high_changes[1],
                    'day_3_high_change': high_changes[2],
                    'day_4_high_change': high_changes[3],
                    'day_5_high_change': high_changes[4]
                })
    
    # 转换为DataFrame便于统计
    df_next_days = pd.DataFrame(all_next_days)
    
    # 计算整体统计情况
    total_cases = len(df_next_days)
    if total_cases > 0:
        # 计算每天的上涨概率
        up_probs = {}
        avg_changes = {}
        for day in range(1, 6):
            col = f'day_{day}_high_change'
            up_probs[f'第{day}天最高价上涨概率'] = (df_next_days[col] > 1.0).mean() * 100
            avg_changes[f'第{day}天最高价平均涨幅'] = df_next_days[col].mean()
        
        print("\n=== 连续三天资金净流入后的最高价涨跌统计 ===")
        print(f"总样本数: {total_cases}")
        for day in range(1, 6):
            print(f"第{day}天最高价上涨概率: {up_probs[f'第{day}天最高价上涨概率']:.2f}%")
        
        print("\n=== 最高价涨跌幅统计 ===")
        for day in range(1, 6):
            print(f"第{day}天最高价平均涨幅: {avg_changes[f'第{day}天最高价平均涨幅']:.2f}%")
        
        # 返回个股明细数据
        results_df = pd.DataFrame(stock_specific_results)
        # 按日期降序排序
        results_df = results_df.sort_values('start_date', ascending=False)
        # 保存为CSV文件
        output_file = 'stock_analysis_results.csv'
        results_df.to_csv(output_file, index=False)
        print(f"\n个股明细数据已保存至: {output_file}")
        return results_df
    else:
        print("没有找到符合条件的数据")
        return pd.DataFrame()

if __name__ == "__main__":
    df = get_data()
    results = analyze_trends(df)
    if not results.empty:
        print("\n=== 最近的几条个股明细数据 ===")
        print(results.head()) 