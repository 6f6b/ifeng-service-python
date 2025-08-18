import pandas as pd
import pymysql
from pymysql.cursors import DictCursor
import numpy as np

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
        SELECT * FROM price_trend_analysis
        """
        with conn.cursor() as cursor:
            cursor.execute(sql)
            results = cursor.fetchall()
            
        # 转换为DataFrame
        df = pd.DataFrame(results)
        
        # 转换数值列的数据类型
        numeric_columns = [
            '信号日最高价涨幅', '信号日收盘价涨幅',
            '后续第1天最高价涨幅', '后续第1天收盘价涨幅',
            '后续第2天最高价涨幅', '后续第2天收盘价涨幅',
            '后续第3天最高价涨幅', '后续第3天收盘价涨幅'
        ]
        
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        return df
    except Exception as e:
        print(f"数据库查询错误: {str(e)}")
        return pd.DataFrame()  # 返回空DataFrame
    finally:
        conn.close()

def analyze_conditions(df, target_high_pct=1.0, target_close_pct=0.5):
    # 检查数据是否为空
    if df.empty:
        print("错误：没有读取到数据！")
        return None
        
    # 检查数据类型
    print("\n数据类型检查:")
    print(df.dtypes)
    
    # 检查数据范围
    print("\n数据范围检查:")
    numeric_columns = [
        '信号日最高价涨幅', '信号日收盘价涨幅',
        '后续第2天最高价涨幅', '后续第2天收盘价涨幅'
    ]
    for col in numeric_columns:
        print(f"\n{col}:")
        print(f"  最小值: {df[col].min():.2f}")
        print(f"  最大值: {df[col].max():.2f}")
        print(f"  平均值: {df[col].mean():.2f}")
        print(f"  空值数量: {df[col].isna().sum()}")
    
    # 定义要测试的条件范围
    high_ranges = np.arange(-3, 5, 0.5)  # 信号日最高价涨幅范围
    close_ranges = np.arange(-3, 3, 0.5)  # 信号日收盘价涨幅范围
    
    results = []
    
    # 遍历所有可能的条件组合
    for high_threshold in high_ranges:
        for close_threshold in close_ranges:
            # 应用条件筛选
            condition_high = df['信号日最高价涨幅'] > high_threshold
            condition_close = df['信号日收盘价涨幅'] < close_threshold
            
            filtered_df = df[condition_high & condition_close]
            
            if len(filtered_df) >= 50:  # 只考虑样本量足够的情况
                # 计算满足目标条件的概率
                success_condition = (filtered_df['后续第2天最高价涨幅'] > target_high_pct) & \
                                 (filtered_df['后续第2天收盘价涨幅'] > target_close_pct)
                success_rate = success_condition.mean() * 100
                
                results.append({
                    '信号日最高价涨幅阈值': high_threshold,
                    '信号日收盘价涨幅阈值': close_threshold,
                    '样本数量': len(filtered_df),
                    '成功概率': success_rate,
                    '第2天最高价平均涨幅': filtered_df['后续第2天最高价涨幅'].mean(),
                    '第2天收盘价平均涨幅': filtered_df['后续第2天收盘价涨幅'].mean()
                })
    
    if not results:
        print("警告：没有找到满足条件的组合！")
        return None
    
    # 转换为DataFrame并按成功概率排序
    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values('成功概率', ascending=False)
    
    # 保存结果
    results_df.to_csv('optimal_conditions_analysis.csv', index=False, encoding='utf-8-sig')
    
    # 打印前10个最优组合
    print("\n=== 最优信号组合（按成功概率排序）===")
    print("\n前10个最优组合：")
    print(results_df.head(10).to_string(index=False))
    
    # 打印最优组合的详细统计
    best_combination = results_df.iloc[0]
    print("\n=== 最优组合详细统计 ===")
    print(f"信号日条件:")
    print(f"  最高价涨幅 > {best_combination['信号日最高价涨幅阈值']:.1f}%")
    print(f"  收盘价涨幅 < {best_combination['信号日收盘价涨幅阈值']:.1f}%")
    print(f"样本数量: {best_combination['样本数量']:.0f}")
    print(f"成功概率: {best_combination['成功概率']:.2f}%")
    print(f"第2天平均表现:")
    print(f"  最高价平均涨幅: {best_combination['第2天最高价平均涨幅']:.2f}%")
    print(f"  收盘价平均涨幅: {best_combination['第2天收盘价平均涨幅']:.2f}%")
    
    return results_df

if __name__ == "__main__":
    print("开始分析最优信号组合...")
    print("目标条件：后续第2天最高价涨幅>1.0% 且 收盘价涨幅>0.5%")
    
    df = get_data()
    
    # 打印数据基本信息
    print("\n数据基本信息:")
    print(f"总样本数: {len(df)}")
    
    results = analyze_conditions(df)
    
    if results is not None:
        print("\n分析完成！详细结果已保存至 optimal_conditions_analysis.csv") 