import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import pymysql
from pymysql.cursors import DictCursor
import hashlib
from sklearn.metrics import roc_auc_score

# 数据库配置
DB_CONFIG = {
    'host': 'rds.6f6b.cn',
    'port': 3306,
    'user': 'root',
    'password': 'FuckTheHaker@666',
    'database': 'stock',
    'charset': 'utf8mb4'
}

def encode_stock_code(code):
    """将股票代码转换为固定的整数（1-1亿之间）"""
    # 使用md5生成哈希值
    hash_object = hashlib.md5(code.encode())
    # 取前8位转换为16进制数
    hex_dig = hash_object.hexdigest()[:8]
    # 转换为1-1亿之间的整数
    num = int(hex_dig, 16) % 100000000 + 1  # +1 确保不会出现0
    return num

def get_training_data(stock_codes=['600519.SH'], start_date='20250201', end_date='20250801'):
    """获取训练数据"""
    conn = pymysql.connect(**DB_CONFIG, cursorclass=DictCursor)
    try:
        # SQL查询，合并日线数据和资金流数据
        sql = """
        SELECT 
            d.trade_date,
            d.ts_code,
            d.pct_chg,
            d.vol,
            d.amount,
            -- 计算各类资金净流入（买入金额 - 卖出金额）
            (m.buy_sm_amount - m.sell_sm_amount) as small_net,
            (m.buy_md_amount - m.sell_md_amount) as medium_net,
            (m.buy_lg_amount - m.sell_lg_amount) as large_net,
            (m.buy_elg_amount - m.sell_elg_amount) as super_large_net,
            m.net_mf_amount as total_net
        FROM stock_daily d
        LEFT JOIN stock_moneyflow m ON d.ts_code = m.ts_code AND d.trade_date = m.trade_date
        WHERE d.ts_code IN ({})
        AND d.trade_date BETWEEN %s AND %s
        ORDER BY d.ts_code, d.trade_date
        """.format(','.join(['%s'] * len(stock_codes)))
        
        print("\n=== SQL查询 ===")
        print("SQL语句:", sql)
        params = stock_codes + [start_date, end_date]
        print("参数:", params)
        print("完整SQL:", sql.replace("%s", "'{}'").format(*params))
        print("============\n")
        
        # 使用cursor直接执行查询
        cursor = conn.cursor()
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        
        # 获取列名
        columns = [desc[0] for desc in cursor.description]
        
        print("\n=== 原始数据验证 ===")
        print("列名:", columns)
        print("前几行数据:")
        for row in list(rows)[:5]:
            print(row)
        print("=================\n")
        
        # 使用获取的数据创建DataFrame
        df = pd.DataFrame(rows, columns=columns)
        
        # 转换数值型列
        numeric_cols = ['pct_chg', 'vol', 'amount', 'small_net', 'medium_net', 
                       'large_net', 'super_large_net', 'total_net']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        print("\n=== DataFrame验证 ===")
        print("DataFrame 形状:", df.shape)
        print("列名:", df.columns.tolist())
        print("数据类型:\n", df.dtypes)
        print("前几行数据:\n", df.head())
        print("=================\n")
        
        # 将trade_date转换为datetime
        print("\n=== 日期转换验证 ===")
        print("转换前 trade_date 类型:", df['trade_date'].dtype)
        print("转换前样例:\n", df['trade_date'].head())
        
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        
        print("\n转换后 trade_date 类型:", df['trade_date'].dtype)
        print("转换后样例:\n", df['trade_date'].head())
        print("=================\n")
        
        df['weekday'] = df['trade_date'].dt.weekday.astype('float64')  # 转换为float64
        
        # 使用哈希编码股票代码
        df['stock_code_encoded'] = df['ts_code'].apply(encode_stock_code).astype('float64')
        
        print("\n===加入weekday和股票代码编码后 DataFrame验证 ===")
        print("DataFrame 形状:", df.shape)
        print("列名:", df.columns.tolist())
        print("数据类型:\n", df.dtypes)
        print("股票代码映射示例:")
        # 打印所有唯一股票代码的映射
        stock_mappings = {code: encode_stock_code(code) for code in sorted(df['ts_code'].unique())}
        for code, encoded in stock_mappings.items():
            print(f"{code}: {encoded:,}")  # 使用千位分隔符格式化数字
        print("前几行数据:\n", df.head(20))
        print("=================\n")

        # ===== 生成特征 =====
        features = []
        
        # 添加基础特征
        features.extend(['weekday', 'stock_code_encoded'])
        
        # 生成滞后特征（过去N天的数据）
        lag_cols = [
            'pct_chg', 'small_net', 'medium_net', 'large_net', 'super_large_net', 
            'total_net', 'vol', 'amount'
        ]
        
        for lag in range(1, 6):  # 最近1~5天
            for col in lag_cols:
                df[f'{col}_lag{lag}'] = df[col].shift(lag)
                features.append(f'{col}_lag{lag}')
        
        # 目标变量：下一天的涨跌幅
        df['target'] = df['pct_chg'].shift(-1)
        
        # 删除NaN值
        df = df.dropna().reset_index(drop=True)
        
        # 确保所有特征列都是float64类型
        for col in features:
            if df[col].dtype != 'float64':
                print(f"转换列 {col} 为float64类型")
                df[col] = df[col].astype('float64')
        
        print("\n=== 特征类型验证 ===")
        print("特征列表:", features)
        print("特征数量:", len(features))
        for col in features:
            print(f"{col}: {df[col].dtype}")
        print("=================\n")
        
        # 打印涨跌幅对照表
        print("\n=== 涨跌幅对照表 ===")
        print("股票代码:", stock_codes[0])
        comparison_df = pd.DataFrame({
            '日期': df['trade_date'].dt.strftime('%Y-%m-%d'),
            '星期': df['trade_date'].dt.strftime('%A'),
            '当日涨跌幅(%)': df['pct_chg'].round(4),
            '次日涨跌幅(%)': df['target'].round(4),
            '当日成交量': df['vol'].round(2),
            '当日成交额(万)': (df['amount']/10000).round(2),
            '主力净流入(万)': (df['total_net']/10000).round(2)
        })
        print(comparison_df.head(20).to_string(index=False))
        print("=================\n")

        return df, features

    finally:
        conn.close()

if __name__ == "__main__":
    # 获取训练数据
    stock_codes = ['600519.SH']  # 定义要分析的股票代码
    df, features = get_training_data(stock_codes,'20230201','20250301')
    
    X = df[features]
    
    # 将涨跌幅转换为涨跌标签（1表示涨，0表示跌）
    y = (df['target'] > 0).astype(int)
    
    # 使用时间顺序划分，保留最后20%作为测试集
    split_idx = int(len(df) * 0.9)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]
    
    # LightGBM参数 - 改为二分类
    params = {
        'objective': 'binary',  # 改为二分类
        'metric': 'auc',        # 使用AUC评估
        'boosting_type': 'gbdt',
        'learning_rate': 0.05,
        'num_leaves': 31,
        'verbose': -1
    }
    
    # 训练模型
    train_data = lgb.Dataset(X_train, label=y_train)
    test_data = lgb.Dataset(X_test, label=y_test)
    
    callbacks = [
        lgb.early_stopping(stopping_rounds=50),
        lgb.log_evaluation(period=-1)
    ]
    
    model = lgb.train(
        params=params,
        train_set=train_data,
        num_boost_round=500,
        valid_sets=[train_data, test_data],
        callbacks=callbacks
    )
    
    # 模型评估
    y_pred_proba = model.predict(X_test)
    
    print("\n=== 模型性能分析 ===")
    print(f"AUC分数: {roc_auc_score(y_test, y_pred_proba):.4f}")
    print("解读: ")
    print("- AUC = 0.5 表示与随机猜测相当")
    print("- AUC > 0.5 表示模型有预测能力")
    print("- 0.5-0.6: 效果较差")
    print("- 0.6-0.7: 效果一般")
    print("- 0.7-0.8: 效果好")
    print("- 0.8-0.9: 效果很好")
    print("- 0.9-1.0: 效果非常好")
    
    # 计算不同概率阈值下的表现
    print("\n不同把握度下的预测效果:")
    print("把握度  准确率  覆盖率  上涨概率  平均涨幅  交易次数")
    print("-" * 50)
    
    for threshold in [0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8]:
        # 高置信度预测
        high_conf_mask = y_pred_proba >= threshold
        if sum(high_conf_mask) > 0:
            pred_accuracy = (y_test[high_conf_mask] == 1).mean() * 100
            coverage = sum(high_conf_mask) / len(y_test) * 100
            avg_return = df.loc[X_test.index[high_conf_mask], 'target'].mean()
            
            print(f"≥{threshold*100:3.0f}%  {pred_accuracy:6.2f}%  {coverage:6.2f}%  {pred_accuracy:6.2f}%  {avg_return:8.2f}%  {sum(high_conf_mask):6d}")
    
    print("\n解读:")
    print("- 把握度：模型预测的上涨概率")
    print("- 准确率：预测正确的比例")
    print("- 覆盖率：满足该把握度的样本比例")
    print("- 上涨概率：实际上涨的概率")
    print("- 平均涨幅：第二天的平均涨跌幅")
    print("- 交易次数：满足该把握度的样本数量")
    
    # 特征重要性
    importance = pd.DataFrame({
        'feature': features,
        'importance': model.feature_importance()
    })
    importance = importance.sort_values('importance', ascending=False)
    print("\n特征重要性TOP 10:")
    print(importance.head(10))
    
    # 输出预测结果
    test_results = pd.DataFrame({
        '日期': df.loc[X_test.index, 'trade_date'].dt.strftime('%Y-%m-%d'),
        '星期': df.loc[X_test.index, 'trade_date'].dt.strftime('%A'),
        '当日涨跌幅': df.loc[X_test.index, 'pct_chg'].round(2),
        '次日实际涨跌幅': df.loc[X_test.index, 'target'].round(2),
        '上涨概率(%)': (y_pred_proba * 100).round(2),
        '预测结果': ['上涨' if p > 0.5 else '下跌' for p in y_pred_proba],
        '实际结果': ['上涨' if x > 0 else '下跌' for x in df.loc[X_test.index, 'target']],
        '预测正确': ['√' if correct else '×' for correct in (y_test == (y_pred_proba > 0.5))],
        '成交额(万)': (df.loc[X_test.index, 'amount']/10000).round(0),
        '主力净流入(万)': (df.loc[X_test.index, 'total_net']/10000).round(0),
    })

    print("\n=== 预测结果示例 ===")
    print(f"股票代码: {stock_codes[0]}")
    print(f"准确率: {(test_results['预测正确'] == '√').mean() * 100:.2f}%")
    print(f"AUC分数: {roc_auc_score(y_test, y_pred_proba):.4f}")
    
    # 计算不同概率阈值下的准确率
    print("\n不同概率阈值下的预测结果:")
    threshold_results = []
    for threshold in [0.6, 0.7, 0.8, 0.9]:
        high_conf_mask = abs(y_pred_proba - 0.5) > (threshold - 0.5)
        high_conf_pred = test_results[high_conf_mask]
        if len(high_conf_pred) > 0:
            accuracy = (high_conf_pred['预测正确'] == '√').mean() * 100
            coverage = (len(high_conf_pred) / len(test_results) * 100)
            threshold_results.append({
                '概率阈值': f"{threshold*100:.0f}%",
                '准确率': f"{accuracy:.2f}%",
                '覆盖率': f"{coverage:.2f}%",
                '信号数量': len(high_conf_pred)
            })
    
    if threshold_results:
        threshold_df = pd.DataFrame(threshold_results)
        print("\n高置信度预测结果统计:")
        print(threshold_df.to_string(index=False))
    
    # 保存完整结果到CSV
    test_results.to_csv(f"{stock_codes[0]}_prediction_results.csv", index=False, encoding='utf-8-sig')
    print(f"\n预测结果已保存到: {stock_codes[0]}_prediction_results.csv")
    
    # 显示最近的预测结果
    print("\n最近20个交易日的预测结果:")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', None)
    print(test_results.tail(20).to_string(index=False))
    
    # 保存模型
    model.save_model("fund_flow_model.txt")
    print("\n模型已保存为 fund_flow_model.txt")
    
    # 输出预测正确率随时间的变化
    monthly_accuracy = test_results.set_index(pd.to_datetime(test_results['日期']))['预测正确'].resample('M').agg(
        预测数量=lambda x: len(x),
        正确数量=lambda x: (x == '√').sum(),
        准确率=lambda x: (x == '√').mean() * 100
    ).round(2)
    
    print("\n按月份统计预测准确率:")
    print(monthly_accuracy.to_string())
