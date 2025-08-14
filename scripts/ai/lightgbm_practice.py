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
        # SQL查询，合并 stock_factor_pro 和 stock_moneyflow_dc
        sql = """
        SELECT 
            f.trade_date,
            f.open, f.high, f.low, f.close, f.pre_close, f.change, f.pct_chg,
            f.vol, f.amount, f.turnover_rate, f.volume_ratio, f.vr,
            f.macd_dif, f.macd_dea, f.macd,
            f.kdj_k, f.kdj_d, f.kdj_j,
            f.rsi_6, f.rsi_12, f.rsi_24,
            f.boll_upper, f.boll_mid, f.boll_lower,
            f.ma_5, f.ma_10, f.ma_20, f.ma_30, f.ma_60,
            f.bias1, f.bias2, f.bias3,
            f.cci,
            f.dmi_pdi, f.dmi_mdi, f.dmi_adx, f.dmi_adxr,
            f.updays, f.downdays,
            m.net_amount, m.net_amount_rate, m.buy_elg_amount, m.buy_elg_amount_rate,
            m.buy_lg_amount, m.buy_lg_amount_rate
        FROM stock_factor_pro f
        LEFT JOIN stock_moneyflow_dc m ON f.ts_code = m.ts_code AND f.trade_date = m.trade_date
        WHERE f.ts_code IN ({})
        AND f.trade_date BETWEEN %s AND %s
        ORDER BY f.ts_code, f.trade_date
        """.format(','.join(['%s'] * len(stock_codes)))
        
        params = stock_codes + [start_date, end_date]
        cursor = conn.cursor()
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        
        # 获取列名
        columns = [desc[0] for desc in cursor.description]
        
        # 创建DataFrame
        df = pd.DataFrame(rows, columns=columns)
        print(df.head())
        # 转换数值型列
        numeric_cols = columns[1:]  # 除去 trade_date
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 将trade_date转换为datetime
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        
        return df
    finally:
        conn.close()

def generate_targets(df):
    """生成目标变量"""
    df['target_1'] = df['pct_chg'].shift(-1)
    df['target_2'] = df['pct_chg'].shift(-2)
    df['target_1_cls'] = (df['target_1'] > 0).astype(int)
    df['target_2_cls'] = (df['target_2'] > 0).astype(int)
    return df.dropna().reset_index(drop=True)

# 更新主函数
if __name__ == "__main__":
    # 获取训练数据
    stock_codes = ['600519.SH']  # 定义要分析的股票代码
    df = get_training_data(stock_codes, '20250201', '20250801')
    
    # 生成目标变量
    df = generate_targets(df)
    
    # 特征和目标
    features = df.columns.difference(['trade_date', 'target_1', 'target_2', 'target_1_cls', 'target_2_cls'])
    X = df[features]
    y1, y2 = df['target_1'], df['target_2']
    y1_cls, y2_cls = df['target_1_cls'], df['target_2_cls']
    
    # 使用时间顺序划分，保留最后20%作为测试集
    split_idx = int(len(df) * 0.8)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y1_train, y1_test = y1[:split_idx], y1[split_idx:]
    y2_train, y2_test = y2[:split_idx], y2[split_idx:]
    y1_cls_train, y1_cls_test = y1_cls[:split_idx], y1_cls[split_idx:]
    y2_cls_train, y2_cls_test = y2_cls[:split_idx], y2_cls[split_idx:]
    
    # LightGBM参数
    params_reg = {
        'objective': 'regression',
        'metric': 'rmse',
        'boosting_type': 'gbdt',
        'learning_rate': 0.05,
        'num_leaves': 31,
        'verbose': -1
    }
    params_cls = {
        'objective': 'binary',
        'metric': 'auc',
        'boosting_type': 'gbdt',
        'learning_rate': 0.05,
        'num_leaves': 31,
        'verbose': -1
    }
    print("X_train shape:", X_train.shape)
    print("X_train head:\n", X_train.head())
    print("y1_train shape:", y1_train.shape)
    print("y1_train head:\n", y1_train.head())
    # 训练回归模型
    train_data_1 = lgb.Dataset(X_train, label=y1_train)
    train_data_2 = lgb.Dataset(X_train, label=y2_train)
    model_1 = lgb.train(params_reg, train_data_1, num_boost_round=500)
    model_2 = lgb.train(params_reg, train_data_2, num_boost_round=500)
    
    # 训练分类模型
    train_data_1_cls = lgb.Dataset(X_train, label=y1_cls_train)
    train_data_2_cls = lgb.Dataset(X_train, label=y2_cls_train)
    model_1_cls = lgb.train(params_cls, train_data_1_cls, num_boost_round=500)
    model_2_cls = lgb.train(params_cls, train_data_2_cls, num_boost_round=500)
    
    # 预测
    y1_pred = model_1.predict(X_test)
    y2_pred = model_2.predict(X_test)
    y1_cls_pred = model_1_cls.predict(X_test)
    y2_cls_pred = model_2_cls.predict(X_test)
    
    # 输出预测结果
    print("\n=== 预测结果 ===")
    print("T+1 涨跌幅预测:", y1_pred)
    print("T+2 涨跌幅预测:", y2_pred)
    print("T+1 上涨概率:", y1_cls_pred)
    print("T+2 上涨概率:", y2_cls_pred)
    
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
            avg_return = df.loc[X_test.index[high_conf_mask], 'pct_chg'].mean()
            
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
        'feature': X_train.columns.tolist(),
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
        '次日实际涨跌幅': df.loc[X_test.index, 'pct_chg'].round(2), # 实际涨跌幅就是当日涨跌幅
        '上涨概率(%)': (y_pred_proba * 100).round(2),
        '预测结果': ['上涨' if p > 0.5 else '下跌' for p in y_pred_proba],
        '实际结果': ['上涨' if x > 0 else '下跌' for x in y_test],
        '预测正确': ['√' if correct else '×' for correct in (y_test == (y_pred_proba > 0.5))],
        '成交额(万)': (df.loc[X_test.index, 'amount']/10000).round(0),
        '主力净流入(万)': (df.loc[X_test.index, 'net_amount']/10000).round(0),
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
