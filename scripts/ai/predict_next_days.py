import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score, roc_auc_score
import pymysql
from pymysql.cursors import DictCursor
from datetime import datetime, timedelta
import warnings
import os
warnings.filterwarnings('ignore')

# 数据库配置
DB_CONFIG = {
    'host': 'rds.6f6b.cn',
    'port': 3306,
    'user': 'root',
    'password': 'FuckTheHaker@666',
    'database': 'stock',
    'charset': 'utf8mb4'
}



def get_training_data(start_date='20240101', end_date='20250131'):
    """获取所有股票的训练数据"""
    conn = pymysql.connect(**DB_CONFIG, cursorclass=DictCursor)
    try:
        print(f"获取训练数据范围: {start_date} 到 {end_date}")
        
        # SQL查询，合并日线数据和资金流数据（获取所有股票）
        sql = """
        SELECT 
            d.trade_date,
            d.ts_code,
            d.pct_chg,
            d.vol,
            d.amount,
            (m.buy_sm_amount - m.sell_sm_amount) as small_net,
            (m.buy_md_amount - m.sell_md_amount) as medium_net,
            (m.buy_lg_amount - m.sell_lg_amount) as large_net,
            (m.buy_elg_amount - m.sell_elg_amount) as super_large_net,
            m.net_mf_amount as total_net
        FROM stock_daily d
        LEFT JOIN stock_moneyflow m ON d.ts_code = m.ts_code AND d.trade_date = m.trade_date
        WHERE d.trade_date BETWEEN %s AND %s
        ORDER BY d.ts_code, d.trade_date
        """
        
        params = [start_date, end_date]
        cursor = conn.cursor()
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        
        # 获取列名
        columns = [desc[0] for desc in cursor.description]
        
        # 创建DataFrame
        df = pd.DataFrame(rows, columns=columns)
        
        # 转换数值型列
        numeric_cols = ['pct_chg', 'vol', 'amount', 'small_net', 'medium_net', 
                       'large_net', 'super_large_net', 'total_net']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 将trade_date转换为datetime
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df['weekday'] = df['trade_date'].dt.weekday.astype('float64')
        
        # 生成特征
        features = ['weekday']
        
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
                df[col] = df[col].astype('float64')
        
        print(f"获取到 {len(df)} 条训练数据")
        print(f"特征数量: {len(features)}")
        
        # 获取数据中的最新日期
        latest_date = df['trade_date'].max().date()
        
        return df, features, latest_date

    finally:
        conn.close()

def get_backtest_data(stock_code, start_date='20250201', end_date='20250228'):
    """获取特定股票的回测数据"""
    conn = pymysql.connect(**DB_CONFIG, cursorclass=DictCursor)
    try:
        print(f"获取回测数据 - 股票: {stock_code}, 时间范围: {start_date} 到 {end_date}")
        
        # SQL查询，合并日线数据和资金流数据
        sql = """
        SELECT 
            d.trade_date,
            d.ts_code,
            d.pct_chg,
            d.vol,
            d.amount,
            d.high,
            d.low,
            d.close,
            d.pre_close,
            COALESCE((m.buy_sm_amount - m.sell_sm_amount), 0) as small_net,
            COALESCE((m.buy_md_amount - m.sell_md_amount), 0) as medium_net,
            COALESCE((m.buy_lg_amount - m.sell_lg_amount), 0) as large_net,
            COALESCE((m.buy_elg_amount - m.sell_elg_amount), 0) as super_large_net,
            COALESCE(m.net_mf_amount, 0) as total_net
        FROM stock_daily d
        LEFT JOIN stock_moneyflow m ON d.ts_code = m.ts_code AND d.trade_date = m.trade_date
        WHERE d.ts_code = %s
        AND d.trade_date BETWEEN %s AND %s
        ORDER BY d.trade_date
        """
        
        params = [stock_code, start_date, end_date]
        cursor = conn.cursor()
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        
        # 获取列名
        columns = [desc[0] for desc in cursor.description]
        
        # 创建DataFrame
        df = pd.DataFrame(rows, columns=columns)
        
        # 转换数值型列
        numeric_cols = ['pct_chg', 'vol', 'amount', 'high', 'low', 'close', 'pre_close', 'small_net', 'medium_net', 
                       'large_net', 'super_large_net', 'total_net']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 将trade_date转换为datetime
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df['weekday'] = df['trade_date'].dt.weekday.astype('float64')
        
        # 生成特征
        features = ['weekday']
        
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
                df[col] = df[col].astype('float64')
        
        print(f"获取到 {len(df)} 条回测数据")
        print(f"特征数量: {len(features)}")
        
        # 显示数据的时间范围
        if not df.empty:
            print(f"数据时间范围: {df['trade_date'].min().strftime('%Y-%m-%d')} 到 {df['trade_date'].max().strftime('%Y-%m-%d')}")
            print(f"交易日数量: {len(df)}")
            # 显示前几个交易日
            print("前10个交易日:")
            for i, date in enumerate(df['trade_date'].head(10)):
                print(f"  {i+1}. {date.strftime('%Y-%m-%d')} ({date.strftime('%A')})")
        
        return df, features

    finally:
        conn.close()

def train_model(df, features):
    """训练通用模型"""
    print(f"\n=== 开始训练通用模型 ===")
    
    X = df[features]
    
    # 将涨跌幅转换为涨跌标签（1表示涨，0表示跌）
    y = (df['target'] > 0).astype(int)
    
    print(f"训练数据形状: X={X.shape}, y={y.shape}")
    print(f"上涨样本: {y.sum()}, 下跌样本: {len(y) - y.sum()}")
    
    # 使用固定时间范围划分：2024年1月-2025年1月训练，2025年1月-2月测试
    train_end_date = '20250131'
    test_start_date = '20250201'
    
    # 划分训练集和测试集
    train_mask = df['trade_date'] <= pd.to_datetime(train_end_date)
    test_mask = df['trade_date'] >= pd.to_datetime(test_start_date)
    
    X_train = X[train_mask]
    X_test = X[test_mask]
    y_train = y[train_mask]
    y_test = y[test_mask]
    
    print(f"训练集时间范围: {df[train_mask]['trade_date'].min().strftime('%Y-%m-%d')} 到 {df[train_mask]['trade_date'].max().strftime('%Y-%m-%d')}")
    
    if len(X_test) > 0:
        print(f"测试集时间范围: {df[test_mask]['trade_date'].min().strftime('%Y-%m-%d')} 到 {df[test_mask]['trade_date'].max().strftime('%Y-%m-%d')}")
    else:
        print("测试集为空")
    
    print(f"训练集样本数: {len(X_train)}, 测试集样本数: {len(X_test)}")
    
    # LightGBM参数
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'boosting_type': 'gbdt',
        'learning_rate': 0.05,
        'num_leaves': 31,
        'verbose': -1,
        'random_state': 42
    }
    
    # 训练模型
    train_data = lgb.Dataset(X_train, label=y_train)
    
    if len(X_test) > 0:
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
    else:
        # 如果测试集为空，只使用训练集
        callbacks = [
            lgb.log_evaluation(period=-1)
        ]
        
        model = lgb.train(
            params=params,
            train_set=train_data,
            num_boost_round=500,
            callbacks=callbacks
        )
    
    # 模型评估
    if len(X_test) > 0:
        y_pred_proba = model.predict(X_test)
        auc_score = roc_auc_score(y_test, y_pred_proba)
        
        print(f"\n=== 模型性能 ===")
        print(f"AUC分数: {auc_score:.4f}")
        
        # 计算不同概率阈值下的表现
        print("\n不同把握度下的预测效果:")
        print("把握度  准确率  覆盖率  上涨概率  平均涨幅  交易次数")
        print("-" * 50)
        
        for threshold in [0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8]:
            high_conf_mask = y_pred_proba >= threshold
            if sum(high_conf_mask) > 0:
                pred_accuracy = (y_test[high_conf_mask] == 1).mean() * 100
                coverage = sum(high_conf_mask) / len(y_test) * 100
                avg_return = df.loc[X_test.index[high_conf_mask], 'target'].mean()
                
                print(f"≥{threshold*100:3.0f}%  {pred_accuracy:6.2f}%  {coverage:6.2f}%  {pred_accuracy:6.2f}%  {avg_return:8.2f}%  {sum(high_conf_mask):6d}")
    else:
        print("\n=== 模型性能 ===")
        print("测试集为空，无法进行模型评估")
        auc_score = 0.0
    
    # 特征重要性
    importance = pd.DataFrame({
        'feature': features,
        'importance': model.feature_importance()
    })
    importance = importance.sort_values('importance', ascending=False)
    print("\n特征重要性TOP 10:")
    print(importance.head(10))
    
    return model, auc_score

def predict_next_days(model, df, features, latest_date, stock_code):
    """预测接下来两个交易日的涨跌情况"""
    print(f"\n=== 预测未来交易日涨跌情况 ===")
    print(f"股票代码: {stock_code}")
    print(f"基于最新交易日期: {latest_date}")
    
    # 获取最新的数据用于预测
    latest_data = df.tail(1)  # 获取最新一天的数据
    
    if latest_data.empty:
        print("没有最新的数据用于预测")
        return None
    
    # 准备特征数据
    X_latest = latest_data[features]
    
    # 检查是否有缺失值
    if X_latest.isnull().any().any():
        print("警告：特征数据存在缺失值，将进行插值处理")
        X_latest = X_latest.fillna(method='ffill').fillna(0)
    
    # 进行预测
    predictions = []
    for i in range(2):  # 预测接下来两个交易日
        # 预测上涨概率
        prob_up = model.predict(X_latest)[0]
        prob_down = 1 - prob_up
        
        # 确定预测结果
        if prob_up > 0.5:
            prediction = "上涨"
            confidence = prob_up
        else:
            prediction = "下跌"
            confidence = prob_down
        
        # 计算预测日期（简单加1天）
        if i == 0:
            # 第一个交易日
            next_date = datetime.combine(latest_date, datetime.min.time()) + timedelta(days=1)
        else:
            # 第二个交易日
            next_date = next_date + timedelta(days=1)
        
        predictions.append({
            '预测日期': next_date.strftime('%Y-%m-%d'),
            '星期': next_date.strftime('%A'),
            '预测结果': prediction,
            '上涨概率': f"{prob_up:.2%}",
            '下跌概率': f"{prob_down:.2%}",
            '置信度': f"{confidence:.2%}",
            '把握度': "高" if confidence > 0.7 else "中" if confidence > 0.6 else "低"
        })
    
    # 输出预测结果
    print("\n=== 预测结果 ===")
    for i, pred in enumerate(predictions, 1):
        print(f"第{i}个交易日预测:")
        print(f"  日期: {pred['预测日期']} ({pred['星期']})")
        print(f"  预测结果: {pred['预测结果']}")
        print(f"  上涨概率: {pred['上涨概率']}")
        print(f"  下跌概率: {pred['下跌概率']}")
        print(f"  置信度: {pred['置信度']}")
        print(f"  把握度: {pred['把握度']}")
        print()
    
    # 显示历史数据趋势
    print("\n=== 历史数据趋势 ===")
    recent_data = df.tail(10)[['trade_date', 'pct_chg', 'vol', 'amount', 'total_net']]
    recent_data['trade_date'] = recent_data['trade_date'].dt.strftime('%Y-%m-%d')
    recent_data['pct_chg'] = recent_data['pct_chg'].round(2)
    recent_data['vol'] = (recent_data['vol'] / 10000).round(0)  # 转换为万手
    recent_data['amount'] = (recent_data['amount'] / 10000).round(0)  # 转换为万元
    recent_data['total_net'] = (recent_data['total_net'] / 10000).round(0)  # 转换为万元
    
    print("最近10个交易日数据:")
    print(recent_data.to_string(index=False))
    
    # 计算技术指标
    print("\n=== 技术指标分析 ===")
    latest_pct_chg = df['pct_chg'].iloc[-1]
    latest_vol = df['vol'].iloc[-1]
    latest_amount = df['amount'].iloc[-1]
    latest_net = df['total_net'].iloc[-1]
    
    # 计算移动平均
    ma5 = df['pct_chg'].tail(5).mean()
    ma10 = df['pct_chg'].tail(10).mean()
    
    print(f"最新涨跌幅: {latest_pct_chg:.2f}%")
    print(f"5日平均涨跌幅: {ma5:.2f}%")
    print(f"10日平均涨跌幅: {ma10:.2f}%")
    print(f"最新成交量: {latest_vol/10000:.0f}万手")
    print(f"最新成交额: {latest_amount/10000:.0f}万元")
    print(f"最新主力净流入: {latest_net/10000:.0f}万元")
    
    # 趋势判断
    if latest_pct_chg > ma5 > ma10:
        trend = "强势上涨"
    elif latest_pct_chg < ma5 < ma10:
        trend = "强势下跌"
    elif latest_pct_chg > ma5 and ma5 < ma10:
        trend = "反弹趋势"
    elif latest_pct_chg < ma5 and ma5 > ma10:
        trend = "回调趋势"
    else:
        trend = "震荡整理"
    
    print(f"技术趋势: {trend}")
    
    return predictions

def backtest_model(model, df, features, stock_code):
    """回测模型并按照指定格式输出结果"""
    print(f"\n=== 回测模型 ===")
    print(f"股票代码: {stock_code}")
    
    # 准备回测数据
    X = df[features]
    y = (df['target'] > 0).astype(int)  # 实际涨跌
    
    # 进行预测
    y_pred_proba = model.predict(X)
    
    # 创建回测结果列表
    backtest_results = []
    
    print(f"开始回测，总交易日数: {len(df)}")
    print(f"将进行 {len(df) - 2} 次预测")
    
    # 遍历每个交易日进行预测
    for i in range(len(df) - 2):  # 需要确保有下一天和下下一天的数据
        current_date = df.iloc[i]['trade_date']
        next_date = df.iloc[i + 1]['trade_date']
        actual_change = df.iloc[i + 1]['pct_chg']
        
        # 新的上涨判断逻辑：如果最高价高于前一个交易日的收盘价，也算上涨
        current_close = df.iloc[i]['close']  # 当前交易日收盘价
        next_high = df.iloc[i + 1]['high']   # 下一个交易日最高价
        next_low = df.iloc[i + 1]['low']     # 下一个交易日最低价
        
        # 判断上涨：收盘价上涨 或 最高价高于前一日收盘价
        is_up_by_close = actual_change > 0
        is_up_by_high = next_high > current_close
        actual_direction = "上涨" if (is_up_by_close or is_up_by_high) else "下跌"
        
        # 获取当前特征
        current_features = X.iloc[i:i+1]
        prob_up = model.predict(current_features)[0]
        prob_down = 1 - prob_up
        
        # 确定预测结果
        if prob_up > 0.5:
            prediction = "上涨"
            confidence = prob_up
        else:
            prediction = "下跌"
            confidence = prob_down
        
        # 计算两个交易日的预测
        day1_correct = (prediction == actual_direction)
        
        # 对于第二天，我们需要检查是否有下下一天的数据
        day2_correct = None
        if i + 2 < len(df):
            next_next_date = df.iloc[i + 2]['trade_date']
            next_next_change = df.iloc[i + 2]['pct_chg']
            
            # 第二个交易日的上涨判断逻辑
            next_close = df.iloc[i + 1]['close']  # 第一个交易日收盘价
            next_next_high = df.iloc[i + 2]['high']  # 第二个交易日最高价
            
            is_next_up_by_close = next_next_change > 0
            is_next_up_by_high = next_next_high > next_close
            next_next_direction = "上涨" if (is_next_up_by_close or is_next_up_by_high) else "下跌"
            
            day2_correct = (prediction == next_next_direction)
        
        # 添加结果
        result = {
            '预测日期': current_date.strftime('%Y-%m-%d'),
            '预测结果': prediction,
            '第一个交易日_上涨概率': f"{prob_up:.2%}",
            '第一个交易日_下跌概率': f"{prob_down:.2%}",
            '第一个交易日_置信度': f"{confidence:.2%}",
            '第一个交易日_实际涨跌': actual_direction,
            '第一个交易日_实际涨跌幅': f"{actual_change:.2f}%",
            '第一个交易日_最高价': f"{next_high:.2f}",
            '第一个交易日_最低价': f"{next_low:.2f}",
            '第一个交易日_是否正确': "√" if day1_correct else "×",
            '第二个交易日_上涨概率': f"{prob_up:.2%}",
            '第二个交易日_下跌概率': f"{prob_down:.2%}",
            '第二个交易日_置信度': f"{confidence:.2%}",
            '第二个交易日_实际涨跌': next_next_direction if i + 2 < len(df) else "",
            '第二个交易日_实际涨跌幅': f"{df.iloc[i + 2]['pct_chg']:.2f}%" if i + 2 < len(df) else "",
            '第二个交易日_最高价': f"{df.iloc[i + 2]['high']:.2f}" if i + 2 < len(df) else "",
            '第二个交易日_最低价': f"{df.iloc[i + 2]['low']:.2f}" if i + 2 < len(df) else "",
            '第二个交易日_是否正确': "√" if day2_correct else "×" if day2_correct is not None else ""
        }
        
        backtest_results.append(result)
    
    # 创建DataFrame并保存
    backtest_df = pd.DataFrame(backtest_results)
    
    # 计算准确率
    day1_accuracy = (backtest_df['第一个交易日_是否正确'] == "√").mean() * 100
    day2_accuracy = backtest_df[backtest_df['第二个交易日_是否正确'] != ""]['第二个交易日_是否正确'].apply(lambda x: x == "√").mean() * 100
    
    print(f"\n=== 回测结果 ===")
    print(f"第一个交易日准确率: {day1_accuracy:.2f}%")
    print(f"第二个交易日准确率: {day2_accuracy:.2f}%")
    print(f"总预测次数: {len(backtest_df)}")
    
    # 保存结果
    output_file = f"{stock_code}_backtest_results.csv"
    backtest_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"回测结果已保存到: {output_file}")
    
    # 显示前几行结果
    print("\n前5行回测结果:")
    print(backtest_df.head().to_string(index=False))
    
    return backtest_df

def load_or_train_model():
    """加载现有模型或训练新模型"""
    model_file = "universal_model.txt"
    
    if os.path.exists(model_file):
        print(f"发现现有模型文件: {model_file}")
        try:
            model = lgb.Booster(model_file=model_file)
            print("✅ 成功加载现有模型")
            return model
        except Exception as e:
            print(f"❌ 加载模型失败: {e}")
            print("将重新训练模型...")
    
    print("未发现现有模型，开始训练新模型...")
    
    # 获取训练数据
    df, features, latest_date = get_training_data(start_date='20240101', end_date='20250131')
    
    if df.empty:
        print("❌ 没有获取到训练数据")
        return None
    
    # 训练模型
    model, auc_score = train_model(df, features)
    
    # 保存模型
    model.save_model(model_file)
    print(f"✅ 新模型已保存到: {model_file}")
    
    return model

def main():
    """主函数"""
    print("=== 股票涨跌预测系统 ===")
    print("功能：使用所有股票数据训练通用模型，然后对特定股票进行回测")
    
    try:
        # 1. 加载或训练通用模型
        print("1. 检查并加载/训练通用模型...")
        model = load_or_train_model()
        
        if model is None:
            print("❌ 无法获取模型")
            return
        
        # 2. 对特定股票进行回测
        stock_code = '600519.SH'  # 贵州茅台
        print(f"\n2. 对股票 {stock_code} 进行回测...")
        
        backtest_df, backtest_features = get_backtest_data(stock_code, start_date='20250201', end_date='20250331')
        
        if not backtest_df.empty:
            # 进行回测
            backtest_results = backtest_model(model, backtest_df, backtest_features, stock_code)
            print(f"✅ {stock_code} 回测完成")
        else:
            print(f"❌ {stock_code} 没有回测数据")
            
    except Exception as e:
        print(f"❌ 处理出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 