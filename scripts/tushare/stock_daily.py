import tushare as ts
import pandas as pd
import time

# 设置token
ts.set_token('gx03013e909f633ecb66722df66b360f070426613316ebf06ecd3482')
pro = ts.pro_api()

# 读取股票基础信息
basic_df = pd.read_excel('stock_basic_info.xlsx')

# 获取所有公司的日线数据
df_list = []
total = len(basic_df)
for index, row in basic_df.iterrows():
    try:
        print(f"正在处理 {row['ts_code']} ({index + 1}/{total})")
        df_temp = pro.daily(
            ts_code=row['ts_code'],
            fields='ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount'
        )
        if not df_temp.empty:
            df_list.append(df_temp)
        # 避免频繁调用接口
        time.sleep(0.3)  # 暂停0.3秒
    except Exception as e:
        print(f"处理 {row['ts_code']} 时出错: {str(e)}")
        continue

# 合并所有数据
if df_list:
    df = pd.concat(df_list, ignore_index=True)
    print(f"\n总数据量: {len(df)} 行")

    # 数据类型映射
    dtype_mapping = {
        'object': 'VARCHAR(255)',
        'int64': 'INT',
        'float64': 'DECIMAL(20,4)',
        'datetime64[ns]': 'DATETIME'
    }

    # 字段注释映射
    field_comments = {
        'ts_code': '股票代码',
        'trade_date': '交易日期',
        'open': '开盘价',
        'high': '最高价',
        'low': '最低价',
        'close': '收盘价',
        'pre_close': '昨收价',
        'change': '涨跌额',
        'pct_chg': '涨跌幅',
        'vol': '成交量（手）',
        'amount': '成交额（千元）'
    }

    # 生成建表SQL
    table_name = 'stock_daily'
    columns = []
    for column, dtype in df.dtypes.items():
        sql_type = dtype_mapping.get(str(dtype), 'VARCHAR(255)')
        comment = field_comments.get(column, '')
        columns.append(f"    {column} {sql_type} COMMENT '{comment}'")

    # 添加DROP语句和表注释
    create_table_sql = f"""
    DROP TABLE IF EXISTS {table_name};

    CREATE TABLE IF NOT EXISTS {table_name} (
    {',\n'.join(columns)},
        PRIMARY KEY (ts_code, trade_date),
        INDEX idx_trade_date (trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '股票日线行情表';
    """

    print("\n创建表SQL语句：")
    print(create_table_sql)

    # 生成INSERT语句，使用批量插入以提高效率
    print("\n开始生成INSERT语句...")
    batch_size = 1000  # 每1000条数据一批
    insert_template = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES"
    
    with open('stock_daily.sql', 'w', encoding='utf-8') as f:
        # 写入建表语句
        f.write(create_table_sql)
        f.write('\n\n')
        
        # 分批写入INSERT语句
        total_batches = (len(df) + batch_size - 1) // batch_size
        for i in range(0, len(df), batch_size):
            batch_num = i // batch_size + 1
            print(f"\r正在写入第 {batch_num}/{total_batches} 批数据...", end='')
            
            batch_df = df.iloc[i:i+batch_size]
            insert_values = []
            
            for _, row in batch_df.iterrows():
                values = []
                for val in row:
                    if pd.isna(val):
                        values.append('NULL')
                    elif isinstance(val, (int, float)):
                        values.append(str(val))
                    else:
                        values.append(f"'{str(val)}'")
                insert_values.append(f"({', '.join(values)})")
            
            insert_sql = insert_template + '\n' + ',\n'.join(insert_values) + ';\n'
            f.write(insert_sql)

    print("\n\nSQL文件生成完成！")
else:
    print("没有获取到任何数据") 