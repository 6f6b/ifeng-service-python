import tushare as ts
import pandas as pd

# 设置token
ts.set_token('gx03013e909f633ecb66722df66b360f070426613316ebf06ecd3482')
pro = ts.pro_api()

# 获取指定日期范围的盘前数据
df = pro.stk_premarket(trade_date='20250315')

# 打印结果
print(df)

# 保存到Excel文件
df.to_excel('stock_premarket_info.xlsx', index=False)

# 数据类型映射
dtype_mapping = {
    'object': 'VARCHAR(255)',
    'int64': 'INT',
    'float64': 'DECIMAL(20,4)',
    'datetime64[ns]': 'DATETIME'
}

# 字段注释映射
field_comments = {
    'trade_date': '交易日期',
    'ts_code': 'TS股票代码',
    'total_share': '总股本（万股）',
    'float_share': '流通股本（万股）',
    'pre_close': '昨日收盘价',
    'up_limit': '今日涨停价',
    'down_limit': '今日跌停价'
}

# 生成建表SQL
table_name = 'stock_premarket'
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
    PRIMARY KEY (trade_date, ts_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '股票盘前数据表';
"""

# 生成INSERT语句
insert_template = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES"
insert_values = []
for _, row in df.iterrows():
    values = []
    for val in row:
        if pd.isna(val):
            values.append('NULL')
        elif isinstance(val, (int, float)):
            values.append(str(val))
        else:
            values.append(f"'{str(val)}'")
    insert_values.append(f"({', '.join(values)})")

insert_sql = insert_template + '\n' + ',\n'.join(insert_values) + ';'

# 将SQL语句保存到文件
with open('stock_premarket.sql', 'w', encoding='utf-8') as f:
    f.write(create_table_sql)
    f.write('\n\n')
    f.write(insert_sql)

print("\n创建表SQL语句：")
print(create_table_sql)
print("\n插入数据SQL语句（部分）：")
print(insert_sql[:1000] + '...')  # 只打印前1000个字符 