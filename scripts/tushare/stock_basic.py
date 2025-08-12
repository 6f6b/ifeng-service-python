import tushare as ts
import pandas as pd

# 设置token
ts.set_token('gx03013e909f633ecb66722df66b360f070426613316ebf06ecd3482')
pro = ts.pro_api()

# 获取所有状态的股票列表并合并
df_list = []
for status in ['L', 'D', 'P']:  # L上市 D退市 P暂停上市
    df_temp = pro.stock_basic(
        exchange='', 
        list_status=status,
        fields='ts_code,symbol,name,area,industry,market,exchange,curr_type,list_status,list_date,delist_date,is_hs'
    )
    df_list.append(df_temp)

# 合并所有数据
df = pd.concat(df_list, ignore_index=True)

# 打印结果
print(df)

# 保存到Excel文件
df.to_excel('stock_basic_info.xlsx', index=False)

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
    'symbol': '股票代码',
    'name': '股票名称',
    'area': '地域',
    'industry': '所属行业',
    'market': '市场类型（主板/创业板/科创板/CDR）',
    'exchange': '交易所代码',
    'curr_type': '交易货币',
    'list_status': '上市状态 L上市 D退市 P暂停上市',
    'list_date': '上市日期',
    'delist_date': '退市日期',
    'is_hs': '是否沪深港通标的，N否 H沪股通 S深股通'
}

# 生成建表SQL
table_name = 'stock_basic'
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
    PRIMARY KEY (ts_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '股票基础信息表';
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
with open('stock_basic.sql', 'w', encoding='utf-8') as f:
    f.write(create_table_sql)
    f.write('\n\n')
    f.write(insert_sql)

print("\n创建表SQL语句：")
print(create_table_sql)
print("\n插入数据SQL语句（部分）：")
print(insert_sql[:1000] + '...')  # 只打印前1000个字符