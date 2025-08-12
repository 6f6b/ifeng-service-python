import tushare as ts
import pandas as pd

# 设置token
ts.set_token('gx03013e909f633ecb66722df66b360f070426613316ebf06ecd3482')
pro = ts.pro_api()

# 获取上市公司基本信息数据（分别获取上交所、深交所和北交所的数据）
df_list = []
for exchange in ['SSE', 'SZSE', 'BSE']:
    df_temp = pro.stock_company(exchange=exchange, 
                              fields='ts_code,com_name,com_id,exchange,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,email,office,employees,main_business,business_scope')
    df_list.append(df_temp)

# 合并所有交易所的数据
df = pd.concat(df_list, ignore_index=True)

# 打印结果
print(df)

# 保存到Excel文件
df.to_excel('stock_company_info.xlsx', index=False)

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
    'com_name': '公司全称',
    'com_id': '统一社会信用代码',
    'exchange': '交易所代码',
    'chairman': '法人代表',
    'manager': '总经理',
    'secretary': '董秘',
    'reg_capital': '注册资本(万元)',
    'setup_date': '注册日期',
    'province': '所在省份',
    'city': '所在城市',
    'introduction': '公司介绍',
    'website': '公司主页',
    'email': '电子邮件',
    'office': '办公室',
    'employees': '员工人数',
    'main_business': '主要业务及产品',
    'business_scope': '经营范围'
}

# 生成建表SQL
table_name = 'stock_company'
columns = []
for column, dtype in df.dtypes.items():
    sql_type = dtype_mapping.get(str(dtype), 'VARCHAR(255)')
    # 对于公司介绍和经营范围使用TEXT类型
    if column in ['introduction', 'business_scope', 'main_business']:
        sql_type = 'TEXT'
    comment = field_comments.get(column, '')
    columns.append(f"    {column} {sql_type} COMMENT '{comment}'")

# 添加DROP语句和表注释
create_table_sql = f"""
DROP TABLE IF EXISTS {table_name};

CREATE TABLE IF NOT EXISTS {table_name} (
{',\n'.join(columns)},
    PRIMARY KEY (ts_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '上市公司基本信息表';
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
            # 处理特殊字符，避免SQL注入
            val_escaped = str(val).replace("'", "''")
            values.append(f"'{val_escaped}'")
    insert_values.append(f"({', '.join(values)})")

insert_sql = insert_template + '\n' + ',\n'.join(insert_values) + ';'

# 将SQL语句保存到文件
with open('stock_company.sql', 'w', encoding='utf-8') as f:
    f.write(create_table_sql)
    f.write('\n\n')
    f.write(insert_sql)

print("\n创建表SQL语句：")
print(create_table_sql)
print("\n插入数据SQL语句（部分）：")
print(insert_sql[:1000] + '...')  # 只打印前1000个字符 