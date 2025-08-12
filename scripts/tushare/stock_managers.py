import tushare as ts
import pandas as pd
import time

# 设置token
ts.set_token('gx03013e909f633ecb66722df66b360f070426613316ebf06ecd3482')
pro = ts.pro_api()

# 读取股票基础信息
basic_df = pd.read_excel('stock_basic_info.xlsx')

# 获取所有公司的管理层数据
df_list = []
total = len(basic_df)
for index, row in basic_df.iterrows():
    try:
        print(f"正在处理 {row['ts_code']} ({index + 1}/{total})")
        df_temp = pro.stk_managers(
            ts_code=row['ts_code'], 
            fields='ts_code,ann_date,name,gender,lev,title,edu,national,birthday,begin_date,end_date,resume'
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

    # 打印结果
    print(df)

    # 保存到Excel文件
    df.to_excel('stock_managers_info.xlsx', index=False)

    # 数据类型映射
    dtype_mapping = {
        'object': 'VARCHAR(255)',
        'int64': 'INT',
        'float64': 'DECIMAL(20,4)',
        'datetime64[ns]': 'DATETIME'
    }

    # 字段注释映射
    field_comments = {
        'ts_code': 'TS股票代码',
        'ann_date': '公告日期',
        'name': '姓名',
        'gender': '性别',
        'lev': '岗位类别',
        'title': '岗位',
        'edu': '学历',
        'national': '国籍',
        'birthday': '出生年月',
        'begin_date': '上任日期',
        'end_date': '离任日期',
        'resume': '个人简历'
    }

    # 生成建表SQL
    table_name = 'stock_managers'
    columns = []
    for column, dtype in df.dtypes.items():
        sql_type = dtype_mapping.get(str(dtype), 'VARCHAR(255)')
        # 对于简历使用TEXT类型
        if column == 'resume':
            sql_type = 'TEXT'
        comment = field_comments.get(column, '')
        columns.append(f"    {column} {sql_type} COMMENT '{comment}'")

    # 添加DROP语句和表注释
    create_table_sql = f"""
    DROP TABLE IF EXISTS {table_name};

    CREATE TABLE IF NOT EXISTS {table_name} (
    {',\n'.join(columns)},
        PRIMARY KEY (ts_code, ann_date, name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '上市公司管理层信息表';
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
    with open('stock_managers.sql', 'w', encoding='utf-8') as f:
        f.write(create_table_sql)
        f.write('\n\n')
        f.write(insert_sql)

    print("\n创建表SQL语句：")
    print(create_table_sql)
    print("\n插入数据SQL语句（部分）：")
    print(insert_sql[:1000] + '...')  # 只打印前1000个字符
else:
    print("没有获取到任何数据") 