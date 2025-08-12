import pymysql
import pandas as pd
from datetime import datetime
import re

# 数据库连接配置
DB_CONFIG = {
    'host': 'rplus-dev.mysql.cn-chengdu.rds.aliyuncs.com',
    'user': 'PostopDev',
    'password': 'PostopDevPwd@MySQL',
    'charset': 'utf8mb4'
}

def format_sql(sql):
    """将多行SQL转换为单行，保留基本可读性"""
    # 移除多余的空白字符
    sql = re.sub(r'\s+', ' ', sql)
    # 在逗号后添加空格以提高可读性
    sql = re.sub(r',', ', ', sql)
    # 确保关键字前有空格
    sql = re.sub(r'(\S)(CREATE|TABLE|PRIMARY|KEY|FOREIGN|REFERENCES|DEFAULT|NOT|NULL|AUTO_INCREMENT|COMMENT)', r'\1 \2', sql)
    return sql.strip()

def get_all_databases(cursor):
    """获取所有以cfda_开头的数据库"""
    cursor.execute("SHOW DATABASES")
    databases = cursor.fetchall()
    # 只返回以cfda_开头的数据库
    return [db[0] for db in databases if db[0].startswith('cfda_')]

def check_id_fields(cursor, table_name):
    """检查表中是否包含特定ID字段"""
    cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
    columns = [column[0].lower() for column in cursor.fetchall()]
    
    id_checks = {
        'user_id': 'user_id' in columns,
        'patient_id': 'patient_id' in columns,
        'doctor_id': 'doctor_id' in columns
    }
    
    # 返回包含的ID字段列表
    contained_ids = [field for field, exists in id_checks.items() if exists]
    return ', '.join(contained_ids) if contained_ids else '无'

def get_table_info(cursor, database):
    """获取指定数据库中所有表的信息"""
    cursor.execute(f"USE `{database}`")
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    
    table_info = []
    for (table_name,) in tables:
        try:
            # 获取建表SQL
            cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
            _, create_sql = cursor.fetchone()
            
            # 格式化SQL为单行
            create_sql = format_sql(create_sql)
            
            # 检查ID字段
            id_fields = check_id_fields(cursor, table_name)
            
            table_info.append({
                '数据库名': database,
                '表名': table_name,
                '包含ID字段': id_fields,
                '建表SQL': create_sql
            })
        except Exception as e:
            print(f"获取表 {database}.{table_name} 信息时出错: {str(e)}")
            continue
    
    return table_info

def main():
    try:
        # 连接数据库
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        all_table_info = []
        
        # 获取所有cfda_开头的数据库
        databases = get_all_databases(cursor)
        total_dbs = len(databases)
        
        if not databases:
            print("未找到以cfda_开头的数据库")
            return
        
        print(f"开始导出，共发现 {total_dbs} 个cfda_数据库")
        
        # 遍历每个数据库
        for idx, db in enumerate(databases, 1):
            print(f"正在处理数据库 ({idx}/{total_dbs}): {db}")
            table_info = get_table_info(cursor, db)
            all_table_info.extend(table_info)
            
        # 创建DataFrame并导出到Excel
        df = pd.DataFrame(all_table_info)
        
        # 生成带时间戳的文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'cfda数据库结构_{timestamp}.xlsx'
        
        # 设置Excel列的顺序
        columns = ['数据库名', '表名', '包含ID字段', '建表SQL']
        df = df[columns]
        
        # 导出到Excel，设置列宽并自动调整行高
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='数据库结构')
            worksheet = writer.sheets['数据库结构']
            worksheet.column_dimensions['A'].width = 20  # 数据库名
            worksheet.column_dimensions['B'].width = 30  # 表名
            worksheet.column_dimensions['C'].width = 20  # 包含ID字段
            worksheet.column_dimensions['D'].width = 150 # 建表SQL
            
            # 设置自动换行
            for row in worksheet.iter_rows():
                for cell in row:
                    cell.alignment = cell.alignment.copy(wrap_text=False)
        
        print(f"导出完成！文件已保存为: {filename}")
        print(f"共导出 {len(databases)} 个数据库，{len(all_table_info)} 个表的结构")
        
    except Exception as e:
        print(f"发生错误: {str(e)}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
