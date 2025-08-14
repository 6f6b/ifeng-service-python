"""
批量更新数据脚本

功能：
    一次性更新多个数据表，包括：
    1. 板块资金流向数据
    2. 个股日线数据
    3. 个股资金流向数据
    4. 大盘资金流向数据
    5. 行业资金流向数据

使用方法：
    1. 更新指定开始日期到今天的所有数据：
        python batch_update.py --start_date 2024-01-01

    2. 强制更新指定开始日期到今天的所有数据：
        python batch_update.py --start_date 2024-01-01 --force

参数说明：
    --start_date: 开始日期，支持YYYY-MM-DD或YYYYMMDD格式
    --force: 强制更新标志，会先删除指定日期范围的数据再重新获取

依赖安装：
    pip install tushare pandas pymysql sqlalchemy python-dateutil
"""

import subprocess
import argparse
from datetime import datetime
from dateutil.parser import parse
import sys
import os

def parse_date(date_str):
    """解析日期字符串为datetime对象"""
    try:
        # 尝试解析多种格式的日期
        date = parse(date_str)
        return date.strftime('%Y%m%d')
    except:
        raise argparse.ArgumentTypeError(f'无效的日期格式: {date_str}')

# 需要更新的脚本列表，使用字典来存储不同脚本的参数格式
SCRIPTS = {
    'block_moneyflow_update.py': {'format': 'start_date'},
    'stock_daily_update.py': {'format': 'range'},
    'stock_moneyflow_update.py': {'format': 'start_date'},
    'market_moneyflow_update.py': {'format': 'start_date'},
    'industry_moneyflow_update.py': {'format': 'start_date'}
}

def print_with_time(message):
    """打印带时间戳的消息"""
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{current_time}] {message}")

def get_script_command(script_name, start_date, force=False):
    """根据脚本类型生成相应的命令行参数
    
    Args:
        script_name: 脚本文件名
        start_date: 开始日期
        force: 是否强制更新
    
    Returns:
        list: 命令行参数列表
    """
    # 获取当前脚本所在目录的绝对路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(current_dir, script_name)
    
    cmd = ['python', script_path]
    script_format = SCRIPTS[script_name]['format']
    
    if script_format == 'start_date':
        # 使用 --start_date 参数的脚本
        cmd.extend(['--start_date', start_date])
        if force:
            cmd.append('--force')
    elif script_format == 'range':
        # 使用 --mode range --start 参数的脚本
        cmd.extend(['--mode', 'range', '--start', start_date])
    
    return cmd

def run_script(script_name, start_date, force=False):
    """运行单个更新脚本
    
    Args:
        script_name: 脚本文件名
        start_date: 开始日期
        force: 是否强制更新
    """
    try:
        cmd = get_script_command(script_name, start_date, force)
        
        # 获取当前脚本所在目录
        current_dir = os.path.dirname(os.path.abspath(__file__))
        
        print_with_time(f"开始运行 {script_name}")
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            cwd=current_dir  # 设置工作目录为当前脚本所在目录
        )
        
        # 实时输出脚本的日志
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                sys.stdout.write(output)
                sys.stdout.flush()
        
        # 获取返回码
        return_code = process.poll()
        
        if return_code == 0:
            print_with_time(f"{script_name} 运行完成")
        else:
            # 获取错误输出
            error = process.stderr.read()
            print_with_time(f"{script_name} 运行失败: {error}")
            
    except Exception as e:
        print_with_time(f"运行 {script_name} 时出错: {str(e)}")

def main():
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='批量更新数据')
    parser.add_argument('--start_date', type=parse_date, required=True,
                      help='开始日期 (YYYY-MM-DD 或 YYYYMMDD)')
    parser.add_argument('--force', action='store_true',
                      help='强制更新（覆盖已有数据）')
    
    args = parser.parse_args()
    
    # 记录开始时间
    start_time = datetime.now()
    print_with_time(f"开始批量更新数据，起始日期: {args.start_date}")
    
    # 依次运行每个脚本
    for script in SCRIPTS:
        run_script(script, args.start_date, args.force)
    
    # 记录结束时间和总耗时
    end_time = datetime.now()
    duration = end_time - start_time
    print_with_time(f"所有数据更新完成，总耗时: {duration}")

if __name__ == "__main__":
    main() 