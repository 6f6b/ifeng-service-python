#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime

def main():
    """示例任务函数"""
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'[{current_time}] Application Start Success!')

if __name__ == '__main__':
    main() 