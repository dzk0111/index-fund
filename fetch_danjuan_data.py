#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从蛋卷基金API获取指数PB/ROE数据，更新到index_config.json
数据来源：https://danjuanapp.com/djapi/index_eva/dj
支持指数：沪深300/中证500/上证50/科创50/中证红利/中证1000/中证消费/中证银行/中证医疗/中证白酒

用法：
    python fetch_danjuan_data.py                      # 获取并更新index_config.json
    python fetch_danjuan_data.py --dry-run            # 只查看数据，不更新文件
    python fetch_danjuan_data.py --output other.json   # 输出到其他文件
"""

import json
import sys
import io
import os
import argparse
import urllib.request
from datetime import datetime

# 修复 S4U 后台模式下 stdout 编码问题（计划任务无终端时 encoding=None）
if sys.stdout and hasattr(sys.stdout, 'buffer'):
    if not sys.stdout.encoding or sys.stdout.encoding.lower() not in ('utf-8', 'utf8'):
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr and hasattr(sys.stderr, 'buffer'):
    if not sys.stderr.encoding or sys.stderr.encoding.lower() not in ('utf-8', 'utf8'):
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# 蛋卷基金API
DANJUAN_API_URL = 'https://danjuanapp.com/djapi/index_eva/dj'
DANJUAN_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# 指数ID映射：site_id -> 蛋卷指数代码
# 蛋卷代码格式：SH000300(上海) / SZ399986(深圳)
INDEX_CODE_MAP = {
    'hs300':  'SH000300',
    'zz500':  'SH000905',
    'sz50':   'SH000016',
    'kc50':   'SH000688',
    'zzhl':   'SH000922',
    'zz1000': 'SH000852',
    'zzxf':   'SH000932',
    'zzyh':   'SZ399986',
    'zzyl':   'SZ399989',
    'zzbj':   'SZ399997',
}

# 股息率来源：中证官网 stock_zh_index_value_csindex
# 这里做备份，如果蛋卷没有股息率数据，可以从中证官网获取
DIVIDEND_FALLBACK = {
    'hs300': 0.032,
    'zz500': 0.0118,
    'sz50': 0.0429,
    'kc50': 0.0043,
    'zzhl': 0.0496,
    'zz1000': 0.0099,
    'zzxf': 0.0349,
    'zzyh': 0.0456,
    'zzyl': 0.0084,
    'zzbj': 0.0395,
}


def fetch_danjuan_data():
    """从蛋卷基金API获取所有指数估值数据"""
    print(f'正在从蛋卷基金API获取数据...')
    print(f'API: {DANJUAN_API_URL}')

    req = urllib.request.Request(DANJUAN_API_URL, headers=DANJUAN_HEADERS)
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode('utf-8'))

    items = data.get('data', {}).get('items', [])
    print(f'共获取到 {len(items)} 个指数数据\n')

    # 构建蛋卷代码 -> 数据的映射
    danjuan_map = {}
    for item in items:
        code = item.get('index_code', '') or item.get('code', '')
        danjuan_map[code] = item

    # 提取我们需要的指数数据
    result = {}
    missing = []

    for site_id, dj_code in INDEX_CODE_MAP.items():
        if dj_code in danjuan_map:
            item = danjuan_map[dj_code]
            name = item.get('index_name', '') or item.get('name', '')
            pe = item.get('pe')
            pb = item.get('pb')
            roe = item.get('roe')
            dividend_yield = item.get('dividend_yield')

            # 股息率：蛋卷可能为 N/A，使用中证官网的备用数据
            dividend = None
            if dividend_yield and dividend_yield not in ('N/A', '', None):
                try:
                    dividend = float(dividend_yield) / 100 if float(dividend_yield) > 1 else float(dividend_yield)
                except (ValueError, TypeError):
                    dividend = DIVIDEND_FALLBACK.get(site_id)
            else:
                dividend = DIVIDEND_FALLBACK.get(site_id)

            result[site_id] = {
                'name': name,
                'code': dj_code[2:],  # 去掉SH/SZ前缀
                'pe': round(float(pe), 2) if pe else None,
                'pb': round(float(pb), 2) if pb else None,
                'roe': round(float(roe), 4) if roe else None,  # 0.1087 格式
                'dividend_rate': round(float(dividend), 4) if dividend else None,
            }
            print(f'  ✅ {name}({dj_code}): PE={result[site_id]["pe"]}, PB={result[site_id]["pb"]}, ROE={result[site_id]["roe"]}, 股息率={result[site_id]["dividend_rate"]}')
        else:
            missing.append(f'{site_id}({dj_code})')
            print(f'  ❌ {site_id}({dj_code}): 未找到')

    if missing:
        print(f'\n⚠️ 未找到的指数: {", ".join(missing)}')

    return result


def load_config(config_path):
    """加载现有配置"""
    if os.path.exists(config_path):
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {
        'update_date': '',
        'description': '指数基金辅助工具手动配置数据',
        'admin_password_hash': '',
        'change_history': [],
        'indices': {}
    }


def save_config(config_path, config):
    """保存配置"""
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    print(f'\n✅ 配置已保存到: {config_path}')


def main():
    parser = argparse.ArgumentParser(description='从蛋卷基金API获取PB/ROE数据并更新index_config.json')
    parser.add_argument('--output', default=None, help='输出文件路径（默认自动检测）')
    parser.add_argument('--dry-run', action='store_true', help='只查看数据，不更新文件')
    args = parser.parse_args()

    # 确定配置文件路径
    if args.output:
        config_path = args.output
    else:
        # 默认路径
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, 'index_config.json')
        if not os.path.exists(config_path):
            # 尝试 github-deploy 目录
            alt_path = os.path.join(os.path.dirname(script_dir), 'github-deploy', 'index_config.json')
            if os.path.exists(alt_path):
                config_path = alt_path

    print('=' * 60)
    print('蛋卷基金 PB/ROE 数据更新工具')
    print(f'配置文件: {config_path}')
    print(f'模式: {"只读" if args.dry_run else "更新"}')
    print('=' * 60)
    print()

    # 获取蛋卷数据
    danjuan_data = fetch_danjuan_data()

    if args.dry_run:
        print('\n[Dry Run] 不会更新任何文件')
        return 0

    # 加载现有配置
    config = load_config(config_path)

    # 更新数据
    today = datetime.now().strftime('%Y-%m-%d')
    old_config = dict(config.get('indices', {}))

    for site_id, data in danjuan_data.items():
        if site_id not in config.get('indices', {}):
            config.setdefault('indices', {})[site_id] = {}

        idx_config = config['indices'][site_id]

        # 记录变更
        for field, key in [('currentPB', 'pb'), ('roe', 'roe'), ('dividendRate', 'dividend_rate')]:
            old_val = idx_config.get(field)
            new_val = data[key]

            if old_val is not None and new_val is not None and abs(old_val - new_val) > 0.0001:
                config.setdefault('change_history', []).append({
                    'date': today,
                    'index_name': data['name'],
                    'field': field,
                    'old_value': str(old_val),
                    'new_value': str(new_val)
                })

        # 更新值
        idx_config['name'] = data['name']
        idx_config['code'] = data['code']
        if data['pb'] is not None:
            idx_config['currentPB'] = data['pb']
        if data['roe'] is not None:
            idx_config['roe'] = data['roe']
        if data['dividend_rate'] is not None:
            idx_config['dividendRate'] = data['dividend_rate']

    config['update_date'] = today

    # 只保留最近50条变更记录
    if len(config.get('change_history', [])) > 50:
        config['change_history'] = config['change_history'][-50:]

    # 保存
    save_config(config_path, config)

    # 输出变更摘要
    today_changes = [c for c in config.get('change_history', []) if c.get('date') == today]
    if today_changes:
        print(f'\n📊 本次更新变更 {len(today_changes)} 项:')
        for c in today_changes:
            print(f'  - {c["index_name"]} {c["field"]}: {c["old_value"]} → {c["new_value"]}')
    else:
        print('\n📊 本次无数据变更')

    return 0


if __name__ == '__main__':
    sys.exit(main())
