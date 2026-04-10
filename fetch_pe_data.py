#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GitHub Actions 专用 PE 数据更新脚本
数据来源：中证指数有限公司（csindex.com.cn）
运行方式：python fetch_pe_data.py [--force]
"""

import json
import sys
import time
import argparse
from datetime import datetime, date, timedelta
import os

import akshare as ak
import pandas as pd

# ──────────────────────────────────────────────────────────
# 指数配置：全部来自中证官网
# ──────────────────────────────────────────────────────────
INDICES = {
    'hs300':  ('000300', '沪深300',    2005),
    'zz500':  ('000905', '中证500',    2007),
    'sz50':   ('000016', '上证50',     2004),
    'kc50':   ('000688', '科创50',     2019),
    'zzhl':   ('000922', '中证红利',   2008),
    'zz1000': ('000852', '中证1000',   2014),
    'zz100':  ('000903', '中证100',    2006),
    'zz800':  ('000906', '中证800',    2007),
    'cyb':    ('930743', '创业板50',   2015),
    'zzyl':   ('399989', '中证医疗',   2005),
    'zzbj':   ('399997', '中证白酒',   2005),
    'fdcre':  ('000975', '中证地产',   2005),
    'xny':    ('931151', '中证新能源', 2013),
}

DATA_FILE   = 'index_pe_history.json'
LOG_FILE    = 'update_logs.json'
SLEEP_SEC   = 10   # 请求间隔，避免触发中证官网限流


def load_json(path):
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_json(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, separators=(',', ':'))
    print(f'  ✅ 已保存 → {path}')


def fetch_index(code, name, start_year, force=False, existing=None, site_id=None):
    """抓取单个指数的 PE 历史数据"""
    today = date.today().strftime('%Y%m%d')

    # 增量模式：只抓最近30天
    if not force and existing and site_id in existing:
        old_dates = existing[site_id].get('dates', [])
        old_pes   = existing[site_id].get('pe_values', [])
        if old_dates and len(old_dates) == len(old_pes):
            last_date = old_dates[-1]
            start_date = (datetime.strptime(last_date, '%Y-%m-%d') - timedelta(days=5)).strftime('%Y%m%d')
            print(f'  增量模式：{last_date} → {today}')
        else:
            force = True  # 数据异常，回退全量

    if force or not (existing and site_id in existing):
        start_date = f'{start_year}0101'
        print(f'  全量模式：{start_date} → {today}')

    df = ak.stock_zh_index_hist_csindex(symbol=code, start_date=start_date, end_date=today)

    # 统一列名
    col_map = {}
    for c in df.columns:
        cl = c.lower()
        if 'date' in cl or '日期' in cl:
            col_map[c] = 'date'
        elif 'pe' in cl:
            col_map[c] = 'pe'
    df = df.rename(columns=col_map)

    # 格式化日期，过滤 PE <= 0
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    df = df[df['pe'] > 0].sort_values('date').drop_duplicates('date')

    new_dates = df['date'].tolist()
    new_pes   = [round(float(v), 2) for v in df['pe'].tolist()]

    # 增量合并
    if not force and existing and site_id in existing:
        old_dates = existing[site_id].get('dates', [])
        old_pes   = existing[site_id].get('pe_values', [])
        last_old  = old_dates[-1] if old_dates else ''
        add_rows  = [(d, p) for d, p in zip(new_dates, new_pes) if d > last_old]
        if not add_rows:
            print(f'  ✓ 已是最新（最新：{last_old}）')
            return existing[site_id]
        add_dates, add_pes = zip(*add_rows)
        all_dates = old_dates + list(add_dates)
        all_pes   = old_pes   + list(add_pes)
        print(f'  ✓ 新增 {len(add_rows)} 条，共 {len(all_dates)} 条')
    else:
        all_dates, all_pes = new_dates, new_pes
        print(f'  ✓ {len(all_dates)} 条，{all_dates[0]} → {all_dates[-1]}')

    return {
        'name':       name,
        'code':       code,
        'source':     'csindex.com.cn',
        'dates':      all_dates,
        'pe_values':  all_pes,
        'latest_pe':  all_pes[-1] if all_pes else None,
        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--force', action='store_true', help='全量重新抓取')
    args = parser.parse_args()

    existing = load_json(DATA_FILE)
    logs     = load_json(LOG_FILE) or {'logs': [], 'last_success': None, 'last_failure': None}
    result   = dict(existing)

    success_list, fail_list = [], []
    total = len(INDICES)

    print(f'\n{"="*50}')
    print(f'PE数据更新  {"全量" if args.force else "增量"}模式  {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print(f'共 {total} 个指数\n{"="*50}')

    for i, (site_id, (code, name, start_year)) in enumerate(INDICES.items(), 1):
        print(f'\n[{i}/{total}] {name} ({code})')
        for attempt in range(3):
            try:
                data = fetch_index(code, name, start_year,
                                   force=args.force,
                                   existing=existing,
                                   site_id=site_id)
                result[site_id] = data
                success_list.append(name)
                break
            except Exception as e:
                print(f'  ⚠️  第{attempt+1}次失败：{e}')
                if attempt < 2:
                    wait = (attempt + 1) * 20
                    print(f'  等待 {wait}s 后重试...')
                    time.sleep(wait)
                else:
                    print(f'  ❌ {name} 最终失败，保留旧数据')
                    fail_list.append(name)

        if i < total:
            time.sleep(SLEEP_SEC)

    # 保存数据
    save_json(DATA_FILE, result)

    # 更新日志
    log_entry = {
        'time':    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'mode':    'force' if args.force else 'incremental',
        'success': success_list,
        'failed':  fail_list,
    }
    logs.setdefault('logs', []).insert(0, log_entry)
    logs['logs'] = logs['logs'][:50]  # 只保留最近50条
    if not fail_list:
        logs['last_success'] = log_entry['time']
        logs['consecutive_failures'] = 0
    else:
        logs['last_failure'] = log_entry['time']
        logs['consecutive_failures'] = logs.get('consecutive_failures', 0) + 1
    save_json(LOG_FILE, logs)

    # 汇总
    print(f'\n{"="*50}')
    print(f'✅ 成功：{len(success_list)} 个')
    if fail_list:
        print(f'❌ 失败：{len(fail_list)} 个 → {", ".join(fail_list)}')
        sys.exit(1)
    else:
        print('🎉 全部更新成功！')


if __name__ == '__main__':
    main()
