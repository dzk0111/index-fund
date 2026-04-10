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

DATA_FILE = 'index_pe_history.json'
LOG_FILE = 'update_logs.json'
SLEEP_SEC = 10

def load_json(path):
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_json(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, separators=(',', ':'))
    print(f'  saved: {path}')

def fetch_index(code, name, start_year, force=False, existing=None, site_id=None):
    today = date.today().strftime('%Y%m%d')

    if not force and existing and site_id in existing:
        old_dates = existing[site_id].get('dates', [])
        old_pes = existing[site_id].get('pe_values', [])
        if old_dates and len(old_dates) == len(old_pes):
            last_date = old_dates[-1]
            start_date = (datetime.strptime(last_date, '%Y-%m-%d') - timedelta(days=5)).strftime('%Y%m%d')
            print(f'  incremental: {last_date} -> {today}')
        else:
            force = True

    if force or not (existing and site_id in existing):
        start_date = f'{start_year}0101'
        print(f'  full: {start_date} -> {today}')

    df = ak.stock_zh_index_hist_csindex(symbol=code, start_date=start_date, end_date=today)

    col_map = {}
    for c in df.columns:
        cl = c.lower()
        if 'date' in cl or c == '日期':
            col_map[c] = 'date'
        elif 'pe' in cl or '市盈率' in c:
            col_map[c] = 'pe'
    df = df.rename(columns=col_map)

    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    df = df[df['pe'] > 0].sort_values('date').drop_duplicates('date')

    new_dates = df['date'].tolist()
    new_pes = [round(float(v), 2) for v in df['pe'].tolist()]

    if not force and existing and site_id in existing:
        old_dates = existing[site_id].get('dates', [])
        old_pes = existing[site_id].get('pe_values', [])
        last_old = old_dates[-1] if old_dates else ''
        add_rows = [(d, p) for d, p in zip(new_dates, new_pes) if d > last_old]
        if not add_rows:
            print(f'  already up to date ({last_old})')
            return existing[site_id]
        add_dates, add_pes = zip(*add_rows)
        all_dates = old_dates + list(add_dates)
        all_pes = old_pes + list(add_pes)
        print(f'  +{len(add_rows)} rows, total {len(all_dates)}')
    else:
        all_dates, all_pes = new_dates, new_pes
        print(f'  {len(all_dates)} rows, {all_dates[0]} -> {all_dates[-1]}')

    return {
        'name': name,
        'code': code,
        'source': 'csindex.com.cn',
        'dates': all_dates,
        'pe_values': all_pes,
        'latest_pe': all_pes[-1] if all_pes else None,
        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--force', action='store_true', help='force full update')
    args = parser.parse_args()

    existing = load_json(DATA_FILE)
    logs = load_json(LOG_FILE) or {'logs': [], 'last_success': None, 'last_failure': None}
    result = dict(existing)

    success_list, fail_list = [], []
    total = len(INDICES)

    print(f'\n{"="*50}')
    print(f'PE update {"full" if args.force else "incremental"} {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print(f'{total} indices\n{"="*50}')

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
                print(f'  attempt {attempt+1} failed: {e}')
                if attempt < 2:
                    wait = (attempt + 1) * 20
                    print(f'  retry in {wait}s...')
                    time.sleep(wait)
                else:
                    print(f'  {name} failed, keeping old data')
                    fail_list.append(name)

        if i < total:
            time.sleep(SLEEP_SEC)

    save_json(DATA_FILE, result)

    log_entry = {
        'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'mode': 'full' if args.force else 'incremental',
        'success': success_list,
        'failed': fail_list,
    }
    logs.setdefault('logs', []).insert(0, log_entry)
    logs['logs'] = logs['logs'][:50]
    if not fail_list:
        logs['last_success'] = log_entry['time']
        logs['consecutive_failures'] = 0
    else:
        logs['last_failure'] = log_entry['time']
        logs['consecutive_failures'] = logs.get('consecutive_failures', 0) + 1
    save_json(LOG_FILE, logs)

    print(f'\n{"="*50}')
    print(f'success: {len(success_list)}')
    if fail_list:
        print(f'failed: {len(fail_list)} -> {", ".join(fail_list)}')
        sys.exit(1)
    else:
        print('all done!')

if __name__ == '__main__':
    main()
