#!/usr/bin/env python3
"""
GitHub Actions 专用 PE 数据抓取脚本
精简版 - 只保留核心功能
"""

import json
import time
import akshare as ak
from datetime import datetime
import os

# 13个指数配置（中证官网数据源）
INDEX_CONFIG = {
    "000300": {"name": "沪深300", "market": "sh"},
    "000905": {"name": "中证500", "market": "sh"},
    "000016": {"name": "上证50", "market": "sh"},
    "000688": {"name": "科创50", "market": "sh"},
    "000922": {"name": "中证红利", "market": "sh"},
    "000852": {"name": "中证1000", "market": "sh"},
    "000903": {"name": "中证100", "market": "sh"},
    "000906": {"name": "中证800", "market": "sh"},
    "399989": {"name": "中证医疗", "market": "sz"},
    "399997": {"name": "中证白酒", "market": "sz"},
    "931775": {"name": "中证地产", "market": "sh"},
    "399808": {"name": "中证新能源", "market": "sz"},
    "930743": {"name": "创业板50", "market": "sh"},
}

DATA_FILE = "index_pe_history.json"
LOG_FILE = "update_logs.json"

def load_existing_data():
    """加载现有数据"""
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {code: [] for code in INDEX_CONFIG}

def save_data(data):
    """保存数据"""
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✓ 数据已保存到 {DATA_FILE}")

def fetch_index_pe(code, name, market):
    """从中证官网抓取指数PE数据"""
    try:
        # 使用中证官网数据源
        df = ak.stock_zh_index_hist_csindex(symbol=code)
        
        if df is None or df.empty:
            print(f"  ✗ {name}({code}): 无数据")
            return None
            
        # 获取最新数据
        latest = df.iloc[-1]
        date_str = str(latest['日期'])[:10]
        pe = float(latest['市盈率'])
        
        print(f"  ✓ {name}({code}): {date_str} PE={pe:.2f}")
        return {
            "date": date_str,
            "pe": round(pe, 4),
            "source": "csindex"
        }
        
    except Exception as e:
        print(f"  ✗ {name}({code}): {str(e)}")
        return None

def update_data():
    """增量更新所有指数数据"""
    print(f"\n{'='*60}")
    print(f"开始增量更新 PE 数据")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    data = load_existing_data()
    today = datetime.now().strftime('%Y-%m-%d')
    updated_count = 0
    
    for code, config in INDEX_CONFIG.items():
        print(f"\n[{config['name']}]")
        
        result = fetch_index_pe(code, config['name'], config['market'])
        
        if result:
            # 检查是否已存在该日期的数据
            existing_dates = {d['date'] for d in data.get(code, [])}
            
            if result['date'] not in existing_dates:
                data[code].append(result)
                updated_count += 1
                print(f"  → 新增数据: {result['date']}")
            else:
                print(f"  → 日期已存在，跳过")
        
        # 避免触发API频率限制
        time.sleep(10)
    
    save_data(data)
    
    # 更新日志
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "type": "incremental",
        "updated_indices": updated_count,
        "date": today
    }
    
    logs = []
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'r', encoding='utf-8') as f:
            logs = json.load(f)
    logs.append(log_entry)
    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        json.dump(logs, f, ensure_ascii=False, indent=2)
    
    print(f"\n{'='*60}")
    print(f"更新完成！共更新 {updated_count} 个指数")
    print(f"{'='*60}")

if __name__ == "__main__":
    update_data()
