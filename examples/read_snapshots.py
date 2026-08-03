from __future__ import annotations

from market_data_api import MarketDataClient


client = MarketDataClient("http://127.0.0.1:18788")
query = {
    "dataset": "snapshots",
    "start": "2026-05-29T09:15:00+08:00",
    "end": "2026-05-29T09:25:00+08:00",
    "mode": "direct",
    "columns": None,
}

print("health:", client.health())
print("estimate:", client.estimate(query))

rows = 0
for batch in client.iter_batches(query):
    rows += batch.num_rows
    # 在这里直接计算、写入用户自己的存储，或按需保留 batch。
print("streamed rows:", rows)

# 如果用户明确希望把全部结果保留在内存中：
# table = client.read_table(query)
