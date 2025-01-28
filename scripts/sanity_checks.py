"""
Question 1 & 2 have me second-guessing - this is for peace of mind
"""

import json
from collections import defaultdict

from load_db import DATA_DIR, parse_timestamp
from infer_schema import decode

receipts = []
for receipt in decode(DATA_DIR / "receipts.json"):
    receipts.append(receipt)

dates = defaultdict(lambda: {"freq": 0, "ct_items": 0})

for receipt in receipts:
    month_scanned = parse_timestamp(receipt["dateScanned"]).strftime("%Y-%m")
    items = receipt.get("rewardsReceiptItemList", [])
    dates[month_scanned]["freq"] += 1
    dates[month_scanned]["ct_items"] += len(items)

print(json.dumps(dates, indent=2))
