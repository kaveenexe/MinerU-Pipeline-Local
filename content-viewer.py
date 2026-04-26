import json

with open("output/49495_2774_1763025600606.09.2025_FINAL/49495_2774_1763025600606.09.2025_FINAL/auto/49495_2774_1763025600606.09.2025_FINAL_content_list.json") as f:
    blocks = json.load(f)

for block in blocks:
    if block.get("type") == "table":
        print(block.get("table_body", ""))  # raw HTML table
        print("---")