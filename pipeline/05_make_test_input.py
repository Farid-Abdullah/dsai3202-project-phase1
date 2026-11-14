# Databricks notebook source
# MAGIC %md
# MAGIC # 05_make_test_input - build the separate, deliberately broken test input (run once, by hand)
# MAGIC
# MAGIC Takes the first 50,000 data lines of the preserved raw file (read-only), corrupts every 20th line (5%)
# MAGIC in five rotating ways, and saves the result to `processed/project/test_inputs/bad_v1/events.csv` with its own manifest.
# MAGIC The raw file and the published tables are never touched.
# MAGIC
# MAGIC Expected job run with `input = test_bad`: `01_clean` succeeds and quarantines the bad lines, `02_validate` fails
# MAGIC `reject_rate` (5% > 1% limit), `03_publish` and `04_features` do not run.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

import hashlib
P_input_saved = P["input"]; P["input"] = "full"
src, _ = input_location(); P["input"] = P_input_saved
text = dbutils.fs.head(src, 9_000_000)
lines = text.split("\n")[:-1]                 # drop a possibly cut last line
header, rows = lines[0], lines[1:50_001]
assert header.split(",")[:2] == ["event_time", "event_type"], header

def corrupt(i, line):
    f = line.split(",")
    kind = (i // 20) % 5
    if kind == 0: f[1] = "refund"                       # bad_event_type
    elif kind == 1: f[6] = "-1.00"                       # bad_price
    elif kind == 2: f[7] = ""                            # missing_key
    elif kind == 3: f[0] = "not-a-time"                  # bad_timestamp
    else: f = f + ["extra", "fields"]                    # malformed_row
    return ",".join(f)

out = [header] + [corrupt(i, r) if i % 20 == 0 else r for i, r in enumerate(rows)]
body = "\n".join(out) + "\n"
dbutils.fs.put(f"{TEST_INPUT}/events.csv", body, overwrite=True)
manifest = {"purpose": "controlled quality failure test input (not real data quality)",
            "derived_from": src, "data_rows": len(rows), "corrupted_rows": len([i for i in range(len(rows)) if i % 20 == 0]),
            "corruption": "every 20th line; rotating bad_event_type, bad_price, missing_key, bad_timestamp, malformed_row",
            "file": {"name": "events.csv", "data_rows": len(rows), "sha256": hashlib.sha256(body.encode()).hexdigest()}}
write_json(f"{TEST_INPUT}/manifest.json", manifest)
print(json.dumps(manifest, indent=2))
