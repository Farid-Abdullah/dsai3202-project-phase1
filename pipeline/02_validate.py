# Databricks notebook source
# MAGIC %md
# MAGIC # 02_validate - quality gate on this run's staging data (task 2 of 4)
# MAGIC
# MAGIC A **profile** describes the data; the **gate** decides. If any FAIL rule fails, this task raises an error,
# MAGIC the job's `Run if = All succeeded` stops `03_publish`, and the published tables stay exactly as they were.
# MAGIC
# MAGIC | Rule | Policy | Why |
# MAGIC |---|---|---|
# MAGIC | `balanced`: rows read = accepted + invalid + duplicate copies | FAIL | every input row is accounted for |
# MAGIC | `complete`: rows read = `data_rows` in the input's manifest | FAIL | detects a truncated download or read |
# MAGIC | `reject_rate` <= 1% | FAIL | more invalid rows means the source or its format changed upstream |
# MAGIC | `event_types`: exactly view, cart, purchase all present | FAIL | the target is built from cart and purchase |
# MAGIC | `time_window`: all events inside November 2019 UTC | FAIL | protects the time-based split |
# MAGIC | `keys_not_null` in staging | FAIL | features are joined on user, session and product |
# MAGIC | `category_missing` <= 40%, `brand_missing` <= 20% | WARN | known gaps; a big jump should be investigated |
# MAGIC | `price_zero_share` | WARN (recorded) | zero prices are kept but flagged for EDA |
# MAGIC | invalid rows | QUARANTINE | saved with reasons by `01_clean`, never published |

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

clean = read_json(f"{REPORTS}/clean.json")
staged = spark.read.format("delta").load(STAGING)

try:
    manifest = read_json(clean["manifest_path"])
    expected_rows = int(manifest["file"]["data_rows"])
except Exception as e:
    manifest, expected_rows = None, None
    print("manifest not readable:", str(e)[:200])

prof = staged.agg(
    F.min("event_time").alias("min_ts"), F.max("event_time").alias("max_ts"),
    F.sum(F.when(F.col("category_code").isNull(), 1).otherwise(0)).alias("category_missing"),
    F.sum(F.when(F.col("brand").isNull(), 1).otherwise(0)).alias("brand_missing"),
    F.sum(F.when(F.col("price") == 0, 1).otherwise(0)).alias("price_zero"),
    F.sum(F.when(F.col("user_id").isNull() | F.col("user_session").isNull() | F.col("product_id").isNull(), 1)
           .otherwise(0)).alias("null_keys"),
    F.countDistinct("user_id").alias("users"), F.countDistinct("product_id").alias("products"),
    F.approx_count_distinct("user_session").alias("sessions_approx"),
).first().asDict()
types = {r["event_type"]: r["count"] for r in staged.groupBy("event_type").count().collect()}
n = clean["accepted"]
reject_rate = clean["invalid"] / max(clean["rows_read"], 1)

checks = [
  ("balanced",        "FAIL", clean["balanced"], f'{clean["rows_read"]} = {n} + {clean["invalid"]} + {clean["duplicate_copies"]}'),
  ("complete",        "FAIL", expected_rows is not None and clean["rows_read"] == expected_rows,
                              f'read {clean["rows_read"]}, manifest says {expected_rows}'),
  ("reject_rate",     "FAIL", reject_rate <= POLICY["max_reject_rate"], f"{reject_rate:.4%} (limit {POLICY['max_reject_rate']:.0%})"),
  ("event_types",     "FAIL", sorted(types) == sorted(EVENT_TYPES), str(types)),
  ("time_window",     "FAIL", prof["min_ts"] is not None and str(prof["min_ts"]) >= MONTH_START and str(prof["max_ts"]) < MONTH_END,
                              f'{prof["min_ts"]} .. {prof["max_ts"]}'),
  ("keys_not_null",   "FAIL", prof["null_keys"] == 0, f'{prof["null_keys"]} rows'),
  ("category_missing","WARN", prof["category_missing"] / max(n, 1) <= POLICY["max_category_missing"], f'{prof["category_missing"]/max(n,1):.2%}'),
  ("brand_missing",   "WARN", prof["brand_missing"] / max(n, 1) <= POLICY["max_brand_missing"], f'{prof["brand_missing"]/max(n,1):.2%}'),
  ("price_zero_share","WARN", True, f'{prof["price_zero"]} rows ({prof["price_zero"]/max(n,1):.4%})'),
]
failed = [c[0] for c in checks if c[1] == "FAIL" and not c[2]]
warned = [c[0] for c in checks if c[1] == "WARN" and not c[2]]
for name, policy, ok, detail in checks:
    print(f"{'PASS' if ok else policy:5s} {name:17s} {detail}")

gate = {"run_id": RUN_ID, "input": P["input"], "passed": not failed, "failed_rules": failed, "warnings": warned,
        "checks": [{"rule": c[0], "policy": c[1], "ok": bool(c[2]), "detail": c[3]} for c in checks],
        "profile": {**prof, "event_types": types, "accepted_rows": n}, "policy": POLICY,
        "finished_utc": datetime.datetime.utcnow().isoformat() + "Z"}
write_json(f"{REPORTS}/gate.json", gate)

if failed:
    raise RuntimeError(f"QUALITY GATE FAILED: {', '.join(failed)} - publish will not run; published tables unchanged")
print("QUALITY GATE PASSED", "(warnings: " + ", ".join(warned) + ")" if warned else "")
