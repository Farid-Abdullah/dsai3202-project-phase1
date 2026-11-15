# Databricks notebook source
# MAGIC %md
# MAGIC # 03_publish - publish silver only after the gate passed (task 3 of 4)
# MAGIC
# MAGIC Double guard: the job runs this task only if `02_validate` succeeded, **and** this notebook re-reads the saved
# MAGIC `gate.json` and refuses to continue unless it says `passed: true` for this run and the input is `full`.
# MAGIC
# MAGIC Publishing is a full rebuild of one Delta table (`processed/project/silver_events`, partitioned by `event_date`).
# MAGIC Delta makes the overwrite atomic and keeps the old version (time travel), unlike plain Parquet in Lab-4.
# MAGIC The table is registered in Unity Catalog with table and column comments (catalog entry for Phase-1 II.3).

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

gate = read_json(f"{REPORTS}/gate.json")
if not gate.get("passed") or gate.get("run_id") != RUN_ID:
    raise RuntimeError("REFUSED: gate.json for this run did not pass")
if P["input"] != "full":
    raise RuntimeError("REFUSED: only the full preserved input may be published")
clean = read_json(f"{REPORTS}/clean.json")

staged = spark.read.format("delta").load(STAGING)
publish_table(staged, SILVER_TABLE, SILVER_PATH, partition="event_date")

# COMMAND ----------

# Catalog entry: describe the table and every column where people will look for it.
src_sha = clean["input_path"].split("sha256=")[1].split("/")[0] if "sha256=" in clean["input_path"] else "n/a"
spark.sql(f"""COMMENT ON TABLE {SILVER_TABLE} IS
'REES46 eCommerce events, November 2019 (Kaggle mkechinov/ecommerce-behavior-data-from-multi-category-store).
One row = one distinct event. Built by job task 01_clean from raw/rees46/2019-Nov/sha256={src_sha}/2019-Nov.csv;
typed, validated, exact duplicates removed; published only after the 02_validate quality gate. Last run {RUN_ID}.'""")
col_docs = {
  "event_time": "Event time, UTC (source text had a trailing UTC).",
  "event_type": "view | cart | purchase.",
  "product_id": "Anonymous product id (key part).",
  "category_id": "Anonymous category id.",
  "category_code": "Category path such as electronics.smartphone; NULL when the source has none (~32%).",
  "brand": "Lower-cased brand; NULL when missing (~14%).",
  "price": "Price at event time; >= 0 (negative rows rejected).",
  "user_id": "Anonymous user id.",
  "user_session": "Session id; one user visit.",
  "event_date": "UTC date of event_time; partition column.",
  "category_l1": "First segment of category_code, or 'unknown'.",
  "brand_clean": "brand, or 'unknown'.",
}
for c, doc in col_docs.items():
    spark.sql(f"ALTER TABLE {SILVER_TABLE} ALTER COLUMN {c} COMMENT '" + doc.replace("'", "\\'") + "'")
spark.sql(f"ALTER TABLE {SILVER_TABLE} SET TBLPROPERTIES ('project.source_sha256'='{src_sha}', 'project.last_run_id'='{RUN_ID}')")

# COMMAND ----------

hist = spark.sql(f"DESCRIBE HISTORY {SILVER_TABLE} LIMIT 1").first()
silver = spark.table(SILVER_TABLE)
fp = fingerprint(silver, COLS)
publish = {"run_id": RUN_ID, "silver_table": SILVER_TABLE, "silver_path": SILVER_PATH,
           "silver_version": int(hist["version"]), "silver_fingerprint": fp,
           "expected_rows": clean["accepted"], "finished_utc": datetime.datetime.utcnow().isoformat() + "Z"}
write_json(f"{REPORTS}/publish.json", publish)
print(json.dumps(publish, indent=2))
if fp["rows"] != clean["accepted"]:
    raise RuntimeError("Published row count differs from accepted rows")
print(f"BALANCED: published {fp['rows']:,} rows = accepted rows")
print("PUBLISHED silver version", publish["silver_version"])
