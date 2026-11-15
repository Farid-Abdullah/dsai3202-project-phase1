# Databricks notebook source
# MAGIC %md
# MAGIC # 01_clean - raw CSV -> typed, validated, de-duplicated staging (task 1 of 4)
# MAGIC
# MAGIC Reads **this run's input** (preserved raw file, or the separate test input), never edits it.
# MAGIC
# MAGIC | Rule (first failure wins, so counts do not overlap) | Reject reason |
# MAGIC |---|---|
# MAGIC | Line has the wrong number of fields | `malformed_row` |
# MAGIC | `event_time` unparseable or outside November 2019 (UTC) | `bad_timestamp` |
# MAGIC | `event_type` not one of view / cart / purchase | `bad_event_type` |
# MAGIC | `product_id`, `user_id` not whole numbers, or `user_session` empty | `missing_key` |
# MAGIC | `price` unparseable or negative | `bad_price` |
# MAGIC
# MAGIC Missing `category_code` / `brand` are **expected** (about 32% / 14%) and are kept: they get the explicit value `unknown`
# MAGIC in derived columns, the original column stays NULL. Exact duplicate events (all nine fields equal) keep one copy.
# MAGIC
# MAGIC **Balance:** `input = accepted + invalid + duplicate copies`. Output goes to this run's own folder only;
# MAGIC published tables change only in `03_publish`, after the gate passes.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

require_new_run_dir()
src, manifest_path = input_location()
print("input:", src)

# Read every field as text first: a failed cast must become a visible reject, not a silent NULL.
from pyspark.sql.types import StructType, StructField, StringType
schema = StructType([StructField(c, StringType()) for c in COLS] + [StructField("_corrupt", StringType())])
raw = (spark.read.option("header", True).option("mode", "PERMISSIVE")
       .option("columnNameOfCorruptRecord", "_corrupt").schema(schema).csv(src)
       .withColumn("_source_file", F.col("_metadata.file_path")))

typed = (raw
  .withColumn("ts", F.expr("try_to_timestamp(regexp_replace(event_time, ' UTC$', ''), 'yyyy-MM-dd HH:mm:ss')"))
  .withColumn("product_id_l", F.expr("try_cast(product_id as bigint)"))
  .withColumn("category_id_l", F.expr("try_cast(category_id as bigint)"))
  .withColumn("user_id_l", F.expr("try_cast(user_id as bigint)"))
  .withColumn("price_d", F.expr("try_cast(price as double)")))

reason = (F.when(F.col("_corrupt").isNotNull(), "malformed_row")
  .when(F.col("ts").isNull() | (F.col("ts") < F.lit(MONTH_START)) | (F.col("ts") >= F.lit(MONTH_END)), "bad_timestamp")
  .when(~F.col("event_type").isin(EVENT_TYPES) | F.col("event_type").isNull(), "bad_event_type")
  .when(F.col("product_id_l").isNull() | F.col("user_id_l").isNull()
        | F.col("user_session").isNull() | (F.trim("user_session") == ""), "missing_key")
  .when(F.col("price_d").isNull() | (F.col("price_d") < 0), "bad_price"))
# Scan the 9 GB CSV exactly once: save every row with its verdict as run-scoped Parquet ("bronze").
BRONZE = f"{RUN_DIR}/bronze_checked"
typed.withColumn("reject_reason", reason).write.mode("error").parquet(BRONZE)
checked = spark.read.parquet(BRONZE)

# COMMAND ----------

# Rejected rows: original text values + reason, saved for inspection (row-level quarantine).
(checked.filter(F.col("reject_reason").isNotNull())
   .select("reject_reason", *COLS, "_corrupt", "_source_file")
   .write.mode("error").parquet(REJECTS))
rejects = spark.read.parquet(REJECTS)
reject_counts = {r["reject_reason"]: r["count"] for r in rejects.groupBy("reject_reason").count().collect()}
invalid = sum(reject_counts.values())

valid = (checked.filter(F.col("reject_reason").isNull())
  .select(F.col("ts").alias("event_time"), "event_type",
          F.col("product_id_l").alias("product_id"), F.col("category_id_l").alias("category_id"),
          F.expr("nullif(trim(category_code), '')").alias("category_code"),
          F.expr("nullif(lower(trim(brand)), '')").alias("brand"),
          F.col("price_d").alias("price"), F.col("user_id_l").alias("user_id"), F.trim("user_session").alias("user_session")))

# One shuffle does both jobs: group identical events, keep one, count the extra copies.
grouped = valid.groupBy(*COLS).agg(F.count(F.lit(1)).alias("_copies"))
(grouped.filter("_copies > 1").write.mode("error").parquet(DUPLICATES))

silver = (grouped.drop("_copies")
  .withColumn("event_date", F.to_date("event_time"))
  .withColumn("category_l1", F.coalesce(F.split("category_code", r"\.").getItem(0), F.lit("unknown")))
  .withColumn("brand_clean", F.coalesce("brand", F.lit("unknown"))))
(silver.write.format("delta").mode("error").partitionBy("event_date").save(STAGING))

# COMMAND ----------

staged = spark.read.format("delta").load(STAGING)
accepted = staged.count()
dup = spark.read.parquet(DUPLICATES)
dup_copies = int(dup.select(F.coalesce(F.sum(F.col("_copies") - 1), F.lit(0))).first()[0])
input_rows = invalid + accepted + dup_copies
read_rows = checked.count()      # every row Spark read from the file (saved once in bronze_checked)

report = {
  "run_id": RUN_ID, "input": P["input"], "input_path": src, "manifest_path": manifest_path,
  "rows_read": read_rows, "accepted": accepted, "invalid": invalid, "duplicate_copies": dup_copies,
  "duplicate_groups": dup.count(), "reject_counts": reject_counts,
  "balanced": read_rows == input_rows,
  "staging_path": STAGING, "rejects_path": REJECTS, "duplicates_path": DUPLICATES,
  "finished_utc": datetime.datetime.utcnow().isoformat() + "Z",
}
write_json(f"{REPORTS}/clean.json", report)
print(json.dumps(report, indent=2))
if not report["balanced"]:
    raise RuntimeError("NOT BALANCED: rows read != accepted + invalid + duplicate copies")
print(f"BALANCED: {read_rows:,} read = {accepted:,} accepted + {invalid:,} invalid + {dup_copies:,} duplicate copies")
print("STAGED - the gate decides next")
