# Databricks notebook source
# MAGIC %md
# MAGIC # 00_common - shared settings, paths and helpers
# MAGIC Every task notebook starts with `%run ./00_common`. Nothing here writes data.
# MAGIC
# MAGIC | Job parameter | Meaning |
# MAGIC |---|---|
# MAGIC | `lake` | Lab-3 storage account, e.g. `amazonlake60304642` |
# MAGIC | `catalog`, `schema` | Unity Catalog names from Lab-5/6 |
# MAGIC | `run_id` | `{{job.run_id}}` in the job; a manual id when run by hand |
# MAGIC | `input` | `full` = the preserved raw file, `test_bad` = the separate corrupted test input |
# MAGIC | `source_container` | `raw` (default) or `processed` if Databricks cannot read raw (landing copy) |
# MAGIC | `compare_to` | a successful run id; the run then proves SAME CONTENT against it |
# MAGIC
# MAGIC Adapted from DSAI3202 Lab-5 (validation, balanced counts, Delta) and Lab-6 (prepare -> validate -> publish, run folders, gate).

# COMMAND ----------

import json, datetime
from pyspark.sql import functions as F, Window

spark.conf.set("spark.sql.session.timeZone", "UTC")

for name, default in [("lake", ""), ("catalog", "dsai3202_databricks"), ("schema", ""), ("run_id", ""),
                      ("input", "full"), ("source_container", "processed"), ("compare_to", "")]:
    dbutils.widgets.text(name, default)

P = {k: dbutils.widgets.get(k).strip() for k in
     ["lake", "catalog", "schema", "run_id", "input", "source_container", "compare_to"]}
if not P["lake"] or not P["schema"]:
    raise ValueError("Fill in the lake and schema widgets / job parameters first.")
if not P["run_id"] or "{{" in P["run_id"]:
    P["run_id"] = "manual-" + datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
assert P["input"] in ("full", "test_bad"), "input must be full or test_bad"

LAKE, CAT, SCH, RUN_ID = P["lake"], P["catalog"], P["schema"], P["run_id"]
def uri(container, path=""):
    return f"abfss://{container}@{LAKE}.dfs.core.windows.net/{path}".rstrip("/")

# ---- layout ------------------------------------------------------------------
RAW_BASE      = uri(P["source_container"], "rees46/2019-Nov") if P["source_container"] == "raw" \
                else uri("processed", "project/landing/rees46/2019-Nov")
TEST_INPUT    = uri("processed", "project/test_inputs/bad_v1")
RUN_DIR       = uri("processed", f"project/runs/run_id={RUN_ID}")       # per-run staging + rejects
STAGING       = f"{RUN_DIR}/silver_staging"
REJECTS       = f"{RUN_DIR}/rejects"
DUPLICATES    = f"{RUN_DIR}/duplicate_groups"
SILVER_PATH   = uri("processed", "project/silver_events")
FEATURES_PATH = uri("curated", "project/features_v1")
REPORTS       = uri("curated", f"project/quality/runs/{RUN_ID}")
LATEST        = uri("curated", "project/quality/latest_success.json")
SILVER_TABLE   = f"{CAT}.{SCH}.project_silver_events"
FEATURES_TABLE = f"{CAT}.{SCH}.project_features_v1"
FEATURE_VERSION = 1

COLS = ["event_time", "event_type", "product_id", "category_id", "category_code",
        "brand", "price", "user_id", "user_session"]
EVENT_TYPES = ["view", "cart", "purchase"]
MONTH_START, MONTH_END = "2019-11-01 00:00:00", "2019-12-01 00:00:00"

# ---- quality policy (the gate reads this) ----------------------------------------
# FAIL       -> stop: publish does not run, published tables stay unchanged
# QUARANTINE -> row-level: the row is kept out of silver and saved with its reason (01_clean)
# WARN       -> recorded in gate.json, does not stop publication
POLICY = {
    "max_reject_rate": 0.01,          # FAIL: >1% invalid rows means the source or its format changed
    "max_category_missing": 0.40,     # WARN: Phase-0 measured ~32%
    "max_brand_missing": 0.20,        # WARN: Phase-0 measured ~14%
}

# ---- helpers ----------------------------------------------------------------------
def exists(path):
    try:
        dbutils.fs.ls(path); return True
    except Exception as e:
        if "FileNotFoundException" in str(e) or "PathNotFound" in str(e) or "does not exist" in str(e):
            return False
        raise

def write_json(path, obj):
    dbutils.fs.mkdirs(path.rsplit("/", 1)[0])
    dbutils.fs.put(path, json.dumps(obj, indent=2, default=str), overwrite=True)
    print(f"saved {path}")

def read_json(path):
    return json.loads(dbutils.fs.head(path, 10_000_000))

def fingerprint(df, cols=None):
    """Order-independent fingerprint of the full content: row count + sum of a 64-bit hash of every row.
    Two tables with the same rows (in any order) give the same pair; any changed value changes the sum."""
    cols = cols or df.columns
    r = df.select(F.count(F.lit(1)).alias("rows"),
                  F.sum(F.xxhash64(*[F.col(c) for c in sorted(cols)]).cast("decimal(38,0)")).alias("hash_sum")).first()
    return {"rows": int(r["rows"]), "hash_sum": str(r["hash_sum"])}

def publish_table(df, name, path, partition=None):
    """Full rebuild of a registered external Delta table - same pattern as Lab-6 03_publish step 6.
    First run: write the files and register the table in one step (mode "error" refuses a non-empty folder).
    Later runs: check location + schema, then replace the contents with insertInto(overwrite) (atomic, keeps history)."""
    if spark.catalog.tableExists(name):
        detail = spark.sql(f"DESCRIBE DETAIL {name}").first()
        if detail["location"].rstrip("/") != path.rstrip("/"):
            raise RuntimeError(f"{name} points to {detail['location']}, expected {path}")
        existing = spark.table(name)
        if existing.schema.simpleString() != df.select(*existing.columns).schema.simpleString():
            raise RuntimeError(f"Schema changed for {name}")
        df.select(*existing.columns).write.mode("overwrite").insertInto(name)
    else:
        w = df.write.format("delta").mode("error").option("path", path)
        (w.partitionBy(partition) if partition else w).saveAsTable(name)

def table_version(name, version):
    return spark.sql(f"SELECT * FROM {name} VERSION AS OF {int(version)}")

def require_new_run_dir():
    if exists(REPORTS) or exists(RUN_DIR):
        raise RuntimeError(f"Run folders for {RUN_ID} already exist. Start a fresh 'Run now' instead of repairing.")

def input_location():
    """Returns (csv_path, manifest_path) for this run's input."""
    if P["input"] == "test_bad":
        return f"{TEST_INPUT}/events.csv", f"{TEST_INPUT}/manifest.json"
    versions = [f.path.rstrip("/") for f in dbutils.fs.ls(RAW_BASE) if f.name.startswith("sha256=")]
    if len(versions) != 1:
        raise RuntimeError(f"Expected exactly one raw version under {RAW_BASE}, found {versions}")
    return f"{versions[0]}/2019-Nov.csv", f"{versions[0]}/_manifest/source-manifest.json"

print(json.dumps({**P, "silver_table": SILVER_TABLE, "features_table": FEATURES_TABLE}, indent=2))
