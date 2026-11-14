# Databricks notebook source
# MAGIC %md
# MAGIC # 04_features - target + point-in-time features, versioned output (task 4 of 4)
# MAGIC
# MAGIC **One row = one (user_session, product_id) pair at its first `cart` event** (the prediction moment).
# MAGIC **Target** `purchased` = 1 if a `purchase` of the same product follows in the same session (at or after the cart time).
# MAGIC
# MAGIC Every feature uses only events **strictly before** the cart time (or the session start, which is before it):
# MAGIC Spark windows with `rowsBetween(unboundedPreceding, -1)` / `rangeBetween(unboundedPreceding, -1)`.
# MAGIC Nothing from after the cart (later views, purchase time, session length) is a feature. Definitions: `docs/FEATURES.md`.
# MAGIC
# MAGIC Output: Delta table `project_features_v1` at `curated/project/features_v1` (feature version 1, history via Delta),
# MAGIC plus this run's `success.json`. With `compare_to` set, the run proves SAME CONTENT against that earlier run.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

publish = read_json(f"{REPORTS}/publish.json")      # exists only if 03_publish succeeded for this run
silver = spark.read.format("delta").option("versionAsOf", publish["silver_version"]).load(SILVER_PATH)

order_code = F.when(F.col("event_type") == "view", 0).when(F.col("event_type") == "cart", 1).otherwise(2)
ev = (silver.select("event_time", "event_type", "product_id", "user_id", "user_session", "price",
                    "category_l1", "brand_clean")
      .withColumn("epoch", F.unix_timestamp("event_time"))
      .withColumn("etype", order_code)
      .withColumn("is_view", (F.col("event_type") == "view").cast("int"))
      .withColumn("is_cart", (F.col("event_type") == "cart").cast("int"))
      .withColumn("is_purchase", (F.col("event_type") == "purchase").cast("int")))

# ---- session context, strictly earlier rows in the same session -------------------------
w_s_order = Window.partitionBy("user_session").orderBy("epoch", "etype", "product_id", "price")
w_s_prev  = w_s_order.rowsBetween(Window.unboundedPreceding, -1)
w_sp_prev = Window.partitionBy("user_session", "product_id").orderBy("epoch", "etype", "price") \
                  .rowsBetween(Window.unboundedPreceding, -1)
w_s_all   = Window.partitionBy("user_session")
ev_s = (ev
  .withColumn("session_start", F.min("epoch").over(w_s_all))
  .withColumn("is_session_start", (F.row_number().over(w_s_order) == 1).cast("int"))
  .withColumn("session_views_before", F.coalesce(F.sum("is_view").over(w_s_prev), F.lit(0)))
  .withColumn("session_carts_before", F.coalesce(F.sum("is_cart").over(w_s_prev), F.lit(0)))
  .withColumn("session_same_product_views_before", F.coalesce(F.sum("is_view").over(w_sp_prev), F.lit(0))))

# ---- user history, strictly earlier seconds, across all sessions --------------------------
w_u_prev = Window.partitionBy("user_id").orderBy("epoch").rangeBetween(Window.unboundedPreceding, -1)
w_u_upto = Window.partitionBy("user_id").orderBy("epoch").rangeBetween(Window.unboundedPreceding, 0)
ev_u = (ev_s
  .withColumn("user_prior_events", F.count(F.lit(1)).over(w_u_prev))
  .withColumn("user_prior_carts", F.coalesce(F.sum("is_cart").over(w_u_prev), F.lit(0)))
  .withColumn("user_prior_purchases", F.coalesce(F.sum("is_purchase").over(w_u_prev), F.lit(0)))
  .withColumn("_starts_before", F.coalesce(F.sum("is_session_start").over(w_u_prev), F.lit(0)))
  .withColumn("user_first_seen", F.min("epoch").over(w_u_upto)))

# first cart of each (session, product) = the prediction moment
w_first = Window.partitionBy("user_session", "product_id").orderBy("epoch", "price")
carts = (ev_u.filter("is_cart = 1")
  .withColumn("_rn", F.row_number().over(w_first)).filter("_rn = 1")
  .select("user_session", "product_id", "user_id", F.col("epoch").alias("cart_epoch"),
          F.col("event_time").alias("cart_ts"), "price", "category_l1", "brand_clean",
          "session_start", "session_views_before", "session_carts_before", "session_same_product_views_before",
          "user_prior_events", "user_prior_carts", "user_prior_purchases", "_starts_before", "user_first_seen"))

# ---- product history from cart/purchase events only (small subset) -------------------------
w_p_prev = Window.partitionBy("product_id").orderBy("epoch").rangeBetween(Window.unboundedPreceding, -1)
prod = (ev.filter("is_view = 0")
  .withColumn("product_prior_carts", F.coalesce(F.sum("is_cart").over(w_p_prev), F.lit(0)))
  .withColumn("product_prior_purchases", F.coalesce(F.sum("is_purchase").over(w_p_prev), F.lit(0)))
  .filter("is_cart = 1")
  .withColumn("_rn", F.row_number().over(w_first)).filter("_rn = 1")
  .select("user_session", "product_id", "product_prior_carts", "product_prior_purchases"))

# ---- label ---------------------------------------------------------------------------------
purch = (ev.filter("is_purchase = 1").groupBy("user_session", "product_id")
           .agg(F.max("epoch").alias("last_purchase_epoch")))

feat = (carts.join(prod, ["user_session", "product_id"], "left")
             .join(purch, ["user_session", "product_id"], "left")
  .withColumn("purchased", F.coalesce((F.col("last_purchase_epoch") >= F.col("cart_epoch")).cast("int"), F.lit(0)))
  .select(
    "user_session", "product_id", "user_id", "cart_ts", F.to_date("cart_ts").alias("cart_date"),
    F.col("price").cast("double").alias("price"),
    "category_l1",
    (F.col("brand_clean") != "unknown").cast("int").alias("brand_known"),
    F.hour("cart_ts").alias("hour_utc"),
    F.dayofweek("cart_ts").alias("weekday"),
    (F.col("cart_epoch") - F.col("session_start")).cast("long").alias("session_elapsed_sec"),
    F.col("session_views_before").cast("long").alias("session_views_before"),
    F.col("session_same_product_views_before").cast("long").alias("session_same_product_views_before"),
    F.col("session_carts_before").cast("long").alias("session_carts_before"),
    F.col("user_prior_events").cast("long").alias("user_prior_events"),
    F.greatest(F.col("_starts_before") - (F.col("session_start") < F.col("cart_epoch")).cast("int"), F.lit(0))
      .cast("long").alias("user_prior_sessions"),
    F.col("user_prior_carts").cast("long").alias("user_prior_carts"),
    F.col("user_prior_purchases").cast("long").alias("user_prior_purchases"),
    F.when(F.col("user_prior_carts") > 0, F.col("user_prior_purchases") / F.col("user_prior_carts"))
      .alias("user_prior_purchase_per_cart"),
    ((F.col("cart_epoch") - F.col("user_first_seen")) / 86400.0).alias("user_days_since_first_seen"),
    F.coalesce("product_prior_carts", F.lit(0)).cast("long").alias("product_prior_carts"),
    F.coalesce("product_prior_purchases", F.lit(0)).cast("long").alias("product_prior_purchases"),
    F.when(F.col("product_prior_carts") > 0, F.col("product_prior_purchases") / F.col("product_prior_carts"))
      .alias("product_prior_conv_rate"),
    "purchased"))

(feat.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(FEATURES_PATH))
spark.sql(f"CREATE TABLE IF NOT EXISTS {FEATURES_TABLE} USING DELTA LOCATION '{FEATURES_PATH}'")
spark.sql(f"ALTER TABLE {FEATURES_TABLE} SET TBLPROPERTIES ('project.feature_version'='{FEATURE_VERSION}', "
          f"'project.silver_version'='{publish['silver_version']}', 'project.last_run_id'='{RUN_ID}')")
spark.sql(f"""COMMENT ON TABLE {FEATURES_TABLE} IS
'Cart-to-purchase features v{FEATURE_VERSION}. One row per (user_session, product_id) at its first cart event; label purchased.
All features use only events strictly before cart_ts. Built from {SILVER_TABLE} version {publish["silver_version"]}. See docs/FEATURES.md.'""")

# COMMAND ----------

ft = spark.table(FEATURES_TABLE)
fhist = spark.sql(f"DESCRIBE HISTORY {FEATURES_TABLE} LIMIT 1").first()
lab = ft.groupBy("purchased").count().collect()
label_counts = {int(r["purchased"]): r["count"] for r in lab}

# purchases that have no earlier cart of the same product in the same session (excluded from the target)
pairs_p = purch.count()
pairs_p_with_cart = purch.join(carts.select("user_session", "product_id", "cart_epoch"), ["user_session", "product_id"]) \
                         .filter("last_purchase_epoch >= cart_epoch").count()
features_fp = fingerprint(ft)

success = {
  "run_id": RUN_ID, "silver_table": SILVER_TABLE, "silver_path": SILVER_PATH, "silver_version": publish["silver_version"],
  "silver_fingerprint": publish["silver_fingerprint"],
  "features_table": FEATURES_TABLE, "features_path": FEATURES_PATH, "features_version": int(fhist["version"]),
  "feature_set_version": FEATURE_VERSION, "features_fingerprint": features_fp,
  "label_counts": label_counts, "positive_rate": label_counts.get(1, 0) / max(features_fp["rows"], 1),
  "purchase_pairs": pairs_p, "purchase_pairs_without_prior_cart": pairs_p - pairs_p_with_cart,
  "finished_utc": datetime.datetime.utcnow().isoformat() + "Z",
}
write_json(f"{REPORTS}/success.json", success)
write_json(LATEST, success)
print(json.dumps(success, indent=2))

# ---- safe-rerun proof ------------------------------------------------------------------------
if P["compare_to"]:
    base = read_json(uri("curated", f"project/quality/runs/{P['compare_to']}/success.json"))
    cmp = {"run_id": RUN_ID, "compare_to": P["compare_to"],
           "silver_same": base["silver_fingerprint"] == success["silver_fingerprint"],
           "features_same": base["features_fingerprint"] == success["features_fingerprint"],
           "silver_versions": [base["silver_version"], success["silver_version"]],
           "features_versions": [base["features_version"], success["features_version"]],
           "baseline": {"silver": base["silver_fingerprint"], "features": base["features_fingerprint"]},
           "this_run": {"silver": success["silver_fingerprint"], "features": success["features_fingerprint"]}}
    write_json(f"{REPORTS}/comparison.json", cmp)
    print(json.dumps(cmp, indent=2))
    if not (cmp["silver_same"] and cmp["features_same"]):
        raise RuntimeError("DIFFERENT CONTENT after rerun - investigate before trusting the outputs")
    print(f"SAME CONTENT: silver v{cmp['silver_versions'][0]} == v{cmp['silver_versions'][1]}, "
          f"features v{cmp['features_versions'][0]} == v{cmp['features_versions'][1]} (rows + full-row hash)")
print("FEATURES PUBLISHED", FEATURES_TABLE, "version", success["features_version"])
