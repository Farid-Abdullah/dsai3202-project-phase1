# Databricks notebook source
# MAGIC %md
# MAGIC # 06_eda - exploratory analysis on the full published data (run by hand after a successful job run)
# MAGIC
# MAGIC **No sampling:** every number and plot below is an aggregate over the full published silver table
# MAGIC (all accepted November 2019 events) or the full feature table. Only small aggregated results are brought to the driver.
# MAGIC Results are also saved to `curated/project/eda/eda_summary.json`. Export this notebook as HTML (with outputs) for the evidence.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

import matplotlib.pyplot as plt
latest = read_json(LATEST)
silver = table_version(SILVER_TABLE, latest["silver_version"])
feat = table_version(FEATURES_TABLE, latest["features_version"])
print("silver version", latest["silver_version"], "| features version", latest["features_version"], "| run", latest["run_id"])
S = {"run_id": latest["run_id"], "silver_version": latest["silver_version"], "features_version": latest["features_version"]}

# COMMAND ----------

# MAGIC %md ## 1. Population counts

# COMMAND ----------

pop = silver.agg(F.count(F.lit(1)).alias("events"), F.countDistinct("user_id").alias("users"),
                 F.countDistinct("user_session").alias("sessions"), F.countDistinct("product_id").alias("products"),
                 F.countDistinct("category_l1").alias("top_categories"), F.countDistinct("brand").alias("brands")).first().asDict()
by_type = {r["event_type"]: r["count"] for r in silver.groupBy("event_type").count().collect()}
S["population"] = {**pop, "by_event_type": by_type}
print(json.dumps(S["population"], indent=2))

# COMMAND ----------

# MAGIC %md ## 2. Events per day (look for sale days such as 15-17 and 29 November)

# COMMAND ----------

daily = silver.groupBy("event_date").pivot("event_type", EVENT_TYPES).count().orderBy("event_date").toPandas()
S["daily"] = daily.astype({"event_date": str}).to_dict(orient="list")
fig, ax = plt.subplots(1, 2, figsize=(14, 4))
ax[0].plot(daily["event_date"], daily["view"] / 1e6); ax[0].set_title("views per day (millions)")
ax[1].plot(daily["event_date"], daily["cart"] / 1e3, label="cart"); ax[1].plot(daily["event_date"], daily["purchase"] / 1e3, label="purchase")
ax[1].set_title("carts and purchases per day (thousands)"); ax[1].legend()
for a in ax: a.tick_params(axis="x", rotation=60)
plt.tight_layout(); display(fig)

# COMMAND ----------

# MAGIC %md ## 3. Missing values

# COMMAND ----------

n = pop["events"]
miss = silver.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in COLS]).first().asDict()
S["missing_share"] = {c: miss[c] / n for c in COLS}
display(spark.createDataFrame([(c, miss[c], miss[c] / n) for c in COLS], "column string, nulls long, share double"))

# COMMAND ----------

# MAGIC %md ## 4. Price distribution and outliers

# COMMAND ----------

qs = [0.0, 0.01, 0.25, 0.5, 0.75, 0.99, 0.999, 1.0]
pq_ = silver.approxQuantile("price", qs, 0.0001)
S["price_quantiles"] = dict(zip(map(str, qs), pq_))
S["price_zero_rows"] = silver.filter("price = 0").count()
hist = (silver.withColumn("bucket", F.floor(F.log10(F.col("price") + 1) * 4) / 4)
              .groupBy("bucket").count().orderBy("bucket").toPandas())
fig, ax = plt.subplots(figsize=(8, 3.5)); ax.bar(hist["bucket"], hist["count"], width=0.23)
ax.set_xlabel("log10(price + 1)"); ax.set_ylabel("events"); ax.set_title("price distribution (all events)")
display(fig); print(S["price_quantiles"], "zero-price rows:", S["price_zero_rows"])

# COMMAND ----------

# MAGIC %md ## 5. Activity skew (possible bots)

# COMMAND ----------

per_user = silver.groupBy("user_id").agg(F.count(F.lit(1)).alias("events"))
uq = per_user.approxQuantile("events", [0.5, 0.9, 0.99, 0.9999, 1.0], 0.0001)
S["events_per_user_quantiles"] = dict(zip(["p50", "p90", "p99", "p99.99", "max"], uq))
top = per_user.orderBy(F.desc("events")).limit(10).toPandas()
S["top_users_events"] = top["events"].tolist()
S["share_of_events_top_0_1pct_users"] = (per_user.orderBy(F.desc("events"))
    .limit(max(int(pop["users"] * 0.001), 1)).agg(F.sum("events")).first()[0]) / n
print(json.dumps({k: S[k] for k in ["events_per_user_quantiles", "top_users_events", "share_of_events_top_0_1pct_users"]}, indent=2))

# COMMAND ----------

# MAGIC %md ## 6. Target: cart -> purchase in the same session

# COMMAND ----------

nf = feat.count()
pos = feat.filter("purchased = 1").count()
S["target"] = {"rows": nf, "positives": pos, "positive_rate": pos / nf,
               "purchase_pairs_without_prior_cart": latest["purchase_pairs_without_prior_cart"],
               "purchase_pairs": latest["purchase_pairs"]}
print(json.dumps(S["target"], indent=2))

def rate_by(col, top=None):
    d = feat.groupBy(col).agg(F.count(F.lit(1)).alias("rows"), F.avg("purchased").alias("conv_rate"))
    d = d.orderBy(F.desc("rows")).limit(top) if top else d.orderBy(d.columns[0])
    return d.toPandas()

cat = rate_by("category_l1", 12); hr = rate_by("hour_utc"); day = rate_by("cart_date")
vb = rate_by(F.least(F.col("session_views_before"), F.lit(20)).alias("views_before_capped_20"))
S["conv_by_category"] = cat.to_dict(orient="list"); S["conv_by_hour_utc"] = hr.to_dict(orient="list")
S["conv_by_day"] = day.astype({"cart_date": str}).to_dict(orient="list"); S["conv_by_views_before"] = vb.to_dict(orient="list")
fig, ax = plt.subplots(2, 2, figsize=(14, 8))
ax[0, 0].barh(cat["category_l1"], cat["conv_rate"]); ax[0, 0].set_title("conversion by top category")
ax[0, 1].plot(hr["hour_utc"], hr["conv_rate"], marker="o"); ax[0, 1].set_title("conversion by hour (UTC)")
ax[1, 0].plot(day["cart_date"], day["conv_rate"], marker="o"); ax[1, 0].set_title("conversion by day"); ax[1, 0].tick_params(axis="x", rotation=60)
ax[1, 1].plot(vb["views_before_capped_20"], vb["conv_rate"], marker="o"); ax[1, 1].set_title("conversion by views before cart (capped 20)")
plt.tight_layout(); display(fig)

# COMMAND ----------

# MAGIC %md ## 7. Correlation of numeric features with the target (full feature table, one pass)

# COMMAND ----------

num = [c for c, t in feat.dtypes if t in ("double", "bigint", "int", "long") and c not in ("product_id", "user_id", "purchased")]
corr = feat.agg(*[F.corr(F.col(c).cast("double"), F.col("purchased").cast("double")).alias(c) for c in num]).first().asDict()
nulls = feat.agg(*[F.avg(F.col(c).isNull().cast("int")).alias(c) for c in num]).first().asDict()
S["corr_with_target"] = corr; S["feature_null_share"] = nulls
display(spark.createDataFrame([(c, float(corr[c]) if corr[c] is not None else None, float(nulls[c])) for c in num],
                              "feature string, pearson_r_with_purchased double, null_share double").orderBy("feature"))

# COMMAND ----------

write_json(uri("curated", "project/eda/eda_summary.json"), S)
