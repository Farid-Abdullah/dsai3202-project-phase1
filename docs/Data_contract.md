# Data contract, catalog and lineage

## Source

**Source details (Item, Agreement):**
Dataset, eCommerce behavior data from multi category store by REES46 Open CDP (Michael Kechinov)
Source URL, https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store
Terms, Free to use with credit to Kaggle and REES46 Marketing Platform
File used, 2019-Nov.csv only (October data is not used)
Format, Comma-separated CSV with a header row, UTF-8 text, UTC timestamps
Size, 9,006,762,395 bytes uncompressed (2,874,125,107 bytes zipped)
Rows, 67,501,979 data rows plus 1 header row
Identity, SHA-256 hash addd9a27ed99abdece368019ebfba19568972c7fcbe949db3484aab8a3bcffec (saved in source-manifest.json)
Ingestion mode, Manual batch script (ingest/ingest.sh) run on the course VM from Kaggle to Azure storage
Refresh strategy, Static historical file; any new file gets a new hash folder and never overwrites old data
Access, Secured via Microsoft Entra login and Unity Catalog grants (no storage keys or public access)
Retention, Raw files stay untouched until the course ends; run logs are kept as proof

## Record meaning and keys

- **One record = one shopper event:** Each row is a single 'view', 'cart', or 'purchase' of one product during one visit at a specific second.
- **Handling duplicates:** The raw file has no unique event ID. If two rows have identical values across all nine columns, we treat them as a double-logged event, keep one copy, and log the duplicate count.
- **IDs:** 'user_session' tracks one website visit, 'user_id' tracks one shopper, and 'product_id' tracks one item. All IDs are anonymous numbers with no personal details.
- **Prediction row:** In the features table, each row represents the **first** time a specific product was added to the cart in a specific session ('user_session', 'product_id').

## Schema

**Table schema (Column, Source type, Silver type, Nulls, Notes):**
event_time, text, timestamp (UTC), rejected, Must fall between Nov 1 and Nov 30, 2019
event_type, text, string, rejected, Only view, cart, or purchase allowed
product_id, text, bigint, rejected, Part of the main key
category_id, text, bigint, allowed, Numeric category ID
category_code, text, string, allowed (~32%), Text path like electronics.smartphone
brand, text, string (lower-case), allowed (~14%), Brand name in lowercase
price, text, double, rejected, Must be 0 or higher ($0 items are kept and counted)
user_id, text, bigint, rejected, Anonymous shopper ID
user_session, text, string, rejected, Anonymous visit ID
event_date, none, date, none, Added during cleaning to partition the table by day
category_l1, none, string, none, First word of category_code, or unknown if missing
brand_clean, none, string, none, Clean brand name, or unknown if missing

## Assumptions

- All timestamps are in UTC, so time-of-day features reflect UTC rather than the shopper's local clock.
- A purchase counts toward a cart if it happens in the same session for the same product at or after the first cart addition.
- If a shopper buys an item without adding it to the cart first, it is excluded from our target table and counted in 'success.json'.
- If multiple events happen in the exact same second, we order them as 'view', then 'cart', then 'purchase' so "before this event" counts stay consistent.
- Because our data starts on November 1, shoppers who visited in October look like brand-new users at the start of the month.

## Storage layout (lake 'amazonlake60304642')

- 'raw/rees46/2019-Nov/sha256=addd9a27ed99/' — Holds the untouched original '2019-Nov.csv' and 'source-manifest.json'.
- 'processed/project/landing/rees46/2019-Nov/sha256=addd9a27ed99/' — Verified copy of the CSV that Databricks can read.
- 'processed/project/test_inputs/bad_v1/' — Our intentionally broken test file ('events.csv') used to test pipeline failure rules.
- 'processed/project/runs/run_id=<id>/' — Working folders for each run ('bronze_checked', 'rejects', 'duplicate_groups', and 'silver_staging').
- 'processed/project/silver_events/' — The clean, published Delta table ('project_silver_events') split by date.
- 'curated/project/features_v1/' — The final machine learning feature table ('project_features_v1').
- 'curated/project/quality/' and 'curated/project/eda/' — Holds JSON quality reports ('clean.json', 'gate.json', 'publish.json', 'success.json', 'comparison.json') and EDA summaries.

## Catalog entries (Unity Catalog)

**Unity Catalog tables (Table, Location, Grain, Registered by):**
dsai3202_databricks.s_60304642.project_silver_events, processed/project/silver_events, One unique shopping event, 03_publish (includes column notes and source hash)
dsai3202_databricks.s_60304642.project_features_v1, curated/project/features_v1, One first-cart addition per session and product, 04_features (includes feature and silver versions)

Both are external Delta tables, meaning every update creates a new version and older versions can still be queried anytime using 'VERSION AS OF'.

## Lineage

1. **Kaggle to VM:** Download '2019-Nov.csv.zip', unzip it, and verify its SHA-256 fingerprint ('addd9a27...') via 'ingest.sh'.
2. **VM to Raw Storage:** Upload to 'raw/rees46/2019-Nov/sha256=addd9a27ed99/' using AzCopy without overwriting existing files.
3. **Raw to Landing:** Copy the verified CSV into 'processed/project/landing/' so Databricks can access it.
4. **Clean ('01_clean'):** Read the landing CSV once into 'bronze_checked', separate out bad rows ('rejects') and duplicates ('duplicate_groups'), and save clean rows to 'silver_staging'.
5. **Validate ('02_validate'):** Check all quality rules and write 'gate.json'.
6. **Publish ('03_publish'):** If quality checks pass, write the data into 'project_silver_events'.
7. **Features ('04_features'):** Read that exact silver table version and build 'project_features_v1'.

Every 'success.json' file links the run ID, silver version, and feature version back to the raw file's fingerprint, and Unity Catalog displays this flow visually in its Lineage tab.