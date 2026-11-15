# Cart-to-purchase conversion on Azure Phase 1: data pipeline, ETL and features

DSAI3202 Cloud Computing, Fall 2026 | Farid Abdul Rehman (60304642) | Instructor: Dr. Elyor Kodirov

## Project and hypothesis

- **Problem and user:** When a shopper adds an item to their cart, we want to predict how likely they are to buy it during that same visit. Online store marketing teams can use this to send discounts to people likely to leave without buying, while avoiding unnecessary discounts for people who were going to buy anyway.
- **Prediction target:** We look at the first time a shopper adds a specific product to their cart during a visit. If they buy that product later in the same visit, the target ('purchased') is 1; otherwise, it is 0.
- **Hypothesis (H1):** Looking at what a shopper is doing right now and what they did in past visits predicts purchases better than just guessing based on how popular a product usually is. We expect our XGBoost model to beat the basic baseline by at least **0.05** in ROC-AUC score, with consistent gains every day, including Black Friday (Nov 29).
- **Baseline and metric:** For our baseline, we simply score each cart using how often that product was bought in the training data (or the category average for brand-new products). We measure success using ROC-AUC, PR-AUC, F1, and accuracy because far more carts are abandoned than purchased.
- **Approved changes:** The core topic and goal have not changed. In Phase 0, we briefly mentioned using an October file, but we are strictly using **November 2019 data only**, which matches our split plan. We also upgraded from basic Parquet files to Delta tables so we can track versions and update data safely.

## Dataset and architecture

- **Source:** We are using the eCommerce behavior dataset from Kaggle (provided by REES46 Open CDP), which is free to use with credit. The file '2019-Nov.csv' is **9.0 GB uncompressed** (2.87 GB zipped) and holds **67,501,979 shopping events**.
- **Raw version and source receipt:** We saved the raw file at 'raw/rees46/2019-Nov/sha256=addd9a27ed99/2019-Nov.csv' on November 14, 2026. Because the folder name uses the file's unique digital fingerprint (SHA-256 hash 'addd9a27ed99abdece368019ebfba19568972c7fcbe949db3484aab8a3bcffec'), the exact same file always goes to the same place, while any new version gets its own folder. Details are saved in 'evidence/ingestion/source-manifest.json'.
- **Upload verified:** After uploading the file to Azure, we read it back and checked its size and fingerprint. Everything matched 100% ('raw-verify.json').
- **Repeat-ingestion evidence:** When we ran the upload script a second time, it safely skipped the file because it was already there. The before-and-after file logs ('receipts-before.tsv', 'receipts-after.tsv', and 'retry-proof.txt') match completely.
- **Storage layout:** Data moves cleanly from the raw folder to a landing copy, then through temporary run folders ('bronze', 'rejects', 'staging'), and finally into our clean silver table ('project_silver_events') and feature table ('project_features_v1'). Everything is kept under 'rees46/' and 'project/' folders so it never mixes with our class lab files.
- **Schema, keys and assumptions:** Full details are listed in 'docs/DATA_CONTRACT.md'.
- **Catalog and lineage:** Both final tables are registered in Unity Catalog with descriptions and version tracking. Screenshots of the catalog and job runs are saved in 'evidence/screenshots/'.

**Azure tools used (Service, Role):**
Azure VM (Lab-2 Ubuntu VM), Downloads the data from Kaggle, unzips it, checks the file fingerprint, and uploads it to Azure
ADLS Gen2 (Qatar Central), Stores all raw files, working folders, Delta tables, and proof logs
AzCopy + Azure CLI, Moves files securely using our student login without needing storage keys
Azure Databricks, Runs Spark to clean data, check quality, publish tables, and build features
Lakeflow Jobs, Connects our four steps in order and stops the pipeline if a quality check fails
Unity Catalog, Organizes the tables, stores descriptions, manages permissions, and tracks data history

## Run the pipeline

1. **Versions:** The final code is on the 'main' branch under the tag 'phase1-submission'. In Databricks, we use Runtime 18.x Photon on cluster 'dsai3202-project-60304642' (2 workers, auto-shuts off after 60 minutes). The Ubuntu VM runs Python 3, Azure CLI, AzCopy 10, and the Kaggle tool.
2. **Azure resources and permissions:** We reused our existing lab resources without needing any new permissions. Our student account can read and write to storage account 'amazonlake60304642'. In Unity Catalog ('dsai3202_databricks', schema 's_60304642'), Databricks can write to the 'processed' and 'curated' folders, **but it is blocked from touching 'raw'**. This protects the original file from accidental edits, so our script places a verified copy in 'processed' for Databricks to read. Our Kaggle API key stays safely on the VM in '~/.kaggle/access_token'.
3. **Ingest (VM):** Log into the VM, load the lab environment ('source ~/.config/dsai3202/lab3.env'), sign into Azure ('az login --use-device-code'), and run 'bash ingest/ingest.sh all'. This downloads, extracts, verifies, and uploads the data, saving logs to 'evidence/ingestion/'. You can also run single steps safely whenever needed.
4. **Import and configure the job (Databricks):** Upload the 'pipeline/*.py' scripts into a Databricks folder (like 'project/'). Set up a Lakeflow job with four steps in a row: 'clean' -> 'validate' -> 'publish' -> 'features'. Set each step to run only if the previous one succeeds, with 0 retries, on our existing cluster. Templates are in 'pipeline/job.template.json' and 'evidence/job-export.json'.

**Job parameters (Parameter, Value, Meaning):**
lake, amazonlake60304642, The Azure storage account name
catalog / schema, dsai3202_databricks / s_60304642, Where the tables live in Unity Catalog
run_id, {{job.run_id}}, Tags all folders and logs for that specific run
input, full or test_bad, Chooses between the real dataset or the broken test file
source_container, processed, Tells Databricks to read the landing copy since raw is locked
compare_to, empty or a past run ID, Compares outputs against an older run to prove they match

5. **Success run:** Click *Run now*. You should see 'BALANCED', 'QUALITY GATE PASSED', 'PUBLISHED', and 'FEATURES PUBLISHED'.
6. **Safe rerun:** Run the job again, setting 'compare_to' to the ID of your first run. The '04_features' step will check the row counts and data fingerprints and print 'SAME CONTENT'.
7. **Controlled failure:** Run 'pipeline/05_make_test_input' once to create a 50,000-row test file where every 20th row is broken ('processed/project/test_inputs/bad_v1/'). Then run the job with 'input = test_bad'. The 'clean' step will catch 2,500 bad rows, and 'validate' will stop the pipeline because the 5% error rate breaks our 1% limit. Nothing gets published, and our live tables stay untouched.
8. **EDA:** Run 'pipeline/06_eda' manually to explore the full published dataset.

## Results and features

**Pipeline runs (Run, Run ID, Input version, Output version/path, Result and evidence):**
Success, 497911063048649, Landing copy of sha256=addd9a27ed99, silver v14 and features v2, Passed all checks and published cleanly (logs in evidence/runs/)
Safe rerun, 926454798811512 (compare_to = 497911063048649), Same landing copy, silver v29 and features v5, Printed SAME CONTENT: exact match on all 67,401,450 silver rows and 2,053,770 feature rows (comparison.json)
Controlled failure, 674607781896911 (input = test_bad), test_inputs/bad_v1 (50,000 rows), None published, Caught 2,500 bad rows (5% error rate > 1% limit) and stopped before publishing; tables stayed at silver v29 and features v5 (gate.json)

- **Row accounting:** In our main run ('497911063048649'), we read **67,501,979 rows**. Out of those, **67,401,450 were clean**, **10 were broken** (all missing a 'user_session'), and **100,519 were exact duplicates**. The clean data has 63,554,512 views, 2,930,008 cart additions, and 916,930 purchases across November 2019. About 32.45% of rows are missing a category, 13.66% are missing a brand, and 0.28% (187,987 rows) have a price of zero.

**Quality rules (Rule, Policy, Rationale):**
rows read = accepted + invalid + duplicate copies, FAIL, Makes sure no rows get lost or created by accident
rows read = manifest data_rows, FAIL, Catches incomplete downloads or cut-off files
reject rate <= 1%, FAIL, Stops the pipeline if the source format suddenly breaks
only view / cart / purchase events exist, FAIL, Ensures our target variable works properly
all events in November 2019 UTC, FAIL, Protects our date-based train/test split
no missing IDs in clean data, FAIL, We need user, session, and product IDs to join tables
category missing <= 40% and brand missing <= 20%, WARN, Alerts us if missing text fields spike beyond normal levels
zero prices, WARN (counted), Keeps $0 items in the data so we can study them
invalid rows, QUARANTINE, Saves broken rows in a rejects folder so they never reach live tables

- **EDA:** We analyzed the entire published dataset without sampling ('evidence/06_eda.html' and 'evidence/eda/'):
  - **Population:** 67,401,450 events from 3,696,117 shoppers across 13,776,050 visits and 190,662 products. Views make up 94.3% of events, carts 4.3%, and purchases 1.4%.
  - **Missing values:** Only category (32.4%) and brand (13.7%) have missing values; all IDs, dates, and prices are complete.
  - **Price:** The median price is 165.77, and the highest price is capped at 2,574.07. The 187,987 zero-price rows are kept and flagged.
  - **Activity skew:** Most shoppers only have about 5 events, while the most active 0.1% of users generate 3.9% of all traffic (up to 22,925 events for one user), which are likely bots.
  - **Daily volume:** Traffic triples during a massive sale on **Nov 15–17** (jumping from 2M to 6M daily views), whereas Black Friday (Nov 29) only shows a tiny bump. Crucially, **purchases stop being logged from Nov 14 to Nov 16** even though views and carts continue. Also, daily cart events jump from ~20k to ~70k starting **Nov 8** while purchases stay flat.
  - **Target:** Out of 2,053,770 first-cart additions, **34.7% lead to a purchase**. Electronics convert best (~39%), conversion peaks around noon UTC (43%), and shoppers who view an item 20+ times before adding it to their cart are less likely to buy (27%) than those who add it right away (43%). Conversion drops to nearly **0% on Nov 15** due to the missing purchase logs.
  - **Correlations with the target:** A shopper's past purchase-per-cart rate has the strongest positive link (0.29), followed by their past purchase count (0.16). Day of the week shows a misleading negative link (-0.24) because of the broken Nov 14–16 logs.
- **Modeling readiness:** Our data is clean, and what we found in EDA shapes how we will build models in Phase 2:
  1. **Missing purchase logs (Nov 14–16):** Since purchases weren't recorded during these three days, carts look abandoned even when they weren't. We will exclude Nov 14–16 from training and testing in Phase 2, and add a quality rule to warn if a day has carts with zero purchases.
  2. **Day-of-week quirk:** Because the Nov 14–16 outage fell on Thursday–Saturday, it skewed the weekday numbers. We will re-check weekday patterns once those days are removed.
  3. **Nov 8 cart jump:** The jump in logged carts on Nov 8 lowers the average conversion rate from ~52% to ~43%. Our time-based split keeps this shift out of our test set, and we will calculate baseline rates using training data only.
  4. **Bots and heavy users:** We kept the top 0.1% heaviest users for now, and in Phase 2 we will test if removing them improves the model. Since 34.7% of carts convert, ROC-AUC and PR-AUC are solid metrics. We will also handle missing values in the historical rate columns during training. Finally, while we will still check Black Friday, Nov 15–17 is clearly the main sale event here.
- **Features:** We built 18 features covering cart details, current visit activity, shopper history, and product history, using only events that happened strictly before the cart addition ('docs/FEATURES.md'). Out of **2,053,770 cart rows**, **713,140 were purchased (34.7%)**. Another 144,744 purchases happened without a cart event first (16.9% of all purchase pairs), so they are excluded.
- **Version numbers:** Delta counts table descriptions and property updates as separate versions. That is why Run 1 finished at silver v14 and features v2. Every run logs its exact version numbers in 'success.json'.
- **Saved feature version:** The features live in Unity Catalog at 'dsai3202_databricks.s_60304642.project_features_v1' ('curated/project/features_v1').
- **Leakage risks and planned split:** In Phase 2, we will split the data chronologically by 'cart_date' and fit all data-cleaning steps on the training set only so future data never leaks backward ('docs/FEATURES.md').

## Resource use and contributions

- **Cost/performance choices:**
  - *Running ingestion on the Azure VM:* Downloading from Kaggle took 3 min 18 s, and uploading the 9 GB file to Azure took just **42 seconds**. Doing this from a home laptop would have taken hours.
  - *Reading the CSV only once:* The 'clean' script scans the 9 GB CSV once and saves all rows with status tags ('bronze_checked'), making it fast to split out bad rows and duplicates. The entire 4-step pipeline takes about **9 minutes** on our 2-worker cluster.
  - *Reusing one cluster:* Running all four tasks on one active cluster avoids waiting several minutes for new clusters to boot up between steps.
- **Cleanup and restart:** When finished, we stop the Databricks cluster and deallocate the VM to save credits, while keeping the storage files for Phase 2. Local temporary files on the VM ('~/project-ingest/') can be deleted. To restart, just turn the VM and cluster back on and run the scripts again.
- **Contributions:** Individual project by Farid Abdul Rehman (handled all ingestion, Spark scripts, runs, analysis, and writing).
- **Reused code:** Built on top of our DSAI3202 Lab-3, Lab-5, and Lab-6 templates. We customized them to parse text-based CSVs safely, remove duplicate events without a unique ID, stop the job if error rates pass 1%, generate a broken test file, calculate point-in-time shopping history, and compare 67-million-row runs using fast hash sums.

## AI use
pending