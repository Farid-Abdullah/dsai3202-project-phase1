# Feature set v1 - cart-to-purchase conversion

**Prediction moment:** the first `cart` event of a product in a session. **Target:** `purchased` = 1 if the same product is purchased later in the same session (at or after the cart time), else 0.

**Rule for every feature:** it may use only information that exists *before* the cart event. Spark windows end one row (`rowsBetween(..., -1)`) or one second (`rangeBetween(..., -1)`) before the cart, so the cart itself, later views and the purchase can never leak in. Verified on synthetic data against a brute-force recomputation (0 mismatches over all rows).

Output: Delta table `project_features_v1` at `curated/project/features_v1` (table property `project.feature_version = 1`). Built by `pipeline/04_features.py` from a fixed silver version.

| Feature | Type | Definition / source | Available at prediction time? | Missing-value policy |
|---|---|---|---|---|
| price | double | price on the first cart event (silver.price) | yes, shown on the cart event | never missing (null/negative rows rejected in 01_clean) |
| category_l1 | string | first segment of category_code | yes, product attribute | `unknown` when category_code is missing (~32% of events) |
| brand_known | int 0/1 | brand is present | yes | 0 when brand missing |
| hour_utc | int | hour of cart time (UTC) | yes | never missing |
| weekday | int | day of week of cart time (1 = Sunday) | yes | never missing |
| session_elapsed_sec | long | cart time − first event time of the session | yes, session start is earlier | 0 for a session that starts with the cart |
| session_views_before | long | views earlier in the same session | yes | 0 |
| session_same_product_views_before | long | views of this product earlier in the session | yes | 0 |
| session_carts_before | long | cart events earlier in the session (any product) | yes | 0 |
| user_prior_events | long | the user's events in earlier seconds (all sessions) | yes | 0 for a new user |
| user_prior_sessions | long | the user's sessions started before this session | yes | 0 |
| user_prior_carts | long | the user's cart events in earlier seconds | yes | 0 |
| user_prior_purchases | long | the user's purchases in earlier seconds | yes | 0 |
| user_prior_purchase_per_cart | double | user_prior_purchases / user_prior_carts | yes | **null** when the user has no earlier cart (a model must impute, e.g. with the training mean, or use a missing indicator) |
| user_days_since_first_seen | double | days since the user's first November event | yes | 0 for the first event |
| product_prior_carts | long | carts of this product (all users) in earlier seconds | yes | 0 |
| product_prior_purchases | long | purchases of this product in earlier seconds | yes | 0 |
| product_prior_conv_rate | double | product_prior_purchases / product_prior_carts | yes | **null** when the product has no earlier cart |

Identifier and bookkeeping columns, **not model inputs:** `user_session`, `product_id`, `user_id`, `cart_ts`, `cart_date`.

## Leakage risks and how they are handled

| Risk | Handling |
|---|---|
| Counting session activity after the cart (total views, session length, later removal) | not computed; all session windows end before the cart row |
| Product popularity computed over the whole month (includes the future) | replaced by a running rate over earlier seconds only (`product_prior_conv_rate`); the Phase-0 baseline must likewise be fit on the training period only |
| A user's later purchases | user windows use `rangeBetween(unbounded, -1)` on epoch seconds |
| Random train/test split mixing days | Phase 2 uses a **time-based split by `cart_date`** (Phase-0 plan: first 60% train, next 15% validation, next 15% test, last 10% held back); encoders and imputers are fit on the training rows only |
| Same-second ties | explicit ordering (view < cart < purchase); counts use strictly earlier rows/seconds |

## Known limitations

- History starts on 1 November (no October data): early-month carts have little user and product history.
- `user_prior_purchase_per_cart` and `product_prior_conv_rate` are null for new users/products; the null itself is informative.
- `hour_utc` is UTC; the shop's local time is not given in the source.
- Heavy-activity users (possible bots) are kept; the EDA reports their share so Phase 2 can decide on filtering.
