# Feature set v1 - cart-to-purchase conversion

**Prediction moment:** The exact moment a product is added to the cart for the first time in a visit.
**Target:** 'purchased' is 1 if the shopper buys that same product later in the same visit, and 0 if they don't.

**Rule for every feature:** Features can only use information known *before* the item is added to the cart. In Spark, our window calculations stop one row or one second before the cart event so future clicks or purchases never leak into the features. We double-checked this on sample data and found zero mismatches.

The output is saved to Delta table 'project_features_v1' ('curated/project/features_v1') by 'pipeline/04_features.py'.

**Features (Feature, Type, Definition / source, Available at prediction time?, Missing-value policy):**
price, double, Item price shown when added to cart, Yes, Never missing (bad prices are removed in cleaning)
category_l1, string, Main product category (first word of category_code), Yes, Set to unknown when missing (~32% of rows)
brand_known, int 0/1, 1 if the brand name is listed and 0 if not, Yes, Set to 0 when missing
hour_utc, int, Hour of the day (0–23 in UTC) when added to cart, Yes, Never missing
weekday, int, Day of the week (1 = Sunday) when added to cart, Yes, Never missing
session_elapsed_sec, long, Seconds passed since the visit started, Yes, 0 if adding to cart was the very first action
session_views_before, long, Total items viewed earlier in this visit, Yes, 0
session_same_product_views_before, long, Times this specific product was viewed earlier in this visit, Yes, 0
session_carts_before, long, Other cart additions earlier in this visit, Yes, 0
user_prior_events, long, Total clicks by this shopper in earlier seconds across all visits, Yes, 0 for new shoppers
user_prior_sessions, long, Number of past visits by this shopper, Yes, 0
user_prior_carts, long, Number of items this shopper added to carts in the past, Yes, 0
user_prior_purchases, long, Number of past purchases by this shopper, Yes, 0
user_prior_purchase_per_cart, double, Ratio of past purchases to past cart additions for this shopper, Yes, Null if the shopper has no past carts (handled during model training)
user_days_since_first_seen, double, Days since this shopper was first seen in November, Yes, 0 on their first visit
product_prior_carts, long, Times this product was added to any cart in earlier seconds, Yes, 0
product_prior_purchases, long, Times this product was bought in earlier seconds, Yes, 0
product_prior_conv_rate, double, Ratio of past purchases to past cart additions for this product, Yes, Null if the product has no past carts

Identifier columns kept for tracking (**not used as model inputs**): 'user_session', 'product_id', 'user_id', 'cart_ts', 'cart_date'.

## Leakage risks and how they are handled

**Leakage handling (Risk, Handling):**
Counting actions that happen after the cart event, Never calculated; all visit summaries stop strictly before the cart row
Using whole-month product popularity, Replaced with a running conversion rate using only past seconds; baseline rates will only use training data
Looking at a shopper's future purchases, User history windows only look at timestamps strictly before the cart second
Mixing past and future days in a random split, Phase 2 splits data strictly by date order (60% train, 15% validation, 15% test, 10% holdout) and fits all imputers on training rows only
Events happening in the exact same second, Ordered cleanly (view < cart < purchase) so counts only include earlier actions

## Known limitations

- Because our data begins on November 1 without October history, shoppers look brand-new at the start of the month.
- Conversion rate features ('user_prior_purchase_per_cart' and 'product_prior_conv_rate') are blank for brand-new shoppers or products, which is useful information on its own.
- Timestamps are in UTC rather than the store's local time zone.
- Very heavy users (likely bots) are still in the data; Phase 2 will test whether filtering them out helps.