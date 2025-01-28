--CREATE DATABASE fetchdb;

CREATE TABLE fetchdb.public.users (
    id TEXT PRIMARY KEY,
    active BOOLEAN NOT NULL,
    created_date TIMESTAMP NOT NULL,
    last_login TIMESTAMP,
    user_role TEXT NOT NULL,
    sign_up_source TEXT,
    state TEXT
);

CREATE TABLE fetchdb.public.brands (
    id TEXT PRIMARY KEY,
    barcode TEXT NOT NULL,
    category TEXT,
    category_code TEXT,
    cpg_id TEXT NOT NULL,
    cpg_ref TEXT NOT NULL,
    name TEXT NOT NULL,
    top_brand BOOLEAN,
    brand_code TEXT
);

CREATE TABLE fetchdb.public.receipts (
    id TEXT PRIMARY KEY,
    bonus_points_earned TEXT,
    bonus_points_earned_reason TEXT,
    create_date TIMESTAMP NOT NULL,
    date_scanned TIMESTAMP NOT NULL,
    finished_date TIMESTAMP,
    modify_date TIMESTAMP NOT NULL,
    points_awarded_date TIMESTAMP,
    points_earned TEXT,
    purchase_date TIMESTAMP,
    purchased_item_count TEXT,
    rewards_receipt_status TEXT NOT NULL,
    total_spent TEXT,
    user_id TEXT
);

CREATE TABLE fetchdb.public.receipt_items (
    id SERIAL PRIMARY KEY,
    receipt_id TEXT REFERENCES receipts (id) ON DELETE CASCADE,
    barcode TEXT,
    description TEXT,
    final_price TEXT,
    item_price TEXT,
    needs_fetch_review BOOLEAN,
    partner_item_id TEXT,
    prevent_target_gap_points BOOLEAN,
    quantity_purchased TEXT,
    user_flagged_barcode TEXT,
    user_flagged_new_item BOOLEAN,
    user_flagged_price TEXT,
    user_flagged_quantity TEXT,
    needs_fetch_review_reason TEXT,
    points_not_awarded_reason TEXT,
    points_payer_id TEXT,
    rewards_group TEXT,
    rewards_product_partner_id TEXT,
    user_flagged_description TEXT,
    original_meta_brite_barcode TEXT,
    original_meta_brite_description TEXT,
    brand_code TEXT,
    competitor_rewards_group TEXT,
    discounted_item_price TEXT,
    original_receipt_item_text TEXT,
    item_number TEXT,
    original_meta_brite_quantity_purchased TEXT,
    points_earned TEXT,
    target_price TEXT,
    competitive_product BOOLEAN,
    original_final_price TEXT,
    original_meta_brite_item_price TEXT,
    deleted BOOLEAN,
    price_after_coupon TEXT,
    metabrite_campaign_id TEXT
);
