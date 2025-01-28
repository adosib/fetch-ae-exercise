"""
This script loads the data into the database.
Expects that the database is set up already (see ddl.sql)
and that there are unzipped JSON source files at the project root subfolder data/ i.e.:
- data/users.json
- data/brands.json
- data/receipts.json

To generate those files, you can run infer_schema.py which will additionally create
the JSON schema files:
- schemas/users_schema.json
- schemas/brands_schema.json
- schemas/receipts_schema.json
"""

import sys
import json
import logging
from pathlib import Path
from datetime import datetime

from psycopg import Connection, sql
from infer_schema import decode

THIS_DIR = Path(__file__).parent
ROOT_DIR = (THIS_DIR / "..").resolve()
DATA_DIR = ROOT_DIR / "data"

# Database connection parameters
DB_CONFIG = {
    "dbname": "fetchdb",
    "user": "postgres",
    "password": "",
    "host": "localhost",
    "port": "5432",
}


def insert_user(cursor, json_obj):
    insert_stmt = sql.SQL(
        "INSERT INTO users (id, active, created_date, last_login, user_role, sign_up_source, state) "
        "VALUES (%(id)s, %(active)s, %(created_date)s, %(last_login)s, %(user_role)s, %(sign_up_source)s, %(state)s) "
    )
    cursor.execute(
        insert_stmt,
        {
            "id": json_obj["_id"]["$oid"],  # Extract the $oid value
            "active": json_obj.get("active"),
            "created_date": parse_timestamp(json_obj.get("createdDate")),
            "last_login": parse_timestamp(json_obj.get("lastLogin")),
            "user_role": json_obj.get("role"),
            "sign_up_source": json_obj.get("signUpSource"),
            "state": json_obj.get("state"),
        },
    )


def insert_brand(cursor, json_obj):
    insert_stmt = sql.SQL(
        "INSERT INTO brands (id, barcode, category, category_code, cpg_id, cpg_ref, name, top_brand, brand_code) "
        "VALUES (%(id)s, %(barcode)s, %(category)s, %(category_code)s, %(cpg_id)s, %(cpg_ref)s, %(name)s, %(top_brand)s, %(brand_code)s) "
    )
    cursor.execute(
        insert_stmt,
        {
            "id": json_obj["_id"]["$oid"],
            "barcode": json_obj.get("barcode"),
            "category": json_obj.get("category"),
            "category_code": json_obj.get("categoryCode"),
            "cpg_id": json_obj.get("cpg", {}).get("$id", {}).get("$oid"),
            "cpg_ref": json_obj.get("cpg", {}).get("$ref", {}),
            "name": json_obj.get("name"),
            "top_brand": json_obj.get("topBrand"),
            "brand_code": json_obj.get("brandCode"),
        },
    )


def insert_receipt(cursor, json_obj):
    insert_stmt = sql.SQL(
        "INSERT INTO receipts (id, bonus_points_earned, bonus_points_earned_reason, create_date, date_scanned, finished_date, modify_date, points_awarded_date, points_earned, purchase_date, purchased_item_count, rewards_receipt_status, total_spent, user_id) "
        "VALUES (%(id)s, %(bonus_points_earned)s, %(bonus_points_earned_reason)s, %(create_date)s, %(date_scanned)s, %(finished_date)s, %(modify_date)s, %(points_awarded_date)s, %(points_earned)s, %(purchase_date)s, %(purchased_item_count)s, %(rewards_receipt_status)s, %(total_spent)s, %(user_id)s) "
    )
    cursor.execute(
        insert_stmt,
        {
            "id": json_obj["_id"]["$oid"],  # Extract the $oid value
            "bonus_points_earned": json_obj.get("bonusPointsEarned"),
            "bonus_points_earned_reason": json_obj.get("bonusPointsEarnedReason"),
            "create_date": parse_timestamp(json_obj.get("createDate")),
            "date_scanned": parse_timestamp(json_obj.get("dateScanned")),
            "finished_date": parse_timestamp(json_obj.get("finishedDate")),
            "modify_date": parse_timestamp(json_obj.get("modifyDate")),
            "points_awarded_date": parse_timestamp(json_obj.get("pointsAwardedDate")),
            "points_earned": json_obj.get("pointsEarned"),
            "purchase_date": parse_timestamp(json_obj.get("purchaseDate")),
            "purchased_item_count": json_obj.get("purchasedItemCount"),
            "rewards_receipt_status": json_obj.get("rewardsReceiptStatus"),
            "total_spent": json_obj.get("totalSpent"),
            "user_id": json_obj.get("userId"),
        },
    )


def insert_receipt_items(cursor, receipt_id, items):
    insert_stmt = """
        INSERT INTO receipt_items (receipt_id, barcode, description, final_price, item_price, needs_fetch_review, partner_item_id, prevent_target_gap_points, quantity_purchased, user_flagged_barcode, user_flagged_new_item, user_flagged_price, user_flagged_quantity, needs_fetch_review_reason, points_not_awarded_reason, points_payer_id, rewards_group, rewards_product_partner_id, user_flagged_description, original_meta_brite_barcode, original_meta_brite_description, brand_code, competitor_rewards_group, discounted_item_price, original_receipt_item_text, item_number, original_meta_brite_quantity_purchased, points_earned, target_price, competitive_product, original_final_price, original_meta_brite_item_price, deleted, price_after_coupon, metabrite_campaign_id) 
        VALUES (%(receipt_id)s, %(barcode)s, %(description)s, %(final_price)s, %(item_price)s, %(needs_fetch_review)s, %(partner_item_id)s, %(prevent_target_gap_points)s, %(quantity_purchased)s, %(user_flagged_barcode)s, %(user_flagged_new_item)s, %(user_flagged_price)s, %(user_flagged_quantity)s, %(needs_fetch_review_reason)s, %(points_not_awarded_reason)s, %(points_payer_id)s, %(rewards_group)s, %(rewards_product_partner_id)s, %(user_flagged_description)s, %(original_meta_brite_barcode)s, %(original_meta_brite_description)s, %(brand_code)s, %(competitor_rewards_group)s, %(discounted_item_price)s, %(original_receipt_item_text)s, %(item_number)s, %(original_meta_brite_quantity_purchased)s, %(points_earned)s, %(target_price)s, %(competitive_product)s, %(original_final_price)s, %(original_meta_brite_item_price)s, %(deleted)s, %(price_after_coupon)s, %(metabrite_campaign_id)s)
    """
    for item in items:
        cursor.execute(
            insert_stmt,
            {
                "receipt_id": receipt_id,
                "barcode": item.get("barcode"),
                "description": item.get("description"),
                "final_price": item.get("finalPrice"),
                "item_price": item.get("itemPrice"),
                "needs_fetch_review": item.get("needsFetchReview"),
                "partner_item_id": item.get("partnerItemId"),
                "prevent_target_gap_points": item.get("preventTargetGapPoints"),
                "quantity_purchased": item.get("quantityPurchased"),
                "user_flagged_barcode": item.get("userFlaggedBarcode"),
                "user_flagged_new_item": item.get("userFlaggedNewItem"),
                "user_flagged_price": item.get("userFlaggedPrice"),
                "user_flagged_quantity": item.get("userFlaggedQuantity"),
                "needs_fetch_review_reason": item.get("needsFetchReviewReason"),
                "points_not_awarded_reason": item.get("pointsNotAwardedReason"),
                "points_payer_id": item.get("pointsPayerId"),
                "rewards_group": item.get("rewardsGroup"),
                "rewards_product_partner_id": item.get("rewardsProductPartnerId"),
                "user_flagged_description": item.get("userFlaggedDescription"),
                "original_meta_brite_barcode": item.get("originalMetaBriteBarcode"),
                "original_meta_brite_description": item.get(
                    "originalMetaBriteDescription"
                ),
                "brand_code": item.get("brandCode"),
                "competitor_rewards_group": item.get("competitorRewardsGroup"),
                "discounted_item_price": item.get("discountedItemPrice"),
                "original_receipt_item_text": item.get("originalReceiptItemText"),
                "item_number": item.get("itemNumber"),
                "original_meta_brite_quantity_purchased": item.get(
                    "originalMetaBriteQuantityPurchased"
                ),
                "points_earned": item.get("pointsEarned"),
                "target_price": item.get("targetPrice"),
                "competitive_product": item.get("competitiveProduct"),
                "original_final_price": item.get("originalFinalPrice"),
                "original_meta_brite_item_price": item.get(
                    "originalMetaBriteItemPrice"
                ),
                "deleted": item.get("deleted"),
                "price_after_coupon": item.get("priceAfterCoupon"),
                "metabrite_campaign_id": item.get("metabriteCampaignId"),
            },
        )


def process_receipt(cursor, json_obj):
    insert_receipt(cursor, json_obj)
    if "rewardsReceiptItemList" in json_obj and json_obj["rewardsReceiptItemList"]:
        insert_receipt_items(
            cursor,
            json_obj["_id"]["$oid"],
            json_obj["rewardsReceiptItemList"],
        )


PROCESSOR_MAP = {  # NOTE: Order matters
    DATA_DIR / "users.json": insert_user,
    DATA_DIR / "brands.json": insert_brand,
    DATA_DIR / "receipts.json": process_receipt,
}


def process_and_insert_data(file_path, process_fn):
    for i, json_obj in enumerate(decode(file_path)):
        try:
            process_fn(dbcur, json_obj)
            dbconn.commit()
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {e}. Skipping line {i}")
        except Exception as e:
            logger.error(f"Error processing line {i}: {e}")
            dbconn.rollback()


# Function to parse timestamps
def parse_timestamp(timestamp_obj):
    if timestamp_obj and "$date" in timestamp_obj:
        # Convert milliseconds to seconds
        return datetime.fromtimestamp(timestamp_obj["$date"] / 1000)
    return None


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)  # Log INFO and above to console
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(str(ROOT_DIR / "logs" / "load_db.log"))
    file_handler.setLevel(logging.DEBUG)  # Log DEBUG and above to file
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    with Connection.connect(**DB_CONFIG) as dbconn:
        with dbconn.cursor() as dbcur:
            for file_path, process_fn in PROCESSOR_MAP.items():
                process_and_insert_data(file_path, process_fn)
