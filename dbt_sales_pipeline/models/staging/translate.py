import snowflake.connector
from deep_translator import GoogleTranslator
import os
from dotenv import load_dotenv
load_dotenv()

# --- Snowflake connection ---
conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse="COMPUTE_WH",
    database="SALES_PIPELINE",
    schema="RAW",
    role="ACCOUNTADMIN"
)
cursor = conn.cursor()


# --- Reusable translation function ---
def translate_rows(rows, has_category_id=False):
    translated = []
    for row in rows:
        if has_category_id:
            item_id, name_ru, category_id = row
        else:
            item_id, name_ru = row
            category_id = None
        try:
            name_en = GoogleTranslator(source='auto', target='en').translate(name_ru)
        except Exception:
            name_en = 'TRANSLATION_FAILED'
        if has_category_id:
            translated.append((item_id, name_en, category_id))
        else:
            translated.append((item_id, name_en))
    return translated


# === ITEM_CATEGORIES ===
cursor.execute("SELECT ITEM_CATEGORY_ID, ITEM_CATEGORY_NAME FROM ITEM_CATEGORIES")
item_cat_rows = cursor.fetchall()
translated_item_categories = translate_rows(item_cat_rows)

cursor.execute("""
    CREATE OR REPLACE TABLE TRANSLATED_ITEM_CATEGORIES (
        ITEM_CATEGORY_ID NUMBER,
        ITEM_CATEGORY_NAME STRING
    )
""")
for row in translated_item_categories:
    cursor.execute("INSERT INTO TRANSLATED_ITEM_CATEGORIES VALUES (%s, %s)", row)

print("✅ TRANSLATED_ITEM_CATEGORIES created.")


# === SHOPS ===
cursor.execute("SELECT SHOP_ID, SHOP_NAME FROM SHOPS")
shop_rows = cursor.fetchall()
translated_shops = translate_rows(shop_rows)

cursor.execute("""
    CREATE OR REPLACE TABLE TRANSLATED_SHOPS (
        SHOP_ID NUMBER,
        SHOP_NAME STRING
    )
""")
for row in translated_shops:
    cursor.execute("INSERT INTO TRANSLATED_SHOPS VALUES (%s, %s)", row)

print("✅ TRANSLATED_SHOPS created.")


# === ITEMS ===
cursor.execute("SELECT ITEM_ID, ITEM_NAME, ITEM_CATEGORY_ID FROM ITEMS")
item_rows = cursor.fetchall()
translated_items = translate_rows(item_rows, has_category_id=True)

cursor.execute("""
    CREATE OR REPLACE TABLE TRANSLATED_ITEMS (
        ITEM_ID NUMBER,
        ITEM_NAME STRING,
        ITEM_CATEGORY_ID NUMBER
    )
""")
for row in translated_items:
    cursor.execute("INSERT INTO TRANSLATED_ITEMS VALUES (%s, %s, %s)", row)

print("✅ TRANSLATED_ITEMS created.")

# --- Done ---
cursor.close()
conn.close()
print("🎉 All translations completed successfully.")
