import azure.functions as func
import logging
import os
import sqlite3
import tempfile
import pandas as pd
from io import StringIO

app = func.FunctionApp()


# ============================================================
# CONFIG
# ============================================================
BLOB_CONTAINER = "raw"

# Meta CSVs: filename → SQLite table name (read from each account subfolder)
META_CSV_TABLE_MAP = {
    "campaigns.csv": "meta_campaign_insights",
    "adsets.csv": "meta_adset_insights",
    "ads.csv": "meta_ad_insights",
    "daily_spend.csv": "meta_daily_insights",
}

META_BLOB_PREFIX = "metaads"

# Non-meta CSVs: full blob path → SQLite table name
OTHER_CSV_TABLE_MAP = {
    "shopify/orders.csv": "shopify_orders",
}

DB_BLOB_FOLDER = "sqldb"
DB_FILENAME = "dravya.db"
DB_BLOB_CONTAINER = "app-dravya-bi-backend-storage"

# ============================================================
# COLUMN DESCRIPTIONS METADATA
# ============================================================
COLUMN_DESCRIPTIONS = {
    "meta_campaign_insights": {
        "account_name": "Name of the Meta ad account (e.g. dravya-ma, dravya-vds)",
        "campaign_name": "Name of the Meta (Facebook/Instagram) ad campaign",
        "campaign_id": "Unique identifier for the campaign in Meta Ads",
        "objective": "Campaign objective type (e.g. OUTCOME_SALES)",
        "spend": "Total amount spent on the campaign in the account currency",
        "impressions": "Total number of times the ads were displayed",
        "clicks": "Total number of clicks on the ads",
        "cpc": "Cost per click - average cost for each click (spend / clicks)",
        "ctr": "Click-through rate - percentage of impressions that resulted in clicks",
        "purchases": "Total number of purchase conversions attributed to the campaign",
        "revenue": "Total revenue generated from purchases attributed to the campaign",
        "roas": "Return on Ad Spend - revenue divided by spend",
        "add_to_cart": "Number of add-to-cart events attributed to the campaign",
        "checkouts": "Number of checkout initiated events attributed to the campaign",
    },
    "meta_adset_insights": {
        "account_name": "Name of the Meta ad account (e.g. dravya-ma, dravya-vds)",
        "campaign_name": "Name of the parent campaign this ad set belongs to",
        "adset_name": "Name of the ad set (audience/targeting group)",
        "adset_id": "Unique identifier for the ad set in Meta Ads",
        "spend": "Total amount spent on the ad set in the account currency",
        "impressions": "Total number of times the ads in this ad set were displayed",
        "clicks": "Total number of clicks on the ads in this ad set",
        "purchases": "Total number of purchase conversions attributed to the ad set",
        "revenue": "Total revenue generated from purchases attributed to the ad set",
        "add_to_cart": "Number of add-to-cart events attributed to the ad set",
        "checkouts": "Number of checkout initiated events attributed to the ad set",
    },
    "meta_ad_insights": {
        "account_name": "Name of the Meta ad account (e.g. dravya-ma, dravya-vds)",
        "campaign_name": "Name of the parent campaign this ad belongs to",
        "adset_name": "Name of the parent ad set this ad belongs to",
        "ad_name": "Name/title of the individual ad creative",
        "ad_id": "Unique identifier for the ad in Meta Ads",
        "spend": "Total amount spent on this ad in the account currency",
        "impressions": "Total number of times this ad was displayed",
        "clicks": "Total number of clicks on this ad",
        "purchases": "Total number of purchase conversions attributed to this ad",
        "revenue": "Total revenue generated from purchases attributed to this ad",
        "add_to_cart": "Number of add-to-cart events attributed to this ad",
        "checkouts": "Number of checkout initiated events attributed to this ad",
    },
    "meta_daily_insights": {
        "account_name": "Name of the Meta ad account (e.g. dravya-ma, dravya-vds)",
        "date": "Date of the daily aggregated metrics (YYYY-MM-DD format)",
        "spend": "Total amount spent across all campaigns on this date",
        "impressions": "Total number of ad impressions on this date",
        "clicks": "Total number of ad clicks on this date",
        "cpc": "Cost per click on this date (spend / clicks)",
        "cpm": "Cost per 1000 impressions on this date (spend / impressions * 1000)",
        "ctr": "Click-through rate on this date (clicks / impressions * 100)",
        "reach": "Number of unique users who saw the ads on this date",
        "purchases": "Total number of purchase conversions on this date",
        "add_to_cart": "Number of add-to-cart events on this date",
        "checkouts": "Number of checkout initiated events on this date",
        "revenue": "Total revenue from purchases on this date",
        "roas": "Return on Ad Spend on this date (revenue / spend)",
    },
    "shopify_orders": {
        "order_id": "Unique Shopify order identifier",
        "order_number": "Sequential order number displayed to the customer",
        "name": "Human-readable order name (e.g. #4953)",
        "created_at": "Timestamp when the order was created (ISO 8601 with timezone)",
        "processed_at": "Timestamp when the order payment was processed",
        "financial_status": "Payment status of the order (e.g. pending, paid, refunded)",
        "fulfillment_status": "Shipping/fulfillment status (e.g. fulfilled, unfulfilled, NULL means unfulfilled)",
        "currency": "Currency code for the order (e.g. INR)",
        "subtotal_price": "Order subtotal before shipping, tax, and discounts",
        "shipping_price": "Shipping cost charged for the order",
        "tax": "Total tax amount on the order",
        "discounts": "Total discount amount applied to the order",
        "total_price": "Final total price charged to the customer (subtotal + shipping + tax - discounts)",
        "net_sales": "Net sales amount after returns/refunds",
        "gross_sales": "Gross sales amount before any deductions",
        "order_margin_base": "Base order margin value",
        "customer_id": "Unique Shopify customer identifier",
        "customer_email": "Email address of the customer",
        "customer_first_name": "First name of the customer",
        "customer_last_name": "Last name of the customer",
        "billing_country": "Country from the billing address",
        "shipping_country": "Country from the shipping address",
        "gateway": "Payment gateway used (e.g. GoKwik Cash on Delivery, Razorpay, etc.)",
        "product_names": "List of product names included in the order (JSON array as string)",
        "line_item_count": "Number of distinct line items (products) in the order",
        "cancelled_at": "Timestamp when the order was cancelled (NULL if not cancelled)",
    },
}


# ============================================================
# BLOB STORAGE HELPERS
# ============================================================
def get_blob_service_client():
    from azure.storage.blob import BlobServiceClient

    conn_str = os.environ.get("DATA_STORAGE_CONNECTION_STRING")
    if not conn_str:
        raise RuntimeError(
            "DATA_STORAGE_CONNECTION_STRING is not set. "
            "Point it to the adlsgen2dravya01 storage account."
        )
    return BlobServiceClient.from_connection_string(conn_str)


def download_csv_from_blob(blob_service_client, blob_path: str) -> pd.DataFrame:
    """Download a CSV from Azure Blob Storage and return as DataFrame."""
    container_client = blob_service_client.get_container_client(BLOB_CONTAINER)
    blob_client = container_client.get_blob_client(blob_path)

    logging.info(f"Downloading blob: {BLOB_CONTAINER}/{blob_path}")
    csv_bytes = blob_client.download_blob().readall()
    csv_text = csv_bytes.decode("utf-8")
    return pd.read_csv(StringIO(csv_text))


def discover_account_folders(blob_service_client) -> list:
    """Discover account subfolders under metaads/ in blob storage."""
    container_client = blob_service_client.get_container_client(BLOB_CONTAINER)
    folders = set()
    for blob in container_client.list_blobs(name_starts_with=f"{META_BLOB_PREFIX}/"):
        parts = blob.name.split("/")
        # Pattern: metaads/<account_name>/filename.csv
        if len(parts) >= 3:
            folders.add(parts[1])
    return sorted(folders)


def upload_file_to_blob(blob_service_client, local_path: str, blob_path: str, container: str = BLOB_CONTAINER):
    """Upload a local file to Azure Blob Storage."""
    container_client = blob_service_client.get_container_client(container)

    try:
        container_client.create_container()
    except Exception:
        pass  # Container already exists

    blob_client = container_client.get_blob_client(blob_path)
    with open(local_path, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)
    logging.info(f"Uploaded to blob: {BLOB_CONTAINER}/{blob_path}")


# ============================================================
# SQLITE DB BUILDER
# ============================================================
def build_sqlite_db(dataframes: dict[str, pd.DataFrame], db_path: str):
    """
    Build a SQLite database from DataFrames.
    - Creates a table for each DataFrame
    - Creates a _column_descriptions metadata table
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # --- 1. Fix types for shopify_orders ---
    if "shopify_orders" in dataframes:
        orders_df = dataframes["shopify_orders"]
        for col in ["created_at", "processed_at", "cancelled_at"]:
            if col in orders_df.columns:
                orders_df[col] = pd.to_datetime(orders_df[col], errors="coerce")
        dataframes["shopify_orders"] = orders_df

    # --- 2. Drop raw_json column if present ---
    for table_name, df in dataframes.items():
        if "raw_json" in df.columns:
            dataframes[table_name] = df.drop(columns=["raw_json"])

    # --- 3. Write DataFrames to SQLite ---
    for table_name, df in dataframes.items():
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        logging.info(f"Loaded {len(df)} rows into '{table_name}'")

    # --- 4. Create column descriptions metadata table ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _column_descriptions (
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            description TEXT NOT NULL,
            PRIMARY KEY (table_name, column_name)
        )
    """)

    for table_name, columns in COLUMN_DESCRIPTIONS.items():
        for col_name, description in columns.items():
            cursor.execute(
                "INSERT OR REPLACE INTO _column_descriptions "
                "(table_name, column_name, description) VALUES (?, ?, ?)",
                (table_name, col_name, description),
            )

    conn.commit()

    desc_count = cursor.execute(
        "SELECT COUNT(*) FROM _column_descriptions"
    ).fetchone()[0]
    logging.info(f"Column descriptions metadata created with {desc_count} entries")

    conn.close()


# ============================================================
# TIMER TRIGGER — Runs every hour
# ============================================================
@app.timer_trigger(schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False)
def SqlDbBuilder(myTimer: func.TimerRequest) -> None:
    logging.info("SqlDbBuilder triggered — building SQLite DB from blob CSVs.")

    try:
        blob_service_client = get_blob_service_client()
    except RuntimeError as e:
        logging.error(str(e))
        return

    # --- 1a. Discover account folders and download/merge meta CSVs ---
    dataframes: dict[str, pd.DataFrame] = {}
    account_folders = discover_account_folders(blob_service_client)
    logging.info(f"Found meta ad account folders: {account_folders}")

    for csv_file, table_name in META_CSV_TABLE_MAP.items():
        merged_dfs = []
        for account_name in account_folders:
            blob_path = f"{META_BLOB_PREFIX}/{account_name}/{csv_file}"
            try:
                df = download_csv_from_blob(blob_service_client, blob_path)
                df.insert(0, "account_name", account_name)
                merged_dfs.append(df)
                logging.info(f"Downloaded {blob_path} ({len(df)} rows)")
            except Exception as e:
                logging.warning(f"Skipped {blob_path}: {e}")
        if merged_dfs:
            dataframes[table_name] = pd.concat(merged_dfs, ignore_index=True)
            logging.info(f"Merged {table_name}: {len(dataframes[table_name])} total rows from {len(merged_dfs)} account(s)")

    # --- 1b. Download non-meta CSVs (shopify, etc.) ---
    for blob_path, table_name in OTHER_CSV_TABLE_MAP.items():
        try:
            df = download_csv_from_blob(blob_service_client, blob_path)
            dataframes[table_name] = df
            logging.info(
                f"Downloaded {blob_path} → {table_name} ({len(df)} rows)"
            )
        except Exception as e:
            logging.error(f"Failed to download {blob_path}: {e}")

    if not dataframes:
        logging.error("No CSVs could be downloaded. Aborting.")
        return

    # --- 2. Build SQLite DB in temp file ---
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = os.path.join(tmp_dir, DB_FILENAME)
        build_sqlite_db(dataframes, db_path)

        db_size_mb = os.path.getsize(db_path) / (1024 * 1024)
        logging.info(f"SQLite DB built: {db_path} ({db_size_mb:.2f} MB)")

        # --- 3. Upload DB to blob ---
        blob_path = f"{DB_BLOB_FOLDER}/{DB_FILENAME}"
        try:
            upload_file_to_blob(blob_service_client, db_path, blob_path, DB_BLOB_CONTAINER)
            logging.info(
                f"Successfully uploaded SQLite DB to {DB_BLOB_CONTAINER}/{blob_path}"
            )
        except Exception as e:
            logging.error(f"Failed to upload DB to blob: {e}")

    logging.info("SqlDbBuilder completed.")
