"""
Paths, allowed values and expected columns, shared by the ETL and ELT pipelines.

Inside the Astro container the project's include/ folder is /usr/local/airflow/include.
The NAMMAMART_BASE environment variable lets the same code run outside Docker (tests).
"""
import os
from pathlib import Path

BASE = Path(os.environ.get("NAMMAMART_BASE", "/usr/local/airflow/include"))
DATA_DIR = BASE / "data" / "nammamart"
SQL_DIR = BASE / "sql"
WAREHOUSE_DB = BASE / "warehouse_saksham.duckdb"      # my own warehouse file
QUARANTINE_DIR = BASE / "quarantine" / "saksham"
STAGE_DIR = BASE / "stage" / "saksham"                 # per-run scratch files (not committed)
REPORT_DIR = BASE / "reports"

# logical name -> csv file name
FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
    "increment": "orders_2026-10-01_increment.csv",
}
KEYS = {"orders": "order_id", "increment": "order_id", "payments": "payment_id",
        "customers": "customer_id", "products": "product_id", "stores": "store_id"}

ORDER_COLUMNS = ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity",
                 "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status",
                 "channel", "delivery_minutes"]
REQUIRED_COLUMNS = {
    "orders": ORDER_COLUMNS,
    "increment": ORDER_COLUMNS,
    "payments": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid",
                 "payment_status", "gateway_ref"],
    "customers": ["customer_id", "full_name", "email", "phone", "city", "area", "pincode",
                  "signup_date", "loyalty_tier", "is_active"],
    "products": ["product_id", "product_name", "category", "sub_category", "brand", "mrp",
                 "cost_price", "unit", "is_perishable", "launch_date"],
    "stores": ["store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name",
               "capacity_orders_per_day"],
}

# Allowed values from the client's business rules
STATUSES = {"DELIVERED", "CANCELLED", "RETURNED"}
PAY_MODES = {"UPI", "Card", "Cash on Delivery", "Wallet"}
# any spelling we have seen -> the standard value
PAYMENT_MODE_MAP = {"upi": "UPI", "card": "Card", "cash on delivery": "Cash on Delivery",
                    "cod": "Cash on Delivery", "cashh": "Cash on Delivery", "wallet": "Wallet"}
PAY_STATUSES = {"SUCCESS", "FAILED", "REFUNDED"}
TIERS = {"Bronze", "Silver", "Gold", "Platinum"}
ZONES = {"North", "South", "East", "West"}
CATEGORIES = {"Fruits & Vegetables", "Dairy & Bakery", "Staples", "Snacks & Beverages",
              "Household", "Personal Care"}

# Thresholds
DEFAULT_AS_OF = "2026-10-01"     # the September export was taken on the morning of 1 Oct
DATA_START = "2026-09-01"
MAX_DELIVERY_MINUTES = 120       # above this is impossible
TYPICAL_DELIVERY = (8, 35)       # normal range from the client
VOLUME_DROP_LIMIT = 0.10         # warn when today's rows < 10% of the last run
AMOUNT_TOLERANCE = 0.01
