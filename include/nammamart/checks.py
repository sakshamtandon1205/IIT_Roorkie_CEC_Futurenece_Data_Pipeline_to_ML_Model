"""
Data quality checks for the ETL pipeline (Part D), in Python.

Every check has a name, one of the six dimensions and a severity:
    Critical -> the row is quarantined with a failure_reason
    Warning  -> the row still loads, the check is logged in dq_results

Each validate_<file>() returns (results, failed_mask, reasons):
    results      one dict per check, ready for the dq_results table
    failed_mask  True for rows that broke at least one Critical check
    reasons      text like "quantity_positive; order_amount_matches_formula"

The same rules are written again in SQL for the ELT pipeline (include/sql/).
"""
import pandas as pd

from nammamart import config as cfg


# ------------------------------------------------------------------ small helpers
def blank(s: pd.Series) -> pd.Series:
    return s.str.strip() == ""


def num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def ts(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", format="%Y-%m-%d %H:%M:%S")


def dt(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", format="%Y-%m-%d")


class Collector:
    """Collects check results for one file and remembers which rows failed which Critical check."""

    def __init__(self, file: str, df: pd.DataFrame):
        self.file = file
        self.n = len(df)
        self.index = df.index
        self.results = []
        self.failed = pd.Series(False, index=df.index)
        self.reason = pd.Series("", index=df.index)

    def add(self, name, dimension, severity, mask):
        """Row-level check: mask is True on the rows that break the rule."""
        mask = mask.fillna(False).astype(bool)
        failed = int(mask.sum())
        self._record(name, dimension, severity, self.n, failed)
        if severity == "Critical" and failed:
            self.failed |= mask
            self.reason[mask] = self.reason[mask] + name + "; "

    def add_file_level(self, name, dimension, severity, is_failing: bool, checked: int = 1):
        """Whole-file check (freshness, volume). `checked` is stored in dq_results.rows_checked,
        so the volume check can read last run's row count from there."""
        self._record(name, dimension, severity, checked, int(bool(is_failing)))

    def _record(self, name, dimension, severity, checked, failed):
        status = "PASS" if failed == 0 else ("FAIL" if severity == "Critical" else "WARN")
        self.results.append({
            "check_name": f"{self.file}.{name}", "dimension": dimension, "severity": severity,
            "rows_checked": int(checked), "rows_failed": int(failed), "status": status})

    def output(self):
        return self.results, self.failed, self.reason.str.rstrip("; ")


class Context:
    """Things a file's checks need to know about the other files."""

    def __init__(self, as_of, master_ids, orders_ref=None, product_mrp=None,
                 store_capacity=None, previous_orders_volume=None):
        self.as_of = pd.Timestamp(as_of)
        self.master_ids = master_ids              # {"customers": set, "products": set, ...}
        self.orders_ref = orders_ref              # raw orders, first row per order_id
        self.product_mrp = product_mrp            # Series product_id -> mrp
        self.store_capacity = store_capacity      # Series store_id -> capacity
        self.previous_orders_volume = previous_orders_volume


# ------------------------------------------------------------------ orders (and the increment file)
def validate_orders(df, file, ctx):
    c = Collector(file, df)
    qty, price, disc = num(df["quantity"]), num(df["unit_price"]), num(df["discount_pct"])
    amount, mins = num(df["order_amount"]), num(df["delivery_minutes"])
    when = ts(df["order_ts"])
    status = df["order_status"].str.strip().str.upper()
    pay_mode = df["payment_mode"].str.strip().str.lower().map(cfg.PAYMENT_MODE_MAP)

    # Completeness
    c.add("customer_id_not_null", "Completeness", "Critical", blank(df["customer_id"]))
    c.add("order_ts_not_null", "Completeness", "Critical", blank(df["order_ts"]))
    c.add("delivered_has_delivery_minutes", "Completeness", "Critical",
          (status == "DELIVERED") & blank(df["delivery_minutes"]))
    # Uniqueness
    c.add("order_id_unique", "Uniqueness", "Critical", df["order_id"].duplicated(keep="first"))
    # Validity
    c.add("order_status_allowed", "Validity", "Critical", ~status.isin(cfg.STATUSES))
    c.add("order_status_casing", "Validity", "Warning",
          status.isin(cfg.STATUSES) & (df["order_status"] != status))
    c.add("payment_mode_known", "Validity", "Critical", pay_mode.isna())
    c.add("payment_mode_standardised", "Validity", "Warning",
          pay_mode.notna() & (df["payment_mode"] != pay_mode))
    c.add("discount_pct_between_0_and_100", "Validity", "Critical",
          disc.isna() | (disc < 0) | (disc > 100))
    c.add("order_ts_parses", "Validity", "Critical", when.isna() & ~blank(df["order_ts"]))
    # Accuracy
    c.add("quantity_positive", "Accuracy", "Critical", qty.isna() | (qty <= 0))
    expected = (qty * price * (1 - disc / 100)).round(2)
    c.add("order_amount_matches_formula", "Accuracy", "Critical",
          ~((expected - amount).abs() <= cfg.AMOUNT_TOLERANCE))
    c.add("delivery_minutes_at_most_120", "Accuracy", "Critical", mins > cfg.MAX_DELIVERY_MINUTES)
    c.add("delivery_minutes_positive", "Accuracy", "Critical", mins <= 0)
    lo, hi = cfg.TYPICAL_DELIVERY
    c.add("delivery_minutes_typical_8_to_35", "Accuracy", "Warning",
          (mins > 0) & (mins <= cfg.MAX_DELIVERY_MINUTES) & ((mins < lo) | (mins > hi)))
    mrp = df["product_id"].map(ctx.product_mrp).pipe(pd.to_numeric, errors="coerce")
    c.add("unit_price_matches_catalogue_mrp", "Accuracy", "Warning",
          mrp.notna() & (mrp > 0) & ((price - mrp).abs() > cfg.AMOUNT_TOLERANCE))
    # Consistency (keys must exist in the master files)
    c.add("customer_exists_in_customers", "Consistency", "Critical",
          ~blank(df["customer_id"]) & ~df["customer_id"].isin(ctx.master_ids["customers"]))
    c.add("product_exists_in_products", "Consistency", "Critical",
          ~df["product_id"].isin(ctx.master_ids["products"]))
    c.add("store_exists_in_stores", "Consistency", "Critical",
          ~df["store_id"].isin(ctx.master_ids["stores"]))
    # Bonus: a store cannot take more orders in a day than its capacity
    capacity = df["store_id"].map(ctx.store_capacity).pipe(pd.to_numeric, errors="coerce")
    per_day = df.assign(_day=when.dt.date).groupby(["store_id", "_day"])["order_id"].transform("count")
    c.add("store_daily_orders_within_capacity", "Consistency", "Warning",
          capacity.notna() & (capacity > 0) & (per_day > capacity))
    # Timeliness
    cutoff = ctx.as_of + pd.Timedelta(days=1) if file == "increment" else ctx.as_of
    c.add("order_ts_not_in_future", "Timeliness", "Critical", when > cutoff)
    if file == "orders":
        c.add("order_ts_inside_september", "Timeliness", "Warning",
              when < pd.Timestamp(cfg.DATA_START))
        latest = when[when <= ctx.as_of].max()      # future-dated rows must not hide a stale file
        c.add_file_level("latest_order_within_1_day", "Timeliness", "Warning",
                         pd.isna(latest) or (ctx.as_of - latest) > pd.Timedelta(days=1))
        prev = ctx.previous_orders_volume
        c.add_file_level("volume_vs_last_run", "Completeness", "Warning",
                         prev is not None and prev > 0 and len(df) < cfg.VOLUME_DROP_LIMIT * prev,
                         checked=len(df))
    return c.output()


# ------------------------------------------------------------------ payments
def validate_payments(df, ctx):
    c = Collector("payments", df)
    amount = num(df["amount_paid"])
    when = ts(df["payment_ts"])
    pay_mode = df["payment_mode"].str.strip().str.lower().map(cfg.PAYMENT_MODE_MAP)
    # ctx.orders_ref is indexed by order_id and has the columns order_amount_o, order_status_o, order_ts_o
    order = df.join(ctx.orders_ref, on="order_id")
    o_amount = num(order["order_amount_o"])
    o_status = order["order_status_o"].fillna("").str.strip().str.upper()
    o_when = ts(order["order_ts_o"].fillna(""))
    known_order = df["order_id"].isin(ctx.master_ids["orders"])

    c.add("gateway_ref_present_for_non_cash", "Completeness", "Warning",
          blank(df["gateway_ref"]) & (pay_mode != "Cash on Delivery"))
    c.add("payment_id_unique", "Uniqueness", "Critical", df["payment_id"].duplicated(keep="first"))
    c.add("payment_status_allowed", "Validity", "Critical", ~df["payment_status"].isin(cfg.PAY_STATUSES))
    c.add("payment_mode_known", "Validity", "Critical", pay_mode.isna())
    c.add("payment_mode_standardised", "Validity", "Warning",
          pay_mode.notna() & (df["payment_mode"] != pay_mode))
    c.add("amount_paid_positive", "Accuracy", "Critical", amount.isna() | (amount <= 0))
    c.add("order_exists_in_orders", "Consistency", "Critical", ~known_order)
    c.add("success_amount_equals_order_amount", "Consistency", "Critical",
          (df["payment_status"] == "SUCCESS") & known_order & o_amount.notna()
          & ~((amount - o_amount).abs() <= cfg.AMOUNT_TOLERANCE))
    c.add("cancelled_order_has_no_payment", "Consistency", "Critical",
          known_order & (o_status == "CANCELLED") & (df["payment_status"] == "SUCCESS"))
    c.add("payment_not_before_order", "Consistency", "Warning",
          o_when.notna() & (when < o_when))
    c.add("delivered_order_payment_not_failed", "Consistency", "Warning",
          (df["payment_status"] == "FAILED") & (o_status == "DELIVERED"))
    c.add("success_payment_has_valid_order_status", "Consistency", "Warning",
          (df["payment_status"] == "SUCCESS") & known_order & ~o_status.isin(cfg.STATUSES))
    c.add("payment_ts_not_in_future", "Timeliness", "Critical", when > ctx.as_of)
    return c.output()


# ------------------------------------------------------------------ customers
def validate_customers(df, ctx):
    c = Collector("customers", df)
    tier = df["loyalty_tier"].str.strip().str.title()
    signup = dt(df["signup_date"])
    email_ok = df["email"].str.match(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")

    c.add("city_not_null", "Completeness", "Warning", blank(df["city"]))
    c.add("customer_id_unique", "Uniqueness", "Critical", df["customer_id"].duplicated(keep="first"))
    c.add("email_unique_across_customers", "Uniqueness", "Warning",
          df["email"].str.lower().duplicated(keep=False) & ~df["customer_id"].duplicated(keep=False))
    c.add("loyalty_tier_allowed", "Validity", "Critical", ~tier.isin(cfg.TIERS))
    c.add("email_format_valid", "Validity", "Warning", ~email_ok)
    c.add("phone_is_10_digits", "Validity", "Warning", ~df["phone"].str.fullmatch(r"\d{10}"))
    c.add("pincode_is_6_digits", "Validity", "Warning", ~df["pincode"].str.fullmatch(r"\d{6}"))
    c.add("is_active_is_y_or_n", "Validity", "Warning", ~df["is_active"].isin({"Y", "N"}))
    c.add("signup_date_parses", "Validity", "Critical", signup.isna())
    c.add("loyalty_tier_casing_consistent", "Consistency", "Warning",
          tier.isin(cfg.TIERS) & (df["loyalty_tier"] != tier))
    c.add("signup_date_not_in_future", "Timeliness", "Critical", signup > ctx.as_of)
    return c.output()


# ------------------------------------------------------------------ products
def validate_products(df, ctx):
    c = Collector("products", df)
    mrp, cost = num(df["mrp"]), num(df["cost_price"])
    is_old = df["product_name"].str.contains(r"\(old\)", regex=True)
    # When a product_id is repeated, keep the row that is not marked "(old)".
    order = is_old.sort_values(kind="stable").index
    repeat = df.loc[order, "product_id"].duplicated(keep="first").reindex(df.index)

    c.add("category_not_null", "Completeness", "Critical", blank(df["category"]))
    c.add("product_id_unique", "Uniqueness", "Critical", repeat)
    c.add("category_is_known", "Validity", "Warning",
          ~blank(df["category"]) & ~df["category"].isin(cfg.CATEGORIES))
    c.add("is_perishable_is_true_or_false", "Validity", "Warning",
          ~df["is_perishable"].isin({"True", "False"}))
    c.add("mrp_positive", "Accuracy", "Critical", mrp.isna() | (mrp <= 0))
    c.add("cost_price_positive", "Accuracy", "Critical", cost.isna() | (cost <= 0))
    c.add("cost_price_not_above_mrp", "Accuracy", "Critical", cost > mrp)
    c.add("no_old_catalogue_rows", "Consistency", "Warning", is_old)
    c.add("launch_date_not_in_future", "Timeliness", "Warning", dt(df["launch_date"]) > ctx.as_of)
    return c.output()


# ------------------------------------------------------------------ stores
def validate_stores(df, ctx):
    c = Collector("stores", df)
    cap = num(df["capacity_orders_per_day"])
    c.add("zone_not_null", "Completeness", "Warning", blank(df["zone"]))
    c.add("store_id_unique", "Uniqueness", "Critical", df["store_id"].duplicated(keep="first"))
    c.add("zone_is_known", "Validity", "Critical", ~blank(df["zone"]) & ~df["zone"].isin(cfg.ZONES))
    c.add("pincode_is_6_digits", "Validity", "Warning", ~df["pincode"].str.fullmatch(r"\d{6}"))
    c.add("capacity_positive", "Accuracy", "Warning", cap.isna() | (cap <= 0))
    c.add("open_date_not_in_future", "Timeliness", "Warning", dt(df["open_date"]) > ctx.as_of)
    return c.output()
