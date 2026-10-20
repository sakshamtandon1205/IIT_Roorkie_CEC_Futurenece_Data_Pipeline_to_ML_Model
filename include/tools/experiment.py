"""
Applies one observability experiment to the input files and undoes it afterwards.

    python include/tools/experiment.py 4          apply experiment 4 (missing source file)
    python include/tools/experiment.py restore    put every original file back

Originals are copied to include/data/_experiment_backup/ the first time, and restore copies them back.
Experiments: 4 missing products.csv | 5 five bad rows in orders.csv | 6 order_amount renamed to total
             7 only 50 orders | 8 unit_price x100 for store S03.
Experiments 1, 2, 3, 9 and 10 need no file change (re-run, increment, as_of parameter, Clear).
"""
import shutil
import sys
from pathlib import Path

import pandas as pd

INCLUDE = Path(__file__).resolve().parents[1]
DATA = INCLUDE / "data" / "nammamart"
BACKUP = INCLUDE / "data" / "_experiment_backup"


def backup():
    if BACKUP.exists():
        return
    shutil.copytree(DATA, BACKUP)


def restore():
    if not BACKUP.exists():
        print("nothing to restore, files are original")
        return
    for f in BACKUP.iterdir():
        shutil.copy2(f, DATA / f.name)
    for f in DATA.glob("*.missing"):
        f.unlink()
    shutil.rmtree(BACKUP)
    print("restored original files")


def read(name):
    return pd.read_csv(DATA / name, dtype=str, keep_default_na=False)


def write(df, name):
    df.to_csv(DATA / name, index=False)


def apply(n):
    backup()
    if n == 4:
        (DATA / "products.csv").rename(DATA / "products.csv.missing")
        print("products.csv renamed to products.csv.missing")
    elif n == 5:
        df = read("orders.csv")
        base = df.iloc[0].to_dict()
        rows = []
        for i, (field, value, label) in enumerate([
                ("customer_id", "", "null customer"), ("quantity", "-2", "negative quantity"),
                ("store_id", "S99", "unknown store"), ("order_status", "SHIPPED", "status SHIPPED"),
                ("order_ts", "2027-03-01 10:00:00", "2027 timestamp")]):
            r = dict(base)
            r["order_id"] = f"ORD9000{i + 1}"
            r["order_status"] = "DELIVERED" if field != "order_status" else value
            r["customer_id"] = "C00001"
            r["payment_mode"] = "UPI"
            r["quantity"] = "1"
            r["unit_price"] = "100.0"
            r["discount_pct"] = "0"
            r["order_amount"] = "100.0"
            r["delivery_minutes"] = "20"
            r[field] = value
            if field == "quantity":          # keep the amount consistent so only the quantity rule fires
                r["order_amount"] = "-200.0"
                r["unit_price"] = "100.0"
            rows.append(r)
            print(f"  added {r['order_id']}: {label}")
        write(pd.concat([df, pd.DataFrame(rows)], ignore_index=True), "orders.csv")
    elif n == 6:
        df = read("orders.csv").rename(columns={"order_amount": "total"})
        write(df, "orders.csv")
        print("orders.csv: column order_amount renamed to total")
    elif n == 7:
        write(read("orders.csv").head(50), "orders.csv")
        print("orders.csv now has 50 rows")
    elif n == 8:
        df = read("orders.csv")
        m = df["store_id"] == "S03"
        df.loc[m, "unit_price"] = (pd.to_numeric(df.loc[m, "unit_price"]) * 100).round(2).astype(str)
        write(df, "orders.csv")
        print(f"orders.csv: unit_price x100 for {int(m.sum())} rows of store S03")
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else ""
    restore() if arg == "restore" else apply(int(arg)) if arg.isdigit() else sys.exit(__doc__)
