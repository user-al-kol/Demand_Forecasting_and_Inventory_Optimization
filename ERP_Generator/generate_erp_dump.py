"""
generate_erp_dump.py
====================
Simulates an ERP end-of-day CSV export.

Each execution represents one simulated working day.
Triggered by Airflow on a 3-minute schedule.

Produces two CSV files in OUTPUT_DIR:
  - sales_YYYYMMDD_HHMMSS.csv
  - inventory_movements_YYYYMMDD_HHMMSS.csv

Reads reference data (products, customers, locations) from postgres_oltp
so all generated records reference entities that actually exist in the DB.
"""

import os
import random
import logging
import uuid
from datetime import datetime, timedelta, timezone

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

# =============================================================================
# Configuration — all overridable via environment variables
# =============================================================================

DB_HOST     = os.getenv("OLTP_HOST",     "postgres_oltp")
DB_PORT     = int(os.getenv("OLTP_PORT", "5432"))
DB_NAME     = os.getenv("OLTP_DB",       "belsani_oltp")
DB_USER     = os.getenv("OLTP_USER",     "belsani")
DB_PASSWORD = os.getenv("OLTP_PASSWORD", "belsani_secret")

OUTPUT_DIR  = os.getenv("OUTPUT_DIR", "/app/erp_dumps")

# Simulated day: each script run = one working day
# MIN/MAX orders and movements per dump
MIN_ORDERS     = int(os.getenv("MIN_ORDERS",     "5"))
MAX_ORDERS     = int(os.getenv("MAX_ORDERS",     "15"))
MIN_MOVEMENTS  = int(os.getenv("MIN_MOVEMENTS",  "20"))
MAX_MOVEMENTS  = int(os.getenv("MAX_MOVEMENTS",  "50"))

# Movement type weights — controls what kind of day it is
MOVEMENT_WEIGHTS = {
    "sale":        0.60,   # 60% of movements are sales deductions
    "po_receipt":  0.20,   # 20% are purchase order receipts arriving
    "adjustment":  0.10,   # 10% are manual stock adjustments
    "transfer":    0.10,   # 10% are inter-location transfers
}

# =============================================================================
# Logging
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# =============================================================================
# Database helpers
# =============================================================================

def get_connection():
    """Open a connection to postgres_oltp."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def fetch_reference_data(conn):
    """
    Load the reference data we need to generate realistic records.
    Returns dicts keyed by entity type.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:

        cur.execute("""
            SELECT product_id, sku, unit_price_eur, min_order_qty, lead_time_days
            FROM products
            WHERE is_active = TRUE
        """)
        products = cur.fetchall()

        cur.execute("""
            SELECT customer_id, customer_code, customer_name, customer_type
            FROM customers
            WHERE is_active = TRUE
        """)
        customers = cur.fetchall()

        cur.execute("""
            SELECT location_id, location_code, location_name, location_type
            FROM locations
            WHERE is_active = TRUE
        """)
        locations = cur.fetchall()

        # Only sanicenter locations fulfill sales orders (not warehouses)
        sanicenters = [l for l in locations if l["location_type"] == "sanicenter"]

        cur.execute("""
            SELECT i.inventory_id, i.product_id, i.location_id,
                   i.qty_on_hand, i.qty_reserved
            FROM inventory i
            JOIN locations l ON l.location_id = i.location_id
            WHERE l.location_type = 'sanicenter'
              AND i.qty_on_hand > 0
        """)
        inventory = cur.fetchall()

        cur.execute("""
            SELECT po.po_id, po.supplier_id, po.location_id,
                   pol.product_id, pol.qty_ordered, pol.qty_received,
                   po.expected_delivery
            FROM purchase_orders po
            JOIN purchase_order_lines pol ON pol.po_id = po.po_id
            WHERE po.status IN ('sent', 'confirmed')
              AND pol.qty_received < pol.qty_ordered
        """)
        open_pos = cur.fetchall()

    return {
        "products":   products,
        "customers":  customers,
        "locations":  locations,
        "sanicenters": sanicenters,
        "inventory":  inventory,
        "open_pos":   open_pos,
    }

# =============================================================================
# Sales order generation
# =============================================================================

def generate_sales(ref, sim_date, n_orders):
    """
    Generate n_orders sales orders with their lines.
    Returns a flat list of dicts (one row per order line) — ERP-style flat export.
    """
    rows = []
    sources = ["counter", "counter", "counter", "phone", "online"]  # weighted

    for _ in range(n_orders):
        customer  = random.choice(ref["customers"])
        location  = random.choice(ref["sanicenters"])
        order_id  = str(uuid.uuid4())
        source    = random.choice(sources)
        n_lines   = random.randint(1, 4)
        order_ts  = _random_business_time(sim_date)

        # Pick products that have stock at this location
        available = [
            inv for inv in ref["inventory"]
            if inv["location_id"] == location["location_id"]
            and inv["qty_on_hand"] > inv["qty_reserved"]
        ]

        if not available:
            # Fall back to any product if location has no tracked inventory
            line_products = random.sample(ref["products"], min(n_lines, len(ref["products"])))
        else:
            # Sample from products with available stock
            sample_inv = random.sample(available, min(n_lines, len(available)))
            product_ids = {p["product_id"] for p in ref["products"]}
            line_products = [
                next(p for p in ref["products"] if p["product_id"] == inv["product_id"])
                for inv in sample_inv
                if inv["product_id"] in product_ids
            ]

        if not line_products:
            continue

        order_total = 0.0
        for product in line_products:
            qty = round(random.uniform(
                float(product["min_order_qty"]),
                float(product["min_order_qty"]) * 5
            ), 2)
            unit_price  = float(product["unit_price_eur"])
            # Apply occasional professional discount (5-15%)
            if customer["customer_type"] in ("plumber", "contractor"):
                discount   = random.uniform(0.05, 0.15)
                unit_price = round(unit_price * (1 - discount), 2)
            line_total  = round(qty * unit_price, 2)
            order_total += line_total

            # Fulfillment: most lines fully fulfilled, occasionally partial
            fulfilled_ratio = random.choices(
                [1.0, random.uniform(0.5, 0.99)],
                weights=[0.85, 0.15]
            )[0]
            qty_fulfilled = round(qty * fulfilled_ratio, 2)

            now = datetime.now(timezone.utc)
            export_ts = now.replace(hour=18, minute=random.randint(0, 59), second=random.randint(0, 59), microsecond=0)

            rows.append({
                "order_id":           order_id,
                "order_ts":           order_ts.isoformat(),
                "order_date":         sim_date.strftime("%Y-%m-%d"),
                "status":             "fulfilled" if fulfilled_ratio == 1.0 else "partial",
                "source":             source,
                "customer_id":        customer["customer_id"],
                "customer_code":      customer["customer_code"],
                "customer_name":      customer["customer_name"],
                "customer_type":      customer["customer_type"],
                "location_id":        location["location_id"],
                "location_code":      location["location_code"],
                "product_id":         product["product_id"],
                "sku":                product["sku"],
                "qty_ordered":        qty,
                "qty_fulfilled":      qty_fulfilled,
                "unit_price_eur":     unit_price,
                "line_total_eur":     line_total,
                "order_total_eur":    round(order_total, 2),
                "erp_export_ts":      export_ts.isoformat(),
            })

    return rows

# =============================================================================
# Inventory movement generation
# =============================================================================

def generate_inventory_movements(ref, sim_date, n_movements):
    """
    Generate n_movements inventory movement records.
    Mirrors what the ERP would export as stock movement journal.
    """
    rows = []
    movement_types = list(MOVEMENT_WEIGHTS.keys())
    weights        = list(MOVEMENT_WEIGHTS.values())

    for _ in range(n_movements):
        movement_type = random.choices(movement_types, weights=weights)[0]
        movement_ts   = _random_business_time(sim_date)
        product       = random.choice(ref["products"])
        movement_id   = str(uuid.uuid4())

        if movement_type == "sale":
            location  = random.choice(ref["sanicenters"])
            qty_delta = -round(random.uniform(1, 10), 2)
            ref_type  = "sales_order"
            ref_id    = str(uuid.uuid4())
            notes     = f"Sale fulfilled at {location['location_name']}"

        elif movement_type == "po_receipt":
            # If there are open POs, use one; otherwise simulate a receipt
            if ref["open_pos"]:
                po = random.choice(ref["open_pos"])
                location  = next(
                    (l for l in ref["locations"] if l["location_id"] == po["location_id"]),
                    random.choice(ref["locations"])
                )
                product   = next(
                    (p for p in ref["products"] if p["product_id"] == po["product_id"]),
                    product
                )
                qty_delta = round(random.uniform(
                    float(po["qty_ordered"]) * 0.5,
                    float(po["qty_ordered"]) - float(po["qty_received"])
                ), 2)
                ref_id   = str(po["po_id"])
            else:
                location  = random.choice(ref["locations"])
                qty_delta = round(random.uniform(10, 100), 2)
                ref_id    = str(uuid.uuid4())
            ref_type  = "purchase_order"
            notes     = f"PO receipt at {location['location_name']}"

        elif movement_type == "adjustment":
            location  = random.choice(ref["locations"])
            # Adjustments can be positive (found stock) or negative (write-off)
            qty_delta = round(random.uniform(-5, 5), 2)
            if qty_delta == 0:
                qty_delta = 1.0
            ref_type  = "adjustment"
            ref_id    = str(uuid.uuid4())
            notes     = random.choice([
                "Cycle count correction",
                "Damaged goods write-off",
                "Found stock after audit",
                "System reconciliation",
            ])

        else:  # transfer
            from_loc  = random.choice(ref["locations"])
            to_loc    = random.choice([l for l in ref["locations"] if l["location_id"] != from_loc["location_id"]])
            qty_delta = round(random.uniform(5, 30), 2)
            location  = from_loc
            ref_type  = "transfer"
            ref_id    = str(uuid.uuid4())
            notes     = f"Transfer to {to_loc['location_name']}"

        export_ts = datetime.now(timezone.utc).replace(
            hour=18, minute=random.randint(0, 59), second=random.randint(0, 59), microsecond=0
        )

        rows.append({
            "movement_id":    movement_id,
            "movement_ts":    movement_ts.isoformat(),
            "movement_date":  sim_date.strftime("%Y-%m-%d"),
            "movement_type":  movement_type,
            "product_id":     product["product_id"],
            "sku":            product["sku"],
            "location_id":    location["location_id"],
            "location_code":  location["location_code"],
            "qty_delta":      qty_delta,
            "ref_order_id":   ref_id,
            "ref_order_type": ref_type,
            "notes":          notes,
            "erp_export_ts":  export_ts.isoformat(),
        })

    return rows

# =============================================================================
# Helpers
# =============================================================================

def _random_business_time(date):
    """Return the current real time on the given date."""
    return date


def _get_simulated_date():
    """
    Returns today's real date and current time.
    Each script execution represents the current working day.
    """
    return datetime.now(timezone.utc)


def write_csv(rows, prefix, output_dir):
    """Write rows to a timestamped CSV file."""
    if not rows:
        log.warning(f"No rows to write for {prefix} — skipping.")
        return None

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename  = f"{prefix}_{timestamp}.csv"
    filepath  = os.path.join(output_dir, filename)

    df = pd.DataFrame(rows)
    df.to_csv(filepath, index=False, encoding="utf-8")

    log.info(f"Written {len(rows)} rows → {filepath}")
    return filepath

# =============================================================================
# Main
# =============================================================================

def main():
    log.info("=" * 60)
    log.info("ERP CSV Generator starting")
    log.info("=" * 60)

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Determine simulated business date
    sim_date = _get_simulated_date()
    log.info(f"Simulated business date: {sim_date.strftime('%Y-%m-%d (%A)')}")

    # Connect to OLTP DB
    log.info(f"Connecting to postgres_oltp at {DB_HOST}:{DB_PORT}")
    try:
        conn = get_connection()
        log.info("Connected successfully.")
    except Exception as e:
        log.error(f"Failed to connect to postgres_oltp: {e}")
        raise

    try:
        # Load reference data
        log.info("Loading reference data from postgres_oltp...")
        ref = fetch_reference_data(conn)
        log.info(
            f"Loaded: {len(ref['products'])} products, "
            f"{len(ref['customers'])} customers, "
            f"{len(ref['sanicenters'])} sanicenters, "
            f"{len(ref['inventory'])} inventory rows with stock"
        )

        # Determine how many records to generate this cycle
        n_orders    = random.randint(MIN_ORDERS, MAX_ORDERS)
        n_movements = random.randint(MIN_MOVEMENTS, MAX_MOVEMENTS)
        log.info(f"Generating {n_orders} orders, {n_movements} inventory movements")

        # Generate sales
        log.info("Generating sales orders...")
        sales_rows = generate_sales(ref, sim_date, n_orders)
        log.info(f"Generated {len(sales_rows)} sales order lines")

        # Generate inventory movements
        log.info("Generating inventory movements...")
        movement_rows = generate_inventory_movements(ref, sim_date, n_movements)
        log.info(f"Generated {len(movement_rows)} inventory movements")

        # Write CSVs
        sales_path     = write_csv(sales_rows,     "sales",               OUTPUT_DIR)
        movements_path = write_csv(movement_rows,  "inventory_movements", OUTPUT_DIR)

        log.info("=" * 60)
        log.info("ERP dump complete.")
        log.info(f"  Sales file:      {sales_path}")
        log.info(f"  Movements file:  {movements_path}")
        log.info(f"  Simulated date:  {sim_date.strftime('%Y-%m-%d')}")
        log.info("=" * 60)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
