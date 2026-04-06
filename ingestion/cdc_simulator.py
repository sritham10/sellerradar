import boto3
import json
import psycopg2
import random
import time
from datetime import datetime, timezone

# ── CONFIG ───────────────────────────────────────────────────────
BUCKET = "sellerradar-bronze-sritham"
CDC_PREFIX = "cdc/seller_data"
RDS_HOST = "sellerradar-postgres.c1w4k6m0skeu.ap-south-1.rds.amazonaws.com"

s3 = boto3.client("s3", region_name="ap-south-1")

ASINS = [
    "B097D69GJ1", "B0GKJ1DJRX", "B09NCFVNK9", "B0CH3G9VR2",
    "B0GS6L79VS", "B09HGSCL9Q", "B0D772K8X8", "B0D6W7F7WH",
    "B0CNXW42H8", "B0CN3D7CL2", "B0FLYDHYY4"
]

TITLES = {
    "B097D69GJ1": "Portronics SoundDrum Speaker",
    "B0GKJ1DJRX": "acer GlowTime Speaker",
    "B09NCFVNK9": "JBL Go Essential Speaker",
    "B0CH3G9VR2": "Boat Aavante Bar 490",
    "B0GS6L79VS": "GOBOULT Rave Q12 Speaker",
    "B09HGSCL9Q": "JBL Flip 6 Speaker",
    "B0D772K8X8": "pTron Fusion Tunes Speaker",
    "B0D6W7F7WH": "Boat Stone 352 Pro",
    "B0CNXW42H8": "Sony SRS-XB100 Speaker",
    "B0CN3D7CL2": "JBL Xtreme 3 Speaker",
    "B0FLYDHYY4": "boAt Stone 1400 Speaker",
}

PRICES = {
    "B097D69GJ1": 999.0,  "B0GKJ1DJRX": 2599.0, "B09NCFVNK9": 1499.0,
    "B0CH3G9VR2": 999.0,  "B0GS6L79VS": 1399.0, "B09HGSCL9Q": 8499.0,
    "B0D772K8X8": 799.0,  "B0D6W7F7WH": 1699.0, "B0CNXW42H8": 5999.0,
    "B0CN3D7CL2": 8399.0, "B0FLYDHYY4": 4599.0,
}

CITIES = [
    "Bengaluru", "Mumbai", "Delhi", "Hyderabad", "Chennai",
    "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Lucknow"
]

STATES = {
    "Bengaluru": "Karnataka", "Mumbai": "Maharashtra",
    "Delhi": "Delhi", "Hyderabad": "Telangana",
    "Chennai": "Tamil Nadu", "Pune": "Maharashtra",
    "Kolkata": "West Bengal", "Ahmedabad": "Gujarat",
    "Jaipur": "Rajasthan", "Lucknow": "Uttar Pradesh"
}

# Track orders in memory for status updates
active_orders = {}
order_counter = 0


def write_cdc_event_to_s3(operation, table, data):
    """
    Writes a CDC event to S3 in DMS format.

    This is exactly what DMS would write — same structure,
    same fields, same operation codes.
    I, U, D = Insert, Update, Delete (DMS convention)
    """
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y/%m/%d")
    timestamp_str = now.strftime("%Y%m%d%H%M%S%f")

    # DMS writes one file per batch of changes
    # We write one file per event for simplicity
    key = f"{CDC_PREFIX}/public/{table}/{date_str}/{table}_{timestamp_str}.csv"

    # DMS CSV format: operation, timestamp, then all columns
    # Operation codes: I=Insert, U=Update, D=Delete
    op_code = {"insert": "I", "update": "U", "delete": "D"}[operation]

    # Build CSV row in DMS format
    if table == "seller_orders":
        csv_row = (
            f"{op_code},"
            f"{now.isoformat()},"
            f"{data.get('order_id', '')},"
            f"{data.get('asin', '')},"
            f"{data.get('product_title', '')},"
            f"{data.get('quantity', '')},"
            f"{data.get('sale_price', '')},"
            f"{data.get('order_status', '')},"
            f"{data.get('customer_city', '')},"
            f"{data.get('customer_state', '')},"
            f"{data.get('marketplace', 'amazon_in')},"
            f"{data.get('created_at', now.isoformat())},"
            f"{now.isoformat()}"
        )
    else:  # seller_inventory
        csv_row = (
            f"{op_code},"
            f"{now.isoformat()},"
            f"{data.get('asin', '')},"
            f"{data.get('product_title', '')},"
            f"{data.get('stock_quantity', '')},"
            f"{data.get('reorder_point', '')},"
            f"{data.get('unit_cost', '')},"
            f"{now.isoformat()}"
        )

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=csv_row,
        ContentType="text/csv"
    )

    print(f"  CDC [{op_code}] {table}: {key.split('/')[-1]}")
    return True


def write_to_postgres(operation, table, data):
    """
    Also writes the actual change to Postgres.
    This keeps Postgres in sync with our S3 CDC events.
    """
    try:
        conn = psycopg2.connect(
            host=RDS_HOST, port=5432,
            database="postgres",
            user="selleradmin",
            password="SellerRadar2026!"
        )
        cursor = conn.cursor()

        if table == "seller_orders" and operation == "insert":
            cursor.execute("""
                INSERT INTO seller_orders
                    (order_id, asin, product_title, quantity,
                     sale_price, order_status, customer_city,
                     customer_state, marketplace, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING
            """, (
                data["order_id"], data["asin"], data["product_title"],
                data["quantity"], data["sale_price"], data["order_status"],
                data["customer_city"], data["customer_state"],
                data.get("marketplace", "amazon_in"),
                data.get("created_at", datetime.now())
            ))

        elif table == "seller_orders" and operation == "update":
            cursor.execute("""
                UPDATE seller_orders
                SET order_status = %s, updated_at = NOW()
                WHERE order_id = %s
            """, (data["order_status"], data["order_id"]))

        elif table == "seller_inventory" and operation == "update":
            cursor.execute("""
                UPDATE seller_inventory
                SET stock_quantity = %s, updated_at = NOW()
                WHERE asin = %s
            """, (data["stock_quantity"], data["asin"]))

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"  Postgres write failed: {e}")


def simulate_new_order():
    """
    Simulates a customer placing a new order.
    INSERT event on seller_orders table.
    """
    global order_counter
    order_counter += 1

    asin = random.choice(ASINS)
    city = random.choice(CITIES)
    quantity = random.randint(1, 3)
    sale_price = PRICES[asin] * random.uniform(0.95, 1.0)

    order_data = {
        "order_id": f"ORD-{datetime.now().strftime('%Y%m%d')}-{order_counter:04d}",
        "asin": asin,
        "product_title": TITLES[asin],
        "quantity": quantity,
        "sale_price": round(sale_price, 2),
        "order_status": "pending",
        "customer_city": city,
        "customer_state": STATES[city],
        "marketplace": "amazon_in",
        "created_at": datetime.now(timezone.utc).isoformat()
    }

    active_orders[order_data["order_id"]] = order_data

    print(f"\nNEW ORDER: {order_data['order_id']} | "
          f"{TITLES[asin][:30]} | "
          f"Qty: {quantity} | "
          f"₹{order_data['sale_price']} | "
          f"{city}")

    write_cdc_event_to_s3("insert", "seller_orders", order_data)
    write_to_postgres("insert", "seller_orders", order_data)
    return order_data["order_id"]


def simulate_order_update(order_id):
    """
    Simulates order status progression.
    pending → confirmed → shipped → delivered
    UPDATE event on seller_orders table.
    """
    if order_id not in active_orders:
        return

    status_flow = {
        "pending": "confirmed",
        "confirmed": "shipped",
        "shipped": "delivered"
    }

    current_status = active_orders[order_id]["order_status"]
    if current_status not in status_flow:
        return

    new_status = status_flow[current_status]
    active_orders[order_id]["order_status"] = new_status

    update_data = {
        "order_id": order_id,
        "order_status": new_status
    }

    print(f"\nORDER UPDATE: {order_id} | "
          f"{current_status} → {new_status}")

    write_cdc_event_to_s3("update", "seller_orders", {
        **active_orders[order_id], **update_data
    })
    write_to_postgres("update", "seller_orders", update_data)


def simulate_inventory_update(asin):
    """
    Simulates inventory decreasing after sales.
    UPDATE event on seller_inventory table.
    """
    inventory_data = {
        "asin": asin,
        "product_title": TITLES[asin],
        "stock_quantity": random.randint(20, 150)
    }

    print(f"\nINVENTORY UPDATE: {asin} | "
          f"New stock: {inventory_data['stock_quantity']}")

    write_cdc_event_to_s3("update", "seller_inventory", inventory_data)
    write_to_postgres("update", "seller_inventory", inventory_data)


def run_simulation(num_events=20):
    """
    Runs the CDC simulation.
    Generates realistic mix of orders and inventory updates.
    """
    print("=" * 60)
    print("SellerRadar CDC Simulator")
    print(f"Generating {num_events} CDC events")
    print("=" * 60)

    order_ids = []

    for i in range(num_events):
        rand = random.random()

        if rand < 0.50 or not order_ids:
            # 50% — new order
            order_id = simulate_new_order()
            order_ids.append(order_id)

        elif rand < 0.80 and order_ids:
            # 30% — update existing order status
            order_id = random.choice(order_ids)
            simulate_order_update(order_id)

        else:
            # 20% — inventory update
            asin = random.choice(ASINS)
            simulate_inventory_update(asin)

        time.sleep(0.5)

    print("\n" + "=" * 60)
    print(f"CDC simulation complete")
    print(f"Total orders created: {len(order_ids)}")
    print(f"Check S3: s3://{BUCKET}/{CDC_PREFIX}/")
    print("=" * 60)


if __name__ == "__main__":
    run_simulation(num_events=20)