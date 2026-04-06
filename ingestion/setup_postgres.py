import psycopg2

conn = psycopg2.connect(
    host="sellerradar-postgres.c1w4k6m0skeu.ap-south-1.rds.amazonaws.com",
    port=5432,
    database="postgres",
    user="selleradmin",
    password="SellerRadar2026!"
)

cursor = conn.cursor()
print("Connected to RDS Postgres successfully")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS seller_orders (
        order_id        VARCHAR(50)     PRIMARY KEY,
        asin            VARCHAR(20)     NOT NULL,
        product_title   VARCHAR(500),
        quantity        INT             NOT NULL DEFAULT 1,
        sale_price      DECIMAL(10,2)   NOT NULL,
        order_status    VARCHAR(20)     NOT NULL DEFAULT 'pending',
        customer_city   VARCHAR(100),
        customer_state  VARCHAR(100),
        marketplace     VARCHAR(50)     DEFAULT 'amazon_in',
        created_at      TIMESTAMP       DEFAULT NOW(),
        updated_at      TIMESTAMP       DEFAULT NOW()
    )
""")
print("seller_orders table created")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS seller_inventory (
        asin            VARCHAR(20)     PRIMARY KEY,
        product_title   VARCHAR(500),
        stock_quantity  INT             NOT NULL DEFAULT 0,
        reorder_point   INT             NOT NULL DEFAULT 50,
        unit_cost       DECIMAL(10,2),
        last_restocked  TIMESTAMP       DEFAULT NOW(),
        updated_at      TIMESTAMP       DEFAULT NOW()
    )
""")
print("seller_inventory table created")

cursor.execute("""
    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
    END;
    $$ language 'plpgsql'
""")

cursor.execute("DROP TRIGGER IF EXISTS update_seller_orders_updated_at ON seller_orders")
cursor.execute("""
    CREATE TRIGGER update_seller_orders_updated_at
        BEFORE UPDATE ON seller_orders
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column()
""")

cursor.execute("DROP TRIGGER IF EXISTS update_seller_inventory_updated_at ON seller_inventory")
cursor.execute("""
    CREATE TRIGGER update_seller_inventory_updated_at
        BEFORE UPDATE ON seller_inventory
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column()
""")
print("Updated_at triggers created")

inventory_data = [
    ("B097D69GJ1", "Portronics SoundDrum Speaker",  150, 30, 650.0),
    ("B0GKJ1DJRX", "acer GlowTime Speaker",          80, 20, 1800.0),
    ("B09NCFVNK9", "JBL Go Essential Speaker",       200, 50, 950.0),
    ("B0CH3G9VR2", "Boat Aavante Bar 490",           120, 25, 700.0),
    ("B0GS6L79VS", "GOBOULT Rave Q12 Speaker",        90, 20, 950.0),
    ("B09HGSCL9Q", "JBL Flip 6 Speaker",              60, 15, 5500.0),
    ("B0D772K8X8", "pTron Fusion Tunes Speaker",     180, 40, 500.0),
    ("B0D6W7F7WH", "Boat Stone 352 Pro",             100, 25, 1100.0),
    ("B0CNXW42H8", "Sony SRS-XB100 Speaker",          70, 15, 3800.0),
    ("B0CN3D7CL2", "JBL Xtreme 3 Speaker",            45, 10, 5500.0),
    ("B0FLYDHYY4", "boAt Stone 1400 Speaker",        110, 25, 3000.0),
]

for asin, title, stock, reorder, cost in inventory_data:
    cursor.execute("""
        INSERT INTO seller_inventory
            (asin, product_title, stock_quantity, reorder_point, unit_cost)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (asin) DO UPDATE SET
            stock_quantity = EXCLUDED.stock_quantity,
            updated_at = NOW()
    """, (asin, title, stock, reorder, cost))

print("Initial inventory seeded for 11 products")

conn.commit()

cursor.execute("SELECT COUNT(*) FROM seller_inventory")
print(f"Inventory rows: {cursor.fetchone()[0]}")

cursor.execute("SELECT COUNT(*) FROM seller_orders")
print(f"Order rows: {cursor.fetchone()[0]}")

cursor.execute("SELECT asin, product_title, stock_quantity FROM seller_inventory ORDER BY asin")
print("\nInitial inventory:")
for row in cursor.fetchall():
    print(f"  {row[0]} | {row[1][:30]} | Stock: {row[2]}")

cursor.close()
conn.close()
print("\nPostgres setup complete.")