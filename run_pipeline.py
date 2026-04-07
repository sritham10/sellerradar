"""
SellerRadar Master Pipeline Runner
Simulates what Airflow DAG does — runs all tasks in sequence.
This is the local development equivalent of MWAA.
"""

import boto3
import time
import sys
from datetime import datetime, timezone

REGION = "ap-south-1"
APP_ID = "00g4lntkb5op261t"
EMR_ROLE = "arn:aws:iam::253025918226:role/sellerradar-emr-role"
SCRIPTS_BUCKET = "s3://sellerradar-scripts-sritham/jobs"
LOGS_BUCKET = "s3://sellerradar-emr-logs-sritham/"
ICEBERG_JAR = "/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar"
GLUE_FACTORY = "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"

emr = boto3.client("emr-serverless", region_name=REGION)


def log(msg, level="INFO"):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] [{level}] {msg}")


def submit_emr_job(script_name):
    log(f"Submitting EMR job: {script_name}")
    response = emr.start_job_run(
        applicationId=APP_ID,
        executionRoleArn=EMR_ROLE,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": f"{SCRIPTS_BUCKET}/{script_name}",
                "sparkSubmitParameters": (
                    f"--conf spark.jars={ICEBERG_JAR} "
                    f"--conf spark.hadoop.hive.metastore.client.factory.class={GLUE_FACTORY}"
                )
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": LOGS_BUCKET}
            }
        }
    )
    job_id = response["jobRunId"]
    log(f"Job submitted → {job_id}")
    return job_id


def wait_for_job(job_id, script_name):
    log(f"Waiting for {script_name}...")
    while True:
        response = emr.get_job_run(applicationId=APP_ID, jobRunId=job_id)
        state = response["jobRun"]["state"]
        log(f"  {script_name} status: {state}")

        if state == "SUCCESS":
            log(f"{script_name} COMPLETED", "SUCCESS")
            return True
        elif state in ["FAILED", "CANCELLED"]:
            details = response["jobRun"].get("stateDetails", "")
            log(f"{script_name} FAILED: {details}", "ERROR")
            return False

        time.sleep(30)


def run_cdc_simulator():
    log("Running CDC simulator...")
    import random
    import boto3 as b3
    from datetime import datetime, timezone

    s3 = b3.client("s3", region_name=REGION)
    BUCKET = "sellerradar-bronze-sritham"
    CDC_PREFIX = "cdc/seller_data"

    ASINS = ["B097D69GJ1","B0GKJ1DJRX","B09NCFVNK9","B0CH3G9VR2",
             "B0GS6L79VS","B09HGSCL9Q","B0D772K8X8","B0D6W7F7WH",
             "B0CNXW42H8","B0CN3D7CL2","B0FLYDHYY4"]

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
        "B097D69GJ1": 999.0,  "B0GKJ1DJRX": 2599.0,
        "B09NCFVNK9": 1499.0, "B0CH3G9VR2": 999.0,
        "B0GS6L79VS": 1399.0, "B09HGSCL9Q": 8499.0,
        "B0D772K8X8": 799.0,  "B0D6W7F7WH": 1699.0,
        "B0CNXW42H8": 5999.0, "B0CN3D7CL2": 8399.0,
        "B0FLYDHYY4": 4599.0,
    }

    CITIES = ["Bengaluru", "Mumbai", "Delhi", "Hyderabad", "Chennai"]
    STATES = {
        "Bengaluru": "Karnataka", "Mumbai": "Maharashtra",
        "Delhi": "Delhi", "Hyderabad": "Telangana",
        "Chennai": "Tamil Nadu"
    }

    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y/%m/%d")
    active_orders = {}
    events = 0

    for i in range(20):
        rand = random.random()
        ts = datetime.now(timezone.utc)
        ts_str = ts.strftime("%Y%m%d%H%M%S%f")

        if rand < 0.50 or not active_orders:
            asin = random.choice(ASINS)
            city = random.choice(CITIES)
            order_id = f"ORD-{now.strftime('%Y%m%d')}-{i+1:04d}"
            order = {
                "order_id": order_id, "asin": asin,
                "title": TITLES[asin],
                "qty": random.randint(1, 3),
                "price": round(PRICES[asin] * random.uniform(0.95, 1.0), 2),
                "status": "pending", "city": city,
                "state": STATES[city]
            }
            active_orders[order_id] = order
            row = (f"I,{ts.isoformat()},{order_id},{asin},"
                   f"{TITLES[asin]},{order['qty']},{order['price']},"
                   f"pending,{city},{STATES[city]},amazon_in,"
                   f"{ts.isoformat()},{ts.isoformat()}")
            key = f"{CDC_PREFIX}/public/seller_orders/{date_str}/orders_{ts_str}.csv"
            s3.put_object(Bucket=BUCKET, Key=key, Body=row, ContentType="text/csv")
            log(f"  INSERT order {order_id} — {TITLES[asin][:25]} — ₹{order['price']}")

        elif rand < 0.80 and active_orders:
            order_id = random.choice(list(active_orders.keys()))
            flow = {"pending": "confirmed", "confirmed": "shipped", "shipped": "delivered"}
            current = active_orders[order_id]["status"]
            if current in flow:
                new_status = flow[current]
                active_orders[order_id]["status"] = new_status
                o = active_orders[order_id]
                row = (f"U,{ts.isoformat()},{order_id},{o['asin']},"
                       f"{o['title']},{o['qty']},{o['price']},"
                       f"{new_status},{o['city']},{o['state']},amazon_in,"
                       f"{ts.isoformat()},{ts.isoformat()}")
                key = f"{CDC_PREFIX}/public/seller_orders/{date_str}/orders_{ts_str}.csv"
                s3.put_object(Bucket=BUCKET, Key=key, Body=row, ContentType="text/csv")
                log(f"  UPDATE order {order_id} → {new_status}")

        else:
            asin = random.choice(ASINS)
            stock = random.randint(20, 150)
            row = (f"U,{ts.isoformat()},{asin},{TITLES[asin]},"
                   f"{stock},25,0,{ts.isoformat()}")
            key = f"{CDC_PREFIX}/public/seller_inventory/{date_str}/inv_{ts_str}.csv"
            s3.put_object(Bucket=BUCKET, Key=key, Body=row, ContentType="text/csv")
            log(f"  UPDATE inventory {asin} — stock: {stock}")

        events += 1
        time.sleep(0.3)

    log(f"CDC complete — {events} events, {len(active_orders)} orders", "SUCCESS")


def main():
    print("=" * 60)
    print("SellerRadar Master Pipeline")
    print(f"Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    # ── TASK 1: Bronze → Silver ──────────────────────────────────
    print(f"\n{'─'*60}")
    log("TASK 1: Bronze → Silver")
    job_id = submit_emr_job("bronze_to_silver.py")

    # ── TASK 2: Wait for Bronze → Silver ────────────────────────
    log("TASK 2: Waiting for Bronze → Silver")
    success = wait_for_job(job_id, "bronze_to_silver")
    if not success:
        log("Pipeline stopped — Bronze→Silver failed", "ERROR")
        sys.exit(1)

    # ── TASK 3: Silver → Gold ────────────────────────────────────
    print(f"\n{'─'*60}")
    log("TASK 3: Silver → Gold")
    job_id = submit_emr_job("silver_to_gold.py")

    # ── TASK 4: Wait for Silver → Gold ──────────────────────────
    log("TASK 4: Waiting for Silver → Gold")
    success = wait_for_job(job_id, "silver_to_gold")
    if not success:
        log("Pipeline stopped — Silver→Gold failed", "ERROR")
        sys.exit(1)

    # ── TASK 5: CDC Simulator ────────────────────────────────────
    print(f"\n{'─'*60}")
    log("TASK 5: CDC Simulator")
    run_cdc_simulator()

    print(f"\n{'='*60}")
    log("ALL TASKS COMPLETED SUCCESSFULLY", "SUCCESS")
    print(f"Finished: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)


if __name__ == "__main__":
    main()