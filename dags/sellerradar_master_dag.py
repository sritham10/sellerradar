from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import boto3
import time

# ── DAG CONFIG ───────────────────────────────────────────────────
default_args = {
    "owner": "sellerradar",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="sellerradar_master_pipeline",
    default_args=default_args,
    description="SellerRadar daily pipeline — Bronze→Silver→Gold + CDC",
    schedule_interval="30 0 * * *",  # 6 AM IST = 00:30 UTC daily
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["sellerradar", "daily", "production"]
)

# ── CONSTANTS ────────────────────────────────────────────────────
REGION = "ap-south-1"
APP_ID = "00g4lntkb5op261t"
EMR_ROLE = "arn:aws:iam::253025918226:role/sellerradar-emr-role"
SCRIPTS_BUCKET = "s3://sellerradar-scripts-sritham/jobs"
LOGS_BUCKET = "s3://sellerradar-emr-logs-sritham/"
ICEBERG_JAR = "/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar"
GLUE_FACTORY = "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"

SPARK_PARAMS = (
    f"--conf spark.jars={ICEBERG_JAR} "
    f"--conf spark.hadoop.hive.metastore.client.factory.class={GLUE_FACTORY}"
)

emr = boto3.client("emr-serverless", region_name=REGION)


def submit_emr_job(script_name: str) -> str:
    """
    Submits a PySpark script to EMR Serverless.
    Returns the job_run_id for monitoring.
    """
    response = emr.start_job_run(
        applicationId=APP_ID,
        executionRoleArn=EMR_ROLE,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": f"{SCRIPTS_BUCKET}/{script_name}",
                "sparkSubmitParameters": SPARK_PARAMS
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": LOGS_BUCKET
                }
            }
        }
    )
    job_run_id = response["jobRunId"]
    print(f"Submitted {script_name} → job run ID: {job_run_id}")
    return job_run_id


def wait_for_emr_job(job_run_id: str, script_name: str) -> None:
    """
    Polls EMR Serverless until job completes or fails.
    Raises exception on failure so Airflow marks task as failed
    and triggers retry logic.
    """
    print(f"Waiting for {script_name} (job: {job_run_id})")

    while True:
        response = emr.get_job_run(
            applicationId=APP_ID,
            jobRunId=job_run_id
        )
        state = response["jobRun"]["state"]
        print(f"  Status: {state}")

        if state == "SUCCESS":
            print(f"{script_name} completed successfully")
            return

        elif state in ["FAILED", "CANCELLED"]:
            details = response["jobRun"].get("stateDetails", "No details")
            raise Exception(f"{script_name} failed: {details}")

        time.sleep(30)


def run_bronze_to_silver(**context):
    """
    Task 1: Submit Bronze → Silver PySpark job.
    Reads raw JSON from S3 Bronze, applies 6-pillar DQ,
    writes clean Iceberg table to S3 Silver.
    """
    job_id = submit_emr_job("bronze_to_silver.py")
    context["ti"].xcom_push(key="b2s_job_id", value=job_id)


def wait_bronze_to_silver(**context):
    """
    Task 2: Wait for Bronze → Silver job to complete.
    XCom pulls the job ID submitted in Task 1.
    """
    job_id = context["ti"].xcom_pull(
        task_ids="run_bronze_to_silver",
        key="b2s_job_id"
    )
    wait_for_emr_job(job_id, "bronze_to_silver.py")


def run_silver_to_gold(**context):
    """
    Task 3: Submit Silver → Gold PySpark job.
    Reads clean Silver data, builds 5 KPI Gold tables
    using window functions and aggregations.
    """
    job_id = submit_emr_job("silver_to_gold.py")
    context["ti"].xcom_push(key="s2g_job_id", value=job_id)


def wait_silver_to_gold(**context):
    """
    Task 4: Wait for Silver → Gold job to complete.
    """
    job_id = context["ti"].xcom_pull(
        task_ids="run_silver_to_gold",
        key="s2g_job_id"
    )
    wait_for_emr_job(job_id, "silver_to_gold.py")


def run_cdc_simulator(**context):
    """
    Task 5: Generate seller CDC events.
    Simulates 20 order and inventory change events,
    writes them to S3 Bronze/cdc/ in DMS CSV format,
    and writes actual changes to RDS Postgres.
    """
    import sys
    sys.path.insert(0, "/usr/local/airflow/dags")

    import random
    import psycopg2
    import boto3 as b3
    from decimal import Decimal
    from datetime import datetime, timezone

    BUCKET = "sellerradar-bronze-sritham"
    CDC_PREFIX = "cdc/seller_data"
    RDS_HOST = "sellerradar-postgres.c1w4k6m0skeu.ap-south-1.rds.amazonaws.com"

    s3_client = b3.client("s3", region_name=REGION)

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
    order_counter = 0
    active_orders = {}

    def write_cdc(operation, table, data):
        op = {"insert": "I", "update": "U", "delete": "D"}[operation]
        ts = datetime.now(timezone.utc)
        ts_str = ts.strftime("%Y%m%d%H%M%S%f")
        key = f"{CDC_PREFIX}/public/{table}/{date_str}/{table}_{ts_str}.csv"

        if table == "seller_orders":
            row = (f"{op},{ts.isoformat()},{data.get('order_id')},"
                   f"{data.get('asin')},{data.get('product_title')},"
                   f"{data.get('quantity')},{data.get('sale_price')},"
                   f"{data.get('order_status')},{data.get('customer_city')},"
                   f"{data.get('customer_state')},amazon_in,"
                   f"{data.get('created_at', ts.isoformat())},{ts.isoformat()}")
        else:
            row = (f"{op},{ts.isoformat()},{data.get('asin')},"
                   f"{data.get('product_title')},{data.get('stock_quantity')},"
                   f"{data.get('reorder_point', 25)},{data.get('unit_cost', 0)},"
                   f"{ts.isoformat()}")

        s3_client.put_object(Bucket=BUCKET, Key=key,
                             Body=row, ContentType="text/csv")
        print(f"CDC [{op}] {table}")

    for i in range(20):
        nonlocal_counter = i + 1
        rand = random.random()

        if rand < 0.50 or not active_orders:
            asin = random.choice(ASINS)
            city = random.choice(CITIES)
            order_id = f"ORD-{now.strftime('%Y%m%d')}-{nonlocal_counter:04d}"
            order = {
                "order_id": order_id,
                "asin": asin,
                "product_title": TITLES[asin],
                "quantity": random.randint(1, 3),
                "sale_price": round(PRICES[asin] * random.uniform(0.95, 1.0), 2),
                "order_status": "pending",
                "customer_city": city,
                "customer_state": STATES[city],
                "created_at": now.isoformat()
            }
            active_orders[order_id] = order
            write_cdc("insert", "seller_orders", order)

        elif rand < 0.80:
            order_id = random.choice(list(active_orders.keys()))
            flow = {"pending": "confirmed", "confirmed": "shipped", "shipped": "delivered"}
            current = active_orders[order_id]["order_status"]
            if current in flow:
                active_orders[order_id]["order_status"] = flow[current]
                write_cdc("update", "seller_orders", active_orders[order_id])

        else:
            asin = random.choice(ASINS)
            write_cdc("update", "seller_inventory", {
                "asin": asin,
                "product_title": TITLES[asin],
                "stock_quantity": random.randint(20, 150)
            })

    print(f"CDC simulation complete — {len(active_orders)} orders created")


# ── TASK DEFINITIONS ─────────────────────────────────────────────
start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

t1 = PythonOperator(
    task_id="run_bronze_to_silver",
    python_callable=run_bronze_to_silver,
    dag=dag
)

t2 = PythonOperator(
    task_id="wait_for_bronze_to_silver",
    python_callable=wait_bronze_to_silver,
    dag=dag
)

t3 = PythonOperator(
    task_id="run_silver_to_gold",
    python_callable=run_silver_to_gold,
    dag=dag
)

t4 = PythonOperator(
    task_id="wait_for_silver_to_gold",
    python_callable=wait_silver_to_gold,
    dag=dag
)

t5 = PythonOperator(
    task_id="run_cdc_simulator",
    python_callable=run_cdc_simulator,
    dag=dag
)

# ── TASK DEPENDENCIES ────────────────────────────────────────────
# This defines the order of execution:
# start → t1 → t2 → t3 → t4 → t5 → end
# Each task must complete successfully before the next starts

start >> t1 >> t2 >> t3 >> t4 >> t5 >> end