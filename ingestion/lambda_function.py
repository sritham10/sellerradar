import json
import boto3
import urllib.request
import urllib.parse
from datetime import datetime, timezone
import time

RAINFOREST_API_KEY = "8F0CC20E1A464AA5B0AC75FE1BB71797"
BUCKET = "sellerradar-bronze-sritham"

ASINS = [
    "B097D69GJ1",
    "B0GKJ1DJRX",
    "B09NCFVNK9",
    "B0CH3G9VR2",
    "B0GS6L79VS",
    "B09HGSCL9Q",
    "B0D772K8X8",
    "B0D6W7F7WH",
    "B0CNXW42H8",
    "B0CN3D7CL2",
    "B0FLYDHYY4"
]

s3 = boto3.client("s3", region_name="ap-south-1")

def scrape_product(asin):
    params = urllib.parse.urlencode({
        "api_key": RAINFOREST_API_KEY,
        "type": "product",
        "asin": asin,
        "amazon_domain": "amazon.in",
    })
    url = f"https://api.rainforestapi.com/request?{params}"

    try:
        with urllib.request.urlopen(url, timeout=15) as response:
            data = json.loads(response.read().decode())

        product = data.get("product", {})
        price = product.get("buybox_winner", {}).get("price", {}).get("value")
        title = product.get("title")
        rating = product.get("rating")
        reviews_count = product.get("ratings_total")
        in_stock = product.get("buybox_winner", {}).get("availability", {}).get("type")
        category = product.get("categories", [{}])[0].get("name") if product.get("categories") else None

        record = {
            "asin": asin,
            "title": title,
            "price": price,
            "rating": rating,
            "reviews_count": reviews_count,
            "in_stock": in_stock,
            "category": category,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "source": "amazon_in",
        }

        print(f"Scraped: {asin} | Price: {price} | Rating: {rating}")
        return record

    except Exception as e:
        print(f"Failed for {asin}: {e}")
        return None

def save_to_s3(record):
    date = record["scraped_at"][:10]
    timestamp = record["scraped_at"].replace(":", "-")
    key = f"raw/amazon/{date}/{record['asin']}_{timestamp}.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(record, indent=2),
        ContentType="application/json",
    )
    print(f"Saved to S3: {key}")

def lambda_handler(event, context):
    print(f"Starting scrape for {len(ASINS)} products")
    success_count = 0
    fail_count = 0

    for asin in ASINS:
        record = scrape_product(asin)
        if record:
            save_to_s3(record)
            success_count += 1
        else:
            fail_count += 1
        time.sleep(1)

    print(f"Done. Success: {success_count} | Failed: {fail_count}")
    return {
        "statusCode": 200,
        "body": json.dumps({
            "success": success_count,
            "failed": fail_count
        })
    }