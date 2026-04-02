import requests
import json
import boto3
from datetime import datetime, timezone
import time

# ── CONFIG ──────────────────────────────────────────────────────
# Your Rainforest API key — store it here for now
# Later in the project we'll move this to AWS Secrets Manager
RAINFOREST_API_KEY = "8F0CC20E1A464AA5B0AC75FE1BB71797"  # paste your key here

# Your S3 bucket name
BUCKET = "sellerradar-bronze-sritham"  # change to your bucket name

# The 15 ASINs you picked — replace with your actual ones
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

# ── AWS CONNECTION ───────────────────────────────────────────────
# boto3 connects to S3 using the credentials you set in aws configure
# region ap-south-1 is Mumbai — closest to Bengaluru
s3 = boto3.client("s3", region_name="ap-south-1")


# ── SCRAPE ONE PRODUCT ───────────────────────────────────────────
def scrape_product(asin):
    """
    Calls Rainforest API for one ASIN.
    Returns a clean dictionary with the product data.
    Returns None if the call fails.
    """
    print(f"Fetching ASIN: {asin}...")

    # This is the Rainforest API endpoint
    # We pass our API key and the ASIN we want
    # amazon_domain tells it which Amazon country site to use
    url = "https://api.rainforestapi.com/request"
    params = {
        "api_key": RAINFOREST_API_KEY,
        "type": "product",
        "asin": asin,
        "amazon_domain": "amazon.in",
    }

    try:
        # Send the request to Rainforest API
        # timeout=15 means if no response in 15 seconds, give up
        response = requests.get(url, params=params, timeout=15)

        # If the API returned an error status code, raise an exception
        response.raise_for_status()

        # Parse the JSON response into a Python dictionary
        data = response.json()

        # The actual product data is inside the "product" key
        product = data.get("product", {})

        # ── EXTRACT WHAT WE NEED ─────────────────────────────────
        # We pull only the fields that matter for SellerRadar
        # .get() is safe — if the field doesn't exist, returns None
        # instead of crashing

        # Price — Rainforest gives price as a float like 1299.0
        price = product.get("buybox_winner", {}).get("price", {}).get("value")

        # Title of the product
        title = product.get("title")

        # Rating out of 5
        rating = product.get("rating")

        # Total number of reviews
        reviews_count = product.get("ratings_total")

        # Is it in stock?
        in_stock = product.get("buybox_winner", {}).get("availability", {}).get("type")

        # Main category
        category = product.get("categories", [{}])[0].get("name") if product.get("categories") else None

        # ── BUILD OUR RECORD ─────────────────────────────────────
        # This is what gets saved to S3 Bronze layer
        # We always save exactly what we received — no transformation
        # Transformation happens later in the Silver layer
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

        print(f"  Title:  {title[:50] if title else 'N/A'}...")
        print(f"  Price:  ₹{price}")
        print(f"  Rating: {rating} ({reviews_count} reviews)")
        print(f"  Stock:  {in_stock}")
        print()

        return record

    except requests.exceptions.RequestException as e:
        # Network error or API error
        print(f"  API call failed for {asin}: {e}")
        return None

    except Exception as e:
        # Anything else that goes wrong
        print(f"  Unexpected error for {asin}: {e}")
        return None


# ── SAVE ONE RECORD TO S3 ────────────────────────────────────────
def save_to_s3(record):
    """
    Saves one product record as a JSON file to S3 Bronze layer.

    The folder structure is:
    raw/amazon/YYYY-MM-DD/ASIN_timestamp.json

    Why this structure?
    - Partitioned by date so Athena queries are faster and cheaper
    - One file per product per scrape so nothing overwrites anything
    - We can replay any day's data if something goes wrong
    """
    date = record["scraped_at"][:10]  # extract just YYYY-MM-DD
    timestamp = record["scraped_at"].replace(":", "-")  # colons not allowed in S3 keys
    key = f"raw/amazon/{date}/{record['asin']}_{timestamp}.json"

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(record, indent=2),
        ContentType="application/json",
    )

    print(f"  Saved to S3: {key}")


# ── MAIN ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"Starting SellerRadar scrape for {len(ASINS)} products")
    print(f"Time: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    success_count = 0
    fail_count = 0

    for asin in ASINS:
        record = scrape_product(asin)

        if record:
            save_to_s3(record)
            success_count += 1
        else:
            fail_count += 1

        # Wait 1 second between calls
        # Rainforest API has rate limits on the free tier
        # Being polite to the API means no throttling
        time.sleep(1)

    print("=" * 60)
    print(f"Done. Success: {success_count} | Failed: {fail_count}")
    print(f"Check your S3 bucket: {BUCKET}")