# Databricks notebook source
pip install requests

# COMMAND ----------

api_key = dbutils.fs.head("dbfs:/configs/api_key.txt")

# COMMAND ----------

import requests
import os

TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIxMmM4ZDc0ZmY2ZGNiMzI2ZmQyYmFjNDhlMjEwODM0MCIsIm5iZiI6MTc2MzgyNDg2Ni4yMTQwMDAyLCJzdWIiOiI2OTIxZDRlMmQ0NTk3MDFiZDJiNzQwMWQiLCJzY29wZXMiOlsiYXBpX3JlYWQiXSwidmVyc2lvbiI6MX0.TDvPi__fC0GSenUywCe5UOl6LU08FMncX5YIrFqeBsE"

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "accept": "application/json"
}

url = "https://api.themoviedb.org/3/discover/person"
params = {
    "page": 1  # IMPORTANT
}
response = requests.get(url, headers=headers,params=params)
data = response.json()

print("Current page:", data.get("page"))
print("Total pages:", data.get("total_pages"))
print("Total results:", data.get("total_results"))

# COMMAND ----------

import requests

TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIxMmM4ZDc0ZmY2ZGNiMzI2ZmQyYmFjNDhlMjEwODM0MCIsIm5iZiI6MTc2MzgyNDg2Ni4yMTQwMDAyLCJzdWIiOiI2OTIxZDRlMmQ0NTk3MDFiZDJiNzQwMWQiLCJzY29wZXMiOlsiYXBpX3JlYWQiXSwidmVyc2lvbiI6MX0.TDvPi__fC0GSenUywCe5UOl6LU08FMncX5YIrFqeBsE"
headers = {
    "Authorization": f"Bearer {TOKEN}",
    "accept": "application/json"
}

movie_id = 1419406
url = f"https://api.themoviedb.org/3/movie/{movie_id}/credits"

response = requests.get(url, headers=headers)
credits = response.json()

# Extract cast
first_actor = cast_list[0]
print(first_actor.values())

# COMMAND ----------

import requests

# === AUTH ===
TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIxMmM4ZDc0ZmY2ZGNiMzI2ZmQyYmFjNDhlMjEwODM0MCIsIm5iZiI6MTc2MzgyNDg2Ni4yMTQwMDAyLCJzdWIiOiI2OTIxZDRlMmQ0NTk3MDFiZDJiNzQwMWQiLCJzY29wZXMiOlsiYXBpX3JlYWQiXSwidmVyc2lvbiI6MX0.TDvPi__fC0GSenUywCe5UOl6LU08FMncX5YIrFqeBsE"   # <-- replace with your TMDB token
headers = {
    "Authorization": f"Bearer {TOKEN}",
    "accept": "application/json"
}

# === MOVIE ID ===
movie_id = 1419406   # The Shadow’s Edge

# === STEP 1: GET MOVIE CREDITS ===
credits_url = f"https://api.themoviedb.org/3/movie/{movie_id}/credits"

credits_response = requests.get(credits_url, headers=headers)
credits = credits_response.json()

cast_list = credits.get("cast", [])

print(f"Total cast members found: {len(cast_list)}")

# Safety check
if not cast_list:
    raise Exception("No actors found for this movie.")

# === PICK FIRST ACTOR ===
first_actor = cast_list[0]
person_id = first_actor["id"]

print(f"\nSelected actor:")
print(f"Person ID: {person_id}")
print(f"Name: {first_actor['name']}")
print(f"Character: {first_actor.get('character')}")

# === STEP 2: GET FULL ACTOR DETAILS ===
person_url = f"https://api.themoviedb.org/3/person/{person_id}"

person_response = requests.get(person_url, headers=headers)
person_details = person_response.json()

print("\n=== FULL ACTOR DETAILS ===")
for key, value in person_details.items():
    print(f"{key}: {value}")

# COMMAND ----------

# DBTITLE 1,Data Extraction from 2003-2010
import requests
import time
import random
import pandas as pd
import os

# -------------------------
# CONFIGURATION
# -------------------------
TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIxMmM4ZDc0ZmY2ZGNiMzI2ZmQyYmFjNDhlMjEwODM0MCIsIm5iZiI6MTc2MzgyNDg2Ni4yMTQwMDAyLCJzdWIiOiI2OTIxZDRlMmQ0NTk3MDFiZDJiNzQwMWQiLCJzY29wZXMiOlsiYXBpX3JlYWQiXSwidmVyc2lvbiI6MX0.TDvPi__fC0GSenUywCe5UOl6LU08FMncX5YIrFqeBsE"  # your TMDb bearer token
BASE_URL = "https://api.themoviedb.org/3/discover/movie"
SAVE_ROOT = "/Volumes/raw_data_files/nyc_taxi_files/abbb/movies"

START_YEAR = 2003
END_YEAR = 2025

# 2-month windows (start_date, end_date, label)
BI_MONTHS = [
    ("01-01", "02-28", "01-02"),
    ("03-01", "04-30", "03-04"),
    ("05-01", "06-30", "05-06"),
    ("07-01", "08-31", "07-08"),
    ("09-01", "10-31", "09-10"),
    ("11-01", "12-31", "11-12"),
]

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "accept": "application/json"
}


# -------------------------
# SAFE REQUEST FUNCTION
# -------------------------
def safe_request(url, params, retries=5):
    """Safely call TMDb with retry + exponential backoff + jitter."""
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=20)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"⚠️ TMDb returned status {response.status_code}. Retrying...")
        except Exception as e:
            print(f"⚠️ Error: {e}. Retrying attempt {attempt}/{retries}...")

        # backoff delay
        time.sleep(1.5 * attempt)

    raise Exception(f"❌ Failed after {retries} retries.")


# -------------------------
# EXTRACTION FUNCTION
# -------------------------
def extract_bimonth(year, start_date, end_date, label):
    print(f"\n📅 Extracting {year} ({label}) ...")

    all_results = []
    page = 1

    while True:
        params = {
            "primary_release_date.gte": f"{year}-{start_date}",
            "primary_release_date.lte": f"{year}-{end_date}",
            "sort_by": "primary_release_date.asc",
            "page": page
        }

        data = safe_request(BASE_URL, params)

        if "results" not in data or not data["results"]:
            break

        all_results.extend(data["results"])

        total_pages = data.get("total_pages", 1)
        print(f"   Page {page}/{total_pages}")

        if page >= total_pages:
            break

        # SAFE delay
        time.sleep(0.25 + random.random() * 0.15)

        page += 1

    # Convert to DataFrame
    df = pd.DataFrame(all_results)

    # Save path
    save_dir = f"{SAVE_ROOT}/year={year}"
    os.makedirs(save_dir, exist_ok=True)

    file_path = f"{save_dir}/bimonth={label}.csv"
    df.to_csv(file_path, index=False)

    print(f"   ✅ Saved {len(df)} records → {file_path}")


# -------------------------
# MAIN LOOP
# -------------------------
for year in range(START_YEAR, END_YEAR + 1):
    print(f"\n====================")
    print(f"📢 STARTING YEAR: {year}")
    print(f"====================")

    for start, end, label in BI_MONTHS:
        extract_bimonth(year, start, end, label)

print("\n🎉 DONE! All 2-month movie extracts completed successfully.")

# COMMAND ----------

# DBTITLE 1,Data Extraction from 2011-2015


# COMMAND ----------

# DBTITLE 1,Data Extraction from 2016-2020


# COMMAND ----------

# DBTITLE 1,Data Extraction from 2021-2023


# COMMAND ----------

# DBTITLE 1,Data Extraction from 2024-2026
