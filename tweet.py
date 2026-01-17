import csv
import os
from datetime import datetime
from dateutil import tz
import requests
from requests_oauthlib import OAuth1

CSV_PATH = "posts.csv"
JST = tz.gettz("Asia/Tokyo")

def now_jst():
    return datetime.now(tz=JST)

def parse_jst(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=JST)

def load_rows():
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def save_rows(rows, fieldnames):
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

def pick_next(rows):
    now = now_jst()
    for r in rows:
        if r["status"] == "queued" and r["post_at_jst"]:
            if parse_jst(r["post_at_jst"]) <= now:
                return r
    return None

def post_to_x(text):
    url = "https://api.twitter.com/2/tweets"
    auth = OAuth1(
        os.environ["X_API_KEY"],
        os.environ["X_API_SECRET"],
        os.environ["X_ACCESS_TOKEN"],
        os.environ["X_ACCESS_TOKEN_SECRET"],
    )
    r = requests.post(url, json={"text": text}, auth=auth, timeout=30)
    r.raise_for_status()
    return r.json()["data"]["id"]

def main():
    rows = load_rows()
    target = pick_next(rows)
    if not target:
        print("No queued posts.")
        return

    tweet_id = post_to_x(target["text"])
    now = now_jst().strftime("%Y-%m-%d %H:%M")

    for r in rows:
        if r["id"] == target["id"]:
            r["status"] = "posted"
            r["tweet_id"] = tweet_id
            r["posted_at_jst"] = now

    save_rows(rows, rows[0].keys())
    print(f"Posted: {tweet_id}")

if __name__ == "__main__":
    main()
