import csv
import os
import time
from datetime import datetime
from dateutil import tz

import requests
from requests_oauthlib import OAuth1

CSV_PATH = "posts.csv"
JST = tz.gettz("Asia/Tokyo")

# Retry settings
MAX_RETRIES = 5
BASE_SLEEP_SEC = 5
MAX_SLEEP_SEC = 120

RETRY_STATUS = {502, 503, 504, 429}


def now_jst():
    return datetime.now(tz=JST)


def parse_jst(s: str):
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
        if r.get("status") == "queued" and r.get("post_at_jst"):
            if parse_jst(r["post_at_jst"]) <= now:
                return r
    return None


def make_auth():
    return OAuth1(
        os.environ["X_API_KEY"],
        os.environ["X_API_SECRET"],
        os.environ["X_ACCESS_TOKEN"],
        os.environ["X_ACCESS_TOKEN_SECRET"],
    )


def calc_wait_seconds(resp: requests.Response | None, attempt: int) -> int:
    # Exponential backoff, capped
    wait = BASE_SLEEP_SEC * (2 ** (attempt - 1))

    # If rate-limited, respect reset header if present
    if resp is not None and resp.status_code == 429:
        reset = resp.headers.get("x-rate-limit-reset")
        if reset and reset.isdigit():
            wait = max(1, int(reset) - int(time.time()))

    return min(wait, MAX_SLEEP_SEC)


def post_to_x(text: str) -> str:
    url = "https://api.twitter.com/2/tweets"
    auth = make_auth()

    last_err = None

    for attempt in range(1, MAX_RETRIES + 1):
        resp = None
        try:
            resp = requests.post(url, json={"text": text}, auth=auth, timeout=30)

            if resp.status_code in RETRY_STATUS:
                body_head = (resp.text or "")[:500]
                print(f"[warn] Retryable HTTP {resp.status_code}. body(head)={body_head}")

                if attempt < MAX_RETRIES:
                    wait = calc_wait_seconds(resp, attempt)
                    print(f"[info] retry {attempt}/{MAX_RETRIES} after {wait}s")
                    time.sleep(wait)
                    continue

            # Non-retryable or retries exhausted -> raise if error
            if resp.status_code >= 400:
                body_head = (resp.text or "")[:500]
                print(f"[error] HTTP {resp.status_code}. body(head)={body_head}")
            resp.raise_for_status()

            data = resp.json()
            return data["data"]["id"]

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_err = e
            print(f"[warn] Network error: {type(e).__name__}: {e}")

            if attempt < MAX_RETRIES:
                wait = min(BASE_SLEEP_SEC * (2 ** (attempt - 1)), MAX_SLEEP_SEC)
                print(f"[info] retry {attempt}/{MAX_RETRIES} after {wait}s")
                time.sleep(wait)
                continue
            raise

        except Exception as e:
            last_err = e
            raise

    # Should not reach here
    raise RuntimeError(f"Failed to post after retries. last_err={last_err!r}")


def main():
    rows = load_rows()
    target = pick_next(rows)

    if not target:
        print("[info] No queued posts.")
        return  # exit 0

    try:
        tweet_id = post_to_x(target["text"])
    except Exception as e:
        # A案：失敗でも緑にする（queuedのまま残す）
        print(f"[warn] Post failed; keep queued. reason={type(e).__name__}: {e}")
        return  # exit 0

    now = now_jst().strftime("%Y-%m-%d %H:%M")

    for r in rows:
        if r.get("id") == target.get("id"):
            r["status"] = "posted"
            r["tweet_id"] = tweet_id
            r["posted_at_jst"] = now
            break

    save_rows(rows, rows[0].keys())
    print(f"[info] Posted: {tweet_id}")


if __name__ == "__main__":
    main()