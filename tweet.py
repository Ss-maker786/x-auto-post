import csv
import os
import time
from datetime import datetime
from dateutil import tz

import requests
from requests_oauthlib import OAuth1

CSV_PATH = "posts.csv"
JST = tz.gettz("Asia/Tokyo")

# リトライ設定（必要なら調整）
MAX_RETRIES = 5
BASE_SLEEP_SEC = 5  # exponential backoff の基準


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


def _auth():
    # env未設定が分かりやすいように KeyError をそのまま出す（設定ミスは落とす）
    return OAuth1(
        os.environ["X_API_KEY"],
        os.environ["X_API_SECRET"],
        os.environ["X_ACCESS_TOKEN"],
        os.environ["X_ACCESS_TOKEN_SECRET"],
    )


def post_to_x(text: str):
    """
    X API に投稿。503/429/一時ネットワークはリトライ。
    成功したら tweet_id を返す。復旧しない場合は例外。
    """
    url = "https://api.twitter.com/2/tweets"
    auth = _auth()

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(url, json={"text": text}, auth=auth, timeout=30)

            # 返ってきた内容をログに残す（失敗時の原因究明が楽）
            if r.status_code >= 400:
                body = r.text[:500]  # 長すぎるとログが汚れるので上限
                print(f"HTTP {r.status_code} from X. body(head): {body}")

            # リトライ対象
            if r.status_code in (503, 502, 504, 429):
                if attempt < MAX_RETRIES:
                    # 429 は可能なら reset を読む（無ければ指数バックオフ）
                    if r.status_code == 429:
                        reset = r.headers.get("x-rate-limit-reset")
                        if reset and reset.isdigit():
                            wait = max(1, int(reset) - int(time.time()))
                        else:
                            wait = BASE_SLEEP_SEC * (2 ** (attempt - 1))
                    else:
                        wait = BASE_SLEEP_SEC * (2 ** (attempt - 1))

                    wait = min(wait, 120)  # 上限（待ちすぎ防止）
                    print(f"Retryable error {r.status_code}. retry {attempt}/{MAX_RETRIES} after {wait}s")
                    time.sleep(wait)
                    continue

            # それ以外は通常処理（ここで4xxなどは raise）
            r.raise_for_status()

            data = r.json()
            return data["data"]["id"]

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            # 一時的なネットワーク系もリトライ
            if attempt < MAX_RETRIES:
                wait = min(BASE_SLEEP_SEC * (2 ** (attempt - 1)), 120)
                print(f"Network error: {type(e).__name__}. retry {attempt}/{MAX_RETRIES} after {wait}s")
                time.sleep(wait)
                continue
            raise

    # ここには通常来ない（念のため）
    raise RuntimeError("Failed to post after retries.")


def main():
    rows = load_rows()
    target = pick_next(rows)
    if not target:
        print("No queued posts.")
        return

    tweet_id = post_to_x(target["text"])
    now = now_jst().strftime("%Y-%m-%d %H:%M")

    for r in rows:
        if r.get("id") == target.get("id"):
            r["status"] = "posted"
            r["tweet_id"] = tweet_id
            r["posted_at_jst"] = now

    save_rows(rows, rows[0].keys())
    print(f"Posted: {tweet_id}")


if __name__ == "__main__":
    main()