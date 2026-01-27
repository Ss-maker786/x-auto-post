import csv
import os
import time
from datetime import datetime
from dateutil import tz

import requests
from requests_oauthlib import OAuth1

CSV_PATH = "posts.csv"
JST = tz.gettz("Asia/Tokyo")

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


def ensure_columns(rows, extra_cols):
    if not rows:
        return rows, []
    fieldnames = list(rows[0].keys())
    for c in extra_cols:
        if c not in fieldnames:
            fieldnames.append(c)
            for r in rows:
                r[c] = ""
    return rows, fieldnames


def save_rows(rows, fieldnames):
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def make_auth():
    return OAuth1(
        os.environ["X_API_KEY"],
        os.environ["X_API_SECRET"],
        os.environ["X_ACCESS_TOKEN"],
        os.environ["X_ACCESS_TOKEN_SECRET"],
    )


def calc_wait_seconds(resp: requests.Response | None, attempt: int) -> int:
    wait = BASE_SLEEP_SEC * (2 ** (attempt - 1))
    if resp is not None and resp.status_code == 429:
        reset = resp.headers.get("x-rate-limit-reset")
        if reset and reset.isdigit():
            wait = max(1, int(reset) - int(time.time()))
    return min(wait, MAX_SLEEP_SEC)


def post_to_x(text: str, reply_to_tweet_id: str | None = None) -> str:
    url = "https://api.twitter.com/2/tweets"
    auth = make_auth()

    payload = {"text": text}
    if reply_to_tweet_id:
        payload["reply"] = {"in_reply_to_tweet_id": reply_to_tweet_id}

    for attempt in range(1, MAX_RETRIES + 1):
        resp = None
        try:
            resp = requests.post(url, json=payload, auth=auth, timeout=30)

            if resp.status_code in RETRY_STATUS:
                body_head = (resp.text or "")[:500]
                print(f"[warn] Retryable HTTP {resp.status_code}. body(head)={body_head}")
                if attempt < MAX_RETRIES:
                    wait = calc_wait_seconds(resp, attempt)
                    print(f"[info] retry {attempt}/{MAX_RETRIES} after {wait}s")
                    time.sleep(wait)
                    continue

            if resp.status_code >= 400:
                body_head = (resp.text or "")[:500]
                print(f"[error] HTTP {resp.status_code}. body(head)={body_head}")
            resp.raise_for_status()

            data = resp.json()
            return data["data"]["id"]

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            print(f"[warn] Network error: {type(e).__name__}: {e}")
            if attempt < MAX_RETRIES:
                wait = min(BASE_SLEEP_SEC * (2 ** (attempt - 1)), MAX_SLEEP_SEC)
                print(f"[info] retry {attempt}/{MAX_RETRIES} after {wait}s")
                time.sleep(wait)
                continue
            raise


def guess_slot_hour(now: datetime) -> int | None:
    h = now.hour
    if 6 <= h < 10:
        return 8
    if 10 <= h < 14:
        return 12
    if 14 <= h < 18:
        return 16
    if 18 <= h < 22:
        return 20
    return None


def pick_slot_post(rows, now: datetime, slot_hour: int):
    today = now.date()
    candidates = []
    for r in rows:
        if r.get("status") != "queued" or not r.get("post_at_jst"):
            continue
        try:
            t = parse_jst(r["post_at_jst"])
        except Exception:
            continue
        if t.date() == today and t.hour == slot_hour and t <= now:
            candidates.append((t, r))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def pick_oldest_overdue(rows, now: datetime):
    candidates = []
    for r in rows:
        if r.get("status") != "queued" or not r.get("post_at_jst"):
            continue
        try:
            t = parse_jst(r["post_at_jst"])
        except Exception:
            continue
        if t <= now:
            candidates.append((t, r))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def main():
    now = now_jst()
    rows = load_rows()
    if not rows:
        print("[info] posts.csv is empty.")
        return

    rows, fieldnames = ensure_columns(
        rows,
        extra_cols=[
            "reply_text",
            "reply_tweet_id",
            "reply_posted_at_jst",
            "last_error",
            "last_attempt_at_jst",
        ],
    )

    slot_hour = guess_slot_hour(now)
    target = None

    if slot_hour:
        target = pick_slot_post(rows, now, slot_hour)
        if target:
            print(f"[info] slot={slot_hour} picked id={target.get('id')} post_at={target.get('post_at_jst')}")
        else:
            print(f"[info] slot={slot_hour} no slot-post found; fallback to backlog.")
            target = pick_oldest_overdue(rows, now)
    else:
        print("[info] not in slot window; fallback to backlog.")
        target = pick_oldest_overdue(rows, now)

    if not target:
        print("[info] No queued posts to send.")
        return  # green

    now_s = now.strftime("%Y-%m-%d %H:%M")
    text = target.get("text", "")

    # 失敗しても緑運用。ただしCSVに痕跡を残す。
    try:
        tweet_id = post_to_x(text)
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        for r in rows:
            if r.get("id") == target.get("id"):
                r["last_error"] = err[:500]
                r["last_attempt_at_jst"] = now_s
                break
        save_rows(rows, fieldnames)
        print(f"[warn] Post failed; keep queued. reason={err}")
        return  # green

    # 成功 → posted 更新
    for r in rows:
        if r.get("id") == target.get("id"):
            r["status"] = "posted"
            r["tweet_id"] = tweet_id
            r["posted_at_jst"] = now_s
            r["last_error"] = ""
            r["last_attempt_at_jst"] = now_s
            break

    # 返信（クイズの答え等）も自動投稿したい場合
    reply_text = (target.get("reply_text") or "").strip()
    if reply_text:
        try:
            # 少し間を置く（任意）
            time.sleep(2)
            reply_id = post_to_x(reply_text, reply_to_tweet_id=tweet_id)
            for r in rows:
                if r.get("id") == target.get("id"):
                    r["reply_tweet_id"] = reply_id
                    r["reply_posted_at_jst"] = now_s
                    break
            print(f"[info] Replied: {reply_id}")
        except Exception as e:
            # 親ツイが成功してるなら、返信失敗は緑で握りつぶす（運用方針どおり）
            err = f"reply {type(e).__name__}: {e}"
            for r in rows:
                if r.get("id") == target.get("id"):
                    r["last_error"] = err[:500]
                    r["last_attempt_at_jst"] = now_s
                    break
            print(f"[warn] Reply failed; keep posted. reason={err}")

    save_rows(rows, fieldnames)
    print(f"[info] Posted: {tweet_id}")


if __name__ == "__main__":
    main()
