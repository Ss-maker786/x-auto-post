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


def ensure_columns(rows, extra_cols):
    """
    CSVに列が存在しない場合でも、後から追跡できるように列を追加する。
    既存のCSVヘッダに影響が出るのが嫌なら extra_cols を空にしてもOK。
    """
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


def guess_slot_hour(now: datetime) -> int | None:
    """
    GitHub Actionsの起動が多少遅れても吸収できるように、時間帯でスロット判定。
    - 06:00-09:59 -> 08:00枠
    - 10:00-13:59 -> 12:00枠
    - 18:00-21:59 -> 20:00枠
    """
    h = now.hour
    if 6 <= h < 10:
        return 8
    if 10 <= h < 14:
        return 12
    if 18 <= h < 22:
        return 20
    return None


def pick_slot_post(rows, now: datetime, slot_hour: int):
    """
    当日・該当スロット時刻の投稿を最優先で拾う（これが「20時だけ出ない」を潰す本丸）
    """
    today = now.date()
    candidates = []
    for r in rows:
        if r.get("status") != "queued" or not r.get("post_at_jst"):
            continue
        t = parse_jst(r["post_at_jst"])
        if t.date() != today:
            continue
        if t.hour != slot_hour:
            continue
        if t <= now:
            candidates.append((t, r))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])  # 同枠に複数あっても最も古いもの
    return candidates[0][1]


def pick_oldest_overdue(rows, now: datetime):
    """
    スロット投稿が無い場合は、期限切れの最古を拾って空振りを減らす（バックログ掃除）
    """
    candidates = []
    for r in rows:
        if r.get("status") != "queued" or not r.get("post_at_jst"):
            continue
        t = parse_jst(r["post_at_jst"])
        if t <= now:
            candidates.append((t, r))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


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

    raise RuntimeError(f"Failed to post after retries. last_err={last_err!r}")


def main():
    now = now_jst()
    rows = load_rows()

    if not rows:
        print("[info] posts.csv is empty.")
        return

    # 失敗しても通知不要（緑運用）でも、後から追える列は持っておくのがおすすめ
    rows, fieldnames = ensure_columns(
        rows,
        extra_cols=["last_error", "last_attempt_at_jst"],
    )
    if not fieldnames:
        fieldnames = list(rows[0].keys())

    slot_hour = guess_slot_hour(now)
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
        return  # exit 0 (green)

    # 念のためログ
    text = target.get("text", "")
    print(f"[info] target id={target.get('id')} len={len(text)} post_at={target.get('post_at_jst')} now={now.strftime('%Y-%m-%d %H:%M')}")

    try:
        tweet_id = post_to_x(text)
    except Exception as e:
        # 失敗しても通知（赤）にしない：緑のまま。ただしCSVに記録は残す。
        err = f"{type(e).__name__}: {e}"
        now_s = now.strftime("%Y-%m-%d %H:%M")

        for r in rows:
            if r.get("id") == target.get("id"):
                r["last_error"] = err[:500]
                r["last_attempt_at_jst"] = now_s
                break

        save_rows(rows, fieldnames)
        print(f"[warn] Post failed; keep queued. reason={err}")
        return  # exit 0 (green)

    # 成功したら posted に更新
    now_s = now.strftime("%Y-%m-%d %H:%M")
    for r in rows:
        if r.get("id") == target.get("id"):
            r["status"] = "posted"
            r["tweet_id"] = tweet_id
            r["posted_at_jst"] = now_s
            r["last_error"] = ""
            r["last_attempt_at_jst"] = now_s
            break

    save_rows(rows, fieldnames)
    print(f"[info] Posted: {tweet_id}")


if __name__ == "__main__":
    main()
