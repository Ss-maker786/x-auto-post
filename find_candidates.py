import csv
import os
import re
from datetime import datetime
from dateutil import tz
import requests

# =========================
# 設定（ここを触れば調整可）
# =========================

JST = tz.gettz("Asia/Tokyo")
OUT_PATH = "candidates.csv"

# FreeプランのReads制限が厳しい前提で、まずは控えめ推奨
# 候補が少なければ 30 -> 50 に上げる
MAX_RESULTS = int(os.getenv("MAX_RESULTS", "30"))

# ハッシュタグ（広めに拾う）
TAGS = [
    "#インフラエンジニア",
    "#運用",
    "#SRE",
    "#基本情報技術者",
    "#駆け出しエンジニア",
    "#AWS",
    "#Linux",
    "#クラウドエンジニア",
    "#ネットワーク",
    "#サーバー",
]

# キーワード（単語1個より「2語以上」を優先するとノイズが減る）
KEYWORDS = [
    "運用 新人",
    "インフラ 1年目",
    "障害対応",
    "監視",
    "オンコール",
    "学習記録",
    "AWS 勉強",
    "AWS ハンズオン",
    "Linux 勉強",
    "基本情報 勉強",
    "サーバー 運用",
    "CloudWatch",
    "IAM",
    "EC2",
]

# ノイズ除外（広告・副業・情報商材などを弾く）
BLOCK_WORDS = [
    "副業", "投資", "稼ぐ", "DM", "LINE", "サロン", "note", "プレゼント企画", "無料配布",
    "スクール", "案件", "フリーランス", "転職保証", "年収", "コンサル", "情報商材",
]

# =========================
# ユーティリティ
# =========================

def now_jst_date() -> str:
    return datetime.now(tz=JST).strftime("%Y-%m-%d")

def sanitize(s: str) -> str:
    """CSV崩れ防止：改行除去、タブ除去、カンマは読点に置換"""
    if s is None:
        return ""
    s = s.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    s = s.replace(",", "、")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def contains_block_words(*texts) -> bool:
    joined = " ".join([t for t in texts if t])
    return any(w in joined for w in BLOCK_WORDS)

def build_query() -> str:
    """
    1回の検索で拾う（APIコール回数を減らす）
    - リツイート除外
    - リプ除外（必要なら -is:reply を外してOK）
    - 日本語のみ
    """
    tag_part = " OR ".join([f'"{t}"' for t in TAGS])
    kw_part = " OR ".join([f'"{k}"' for k in KEYWORDS])
    q = f"({tag_part} OR {kw_part}) lang:ja -is:retweet -is:reply"
    return q

# =========================
# X API呼び出し（Recent Search）
# =========================

def fetch_candidates(bearer: str):
    url = "https://api.twitter.com/2/tweets/search/recent"
    headers = {"Authorization": f"Bearer {bearer}"}

    params = {
        "query": build_query(),
        "max_results": str(MAX_RESULTS),
        # author情報も一緒に取る
        "expansions": "author_id",
        "tweet.fields": "created_at,lang,text",
        "user.fields": "name,username,description",
    }

    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"X API error {r.status_code}: {r.text}")

    payload = r.json()
    tweets = payload.get("data", [])
    users = {u["id"]: u for u in payload.get("includes", {}).get("users", [])}

    rows = []
    seen = set()

    for tw in tweets:
        uid = tw.get("author_id")
        u = users.get(uid, {})
        username = u.get("username")
        if not username or username in seen:
            continue

        display_name = sanitize(u.get("name", ""))
        bio = sanitize(u.get("description", ""))
        last_tweet = sanitize(tw.get("text", ""))

        # ノイズ除外
        if contains_block_words(display_name, bio, last_tweet):
            continue

        seen.add(username)
        rows.append({
            "date": now_jst_date(),
            "username": sanitize(username),
            "display_name": display_name,
            "bio": bio,
            "last_tweet": last_tweet,
            "url": f"https://x.com/{sanitize(username)}",
        })

    return rows

def write_csv(rows):
    fieldnames = ["date", "username", "display_name", "bio", "last_tweet", "url"]
    with open(OUT_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

def main():
    bearer = os.environ.get("X_BEARER_TOKEN", "").strip()
    if not bearer:
        raise RuntimeError("Missing env: X_BEARER_TOKEN (GitHub Secretsに登録してね)")

    rows = fetch_candidates(bearer)
    write_csv(rows)
    print(f"Wrote {len(rows)} candidates to {OUT_PATH}")

if __name__ == "__main__":
    main()
