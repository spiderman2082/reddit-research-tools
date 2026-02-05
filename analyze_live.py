import csv, json, os, time, datetime
from pathlib import Path

import yaml
from dotenv import load_dotenv

# PRAW only needed when OFFLINE_MODE=0
try:
    import praw
except Exception:
    praw = None

ROOT = Path(__file__).resolve().parent
CFG  = ROOT / "config.yaml"
OUT  = ROOT / "output"
LOGD = ROOT / "logs"
MOCK = ROOT / "data" / "mock_posts.json"

OUT.mkdir(exist_ok=True)
LOGD.mkdir(exist_ok=True)

def log(msg: str):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOGD / "run_live.log", "a", encoding="utf-8") as f:
        f.write(line + "\n")

def load_config():
    with open(CFG, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def keyword_hits(text: str, keywords):
    t = (text or "").lower()
    return [k for k in keywords if (k or "").lower() in t]

def load_mock_posts():
    # BOM-safe on Windows
    with open(MOCK, "r", encoding="utf-8-sig") as f:
        posts = json.load(f)

    now = int(time.time())
    cleaned = []
    for i, p in enumerate(posts):
        if not isinstance(p, dict):
            continue

        created = int(p.get("created_utc") or (now - i * 6 * 3600))
        cleaned.append({
            "subreddit": p.get("subreddit", ""),
            "id": p.get("id", ""),
            "title": p.get("title", ""),
            "selftext": p.get("selftext", "") or "",
            "created_utc": created,
            "score": int(p.get("score") or 0),
            "num_comments": int(p.get("num_comments") or 0),
            "permalink": p.get("permalink") or p.get("link") or "",
            "url": p.get("url", "") or "",
        })
    return cleaned

def make_reddit():
    load_dotenv(ROOT / ".env")
    offline = os.getenv("OFFLINE_MODE", "0").strip().lower() in ("1","true","yes")

    if offline:
        log("OFFLINE_MODE=1 -> using mock_posts.json (no API calls).")
        return None

    if praw is None:
        raise RuntimeError("praw not installed, but OFFLINE_MODE is not enabled. Install deps or set OFFLINE_MODE=1.")

    client_id = os.getenv("REDDIT_CLIENT_ID", "").strip()
    client_secret = os.getenv("REDDIT_CLIENT_SECRET", "").strip()
    user_agent = os.getenv("REDDIT_USER_AGENT", "").strip()

    if not client_id or not client_secret or not user_agent:
        raise RuntimeError("Missing creds in .env. Set REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET / REDDIT_USER_AGENT (or set OFFLINE_MODE=1).")

    # Optional auth (not required for read-only public listings)
    username = os.getenv("REDDIT_USERNAME", "").strip()
    password = os.getenv("REDDIT_PASSWORD", "").strip()

    if username and password:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            username=username,
            password=password,
        )
        mode = "auth"
    else:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
        )
        mode = "app-only"

    log(f"Reddit client initialized ({mode}, read-only usage).")
    return reddit

def fetch_posts(reddit, subreddits, hours=72, per_sub_limit=250, polite_pause_s=1.0):
    all_posts = []
    cutoff = int(time.time()) - hours * 3600

    for sub in subreddits:
        log(f"Fetching r/{sub} (new) limit={per_sub_limit} window={hours}h")
        sr = reddit.subreddit(sub)

        scanned = 0
        kept = 0

        for post in sr.new(limit=per_sub_limit):
            scanned += 1
            created = int(getattr(post, "created_utc", 0))
            if created < cutoff:
                break

            all_posts.append({
                "subreddit": sub,
                "id": post.id,
                "title": post.title,
                "selftext": getattr(post, "selftext", "") or "",
                "created_utc": created,
                "score": int(getattr(post, "score", 0) or 0),
                "num_comments": int(getattr(post, "num_comments", 0) or 0),
                "permalink": "https://reddit.com" + getattr(post, "permalink", ""),
                "url": getattr(post, "url", "") or "",
            })
            kept += 1

        log(f"r/{sub}: scanned={scanned}, kept_in_window={kept}")
        time.sleep(polite_pause_s)

    return all_posts

def main():
    cfg = load_config()
    subreddits = cfg.get("subreddits") or []
    keywords = cfg.get("keywords") or []

    hours = int(cfg.get("window_hours") or 72)
    per_sub_limit = int(cfg.get("per_sub_limit") or 250)
    polite_pause_s = float(cfg.get("polite_pause_s") or 1.0)

    log(f"Loaded config: {len(subreddits)} subreddits, {len(keywords)} keywords, window={hours}h")

    reddit = make_reddit()

    if reddit is None:
        posts = load_mock_posts()
        cutoff = int(time.time()) - hours * 3600
        posts = [p for p in posts
                 if (not subreddits or p.get("subreddit") in subreddits)
                 and int(p.get("created_utc", 0)) >= cutoff]
        log(f"Loaded mock posts kept: {len(posts)}")
    else:
        posts = fetch_posts(reddit, subreddits, hours=hours, per_sub_limit=per_sub_limit, polite_pause_s=polite_pause_s)
        log(f"Total posts kept: {len(posts)}")

    enriched = []
    for p in posts:
        combined = f'{p.get("title","")} {p.get("selftext","")}'
        hits = keyword_hits(combined, keywords)
        e = dict(p)
        e["keyword_hits"] = hits
        e["keyword_hit_count"] = len(hits)
        enriched.append(e)

    # Sort demand signals
    enriched.sort(key=lambda x: (x.get("keyword_hit_count", 0), x.get("score", 0), x.get("num_comments", 0)), reverse=True)

    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = OUT / f"live_posts_{stamp}.json"
    out_csv  = OUT / f"live_posts_{stamp}.csv"
    out_summary = OUT / f"live_summary_{stamp}.txt"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)

    fieldnames = ["subreddit","id","created_utc","score","num_comments","title","permalink","url","keyword_hit_count","keyword_hits"]
    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for p in enriched:
            row = {k: p.get(k, "") for k in fieldnames}
            row["keyword_hits"] = ";".join(p.get("keyword_hits") or [])
            w.writerow(row)

    top = enriched[:15]
    lines = []
    lines.append(f"Window: last {hours} hours")
    lines.append(f"Total posts kept: {len(enriched)}")
    lines.append("")
    lines.append("Top demand signals (sorted by keyword hits, score, comments):")
    for p in top:
        dt = datetime.datetime.fromtimestamp(int(p["created_utc"]))
        lines.append(f'- [{p.get("subreddit")}] ({p.get("keyword_hit_count")} hits) score={p.get("score")} comments={p.get("num_comments")} {dt:%Y-%m-%d %H:%M}: {p.get("title")}')
        if p.get("keyword_hits"):
            lines.append(f'  hits: {", ".join(p["keyword_hits"])}')
        if p.get("permalink"):
            lines.append(f'  link: {p["permalink"]}')

    with open(out_summary, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    log(f"Wrote: {out_json.name}, {out_csv.name}, {out_summary.name}")
    log("DONE")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"ERROR: {e}")
        raise
