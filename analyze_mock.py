import json, csv, os, sys, time, datetime
from pathlib import Path

try:
    import yaml
except ImportError:
    print("PyYAML not installed. Run: python -m pip install -r requirements.txt", file=sys.stderr)
    sys.exit(1)

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "mock_posts.json"
CFG  = ROOT / "config.yaml"
OUT  = ROOT / "output"
LOGD = ROOT / "logs"

OUT.mkdir(exist_ok=True)
LOGD.mkdir(exist_ok=True)

def log(msg: str):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOGD / "run.log", "a", encoding="utf-8-sig") as f:
        f.write(line + "\n")

def load_config():
    with open(CFG, "r", encoding="utf-8-sig") as f:
        return yaml.safe_load(f)

def load_posts():
    with open(DATA, "r", encoding="utf-8-sig") as f:
        posts = json.load(f)
    # If created_utc is 0/missing, assign a recent timestamp (within last 48h) for demo purposes
    now = int(time.time())
    for i, p in enumerate(posts):
        if not p.get("created_utc"):
            # spread timestamps across last 48 hours
            p["created_utc"] = now - (i * 6 * 3600)
    return posts

def within_hours(created_utc: int, hours: int) -> bool:
    return created_utc >= int(time.time()) - hours * 3600

def keyword_hits(text: str, keywords):
    t = (text or "").lower()
    hits = []
    for k in keywords:
        if (k or "").lower() in t:
            hits.append(k)
    return hits

def main():
    cfg = load_config()
    subreddits = set((cfg.get("subreddits") or []))
    keywords = (cfg.get("keywords") or [])

    hours = 72  # Phase 1 target window
    log(f"Loaded config: {len(subreddits)} subreddits, {len(keywords)} keywords, window={hours}h")
    posts = load_posts()
    log(f"Loaded mock posts: {len(posts)}")

    # Filter by subreddit + time window
    filtered = []
    for p in posts:
        if subreddits and p.get("subreddit") not in subreddits:
            continue
        if not within_hours(int(p.get("created_utc", 0)), hours):
            continue
        filtered.append(p)

    # Tag with keyword hits
    enriched = []
    for p in filtered:
        combined = f'{p.get("title","")} {p.get("selftext","")}'
        hits = keyword_hits(combined, keywords)
        e = dict(p)
        e["keyword_hits"] = hits
        e["keyword_hit_count"] = len(hits)
        enriched.append(e)

    # Simple “demand signal”: sort by hit_count then score then comments
    enriched.sort(key=lambda x: (x.get("keyword_hit_count", 0), x.get("score", 0), x.get("num_comments", 0)), reverse=True)

    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = OUT / f"posts_{stamp}.json"
    out_csv  = OUT / f"posts_{stamp}.csv"
    out_summary = OUT / f"summary_{stamp}.txt"

    # Write JSON
    with open(out_json, "w", encoding="utf-8-sig") as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)

    # Write CSV
    fieldnames = ["subreddit","id","created_utc","score","num_comments","title","permalink","keyword_hit_count","keyword_hits"]
    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for p in enriched:
            row = {k: p.get(k, "") for k in fieldnames}
            row["keyword_hits"] = ";".join(p.get("keyword_hits") or [])
            w.writerow(row)

    # Write summary
    top = enriched[:10]
    lines = []
    lines.append(f"Window: last {hours} hours")
    lines.append(f"Total posts analyzed: {len(posts)}")
    lines.append(f"Posts in-window + configured subreddits: {len(enriched)}")
    lines.append("")
    lines.append("Top demand signals (sorted by keyword hits, score, comments):")
    for p in top:
        dt = datetime.datetime.fromtimestamp(int(p["created_utc"]))
        lines.append(f'- [{p.get("subreddit")}] ({p.get("keyword_hit_count")} hits) score={p.get("score")} comments={p.get("num_comments")} {dt:%Y-%m-%d %H:%M}: {p.get("title")}')
        if p.get("keyword_hits"):
            lines.append(f'  hits: {", ".join(p["keyword_hits"])}')
        if p.get("permalink"):
            lines.append(f'  link: {p["permalink"]}')
    with open(out_summary, "w", encoding="utf-8-sig") as f:
        f.write("\n".join(lines) + "\n")

    log(f"Wrote: {out_json.name}, {out_csv.name}, {out_summary.name}")
    log("DONE (mock pipeline)")

if __name__ == "__main__":
    main()

