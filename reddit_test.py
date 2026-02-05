# reddit_test.py
# Fill in your Reddit API credentials and run:
#   python reddit_test.py

import praw

reddit = praw.Reddit(
    client_id="PASTE_CLIENT_ID_HERE",
    client_secret="PASTE_CLIENT_SECRET_HERE",
    user_agent="reddit-research-tools by u/YOUR_REDDIT_USERNAME"
)

sub = reddit.subreddit("mechanicadvice")

for post in sub.hot(limit=10):
    print(f"{post.score} | {post.title}")
