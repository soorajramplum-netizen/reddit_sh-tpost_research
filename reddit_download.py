"""Reddit downloader for the tension layer, via the Arctic Shift archive.

No Reddit API approval needed. Reads subreddits.csv (cohort map), pulls posts
(and comments where comments=yes) oldest-first, and appends them to
data/<subreddit>/posts.csv and comments.csv. Checkpoints after every page in
data/state.json, so it can be stopped any time and re-run to resume. Re-running
later also tops up new posts since the last run.

    python reddit_download.py                     # priority 1 subs, since 2023-01-01
    python reddit_download.py --priority 2        # priority 1 and 2
    python reddit_download.py --subs IndianHaircare TwoXIndia
    python reddit_download.py --since 2021-01-01 --no-comments

To stop cleanly: create a file named STOP in this folder.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import csv
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

API = "https://arctic-shift.photon-reddit.com/api"
HERE = Path(__file__).resolve().parent
# Where downloads go. Put a folder path in data_dir.txt to keep the (large) data outside a
# synced folder such as OneDrive; otherwise it goes to ./data next to this script.
_dir_file = HERE / "data_dir.txt"
DATA = Path(_dir_file.read_text(encoding="utf-8").strip()) if _dir_file.exists() else HERE / "data"
STATE = DATA / "state.json"
STOP = HERE / "STOP"

POST_FIELDS = ["id", "created_utc", "author", "title", "selftext", "score",
               "num_comments", "link_flair_text", "over_18"]
COMMENT_FIELDS = ["id", "created_utc", "author", "body", "score", "link_id", "parent_id"]
SKIP_TEXT = {"[deleted]", "[removed]"}
SKIP_AUTHORS = {"AutoModerator"}
LOCK = threading.Lock()  # one state file shared by parallel workers


def get(endpoint, params, tries=10, label=""):
    """422s and timeouts mean the archive is busy and clear up on their own, so wait and retry
    (5s, 10s, 20s … capped at 2 min; about 12 minutes in total before giving up)."""
    url = f"{API}/{endpoint}?{urllib.parse.urlencode(params)}"
    for i in range(tries):
        if STOP.exists():
            raise StopRequested()
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "RevProject-tension-research/1.0"})
            with urllib.request.urlopen(req, timeout=90) as r:
                return json.load(r).get("data") or []
        except urllib.error.HTTPError as e:
            if e.code == 400:  # a bad request never succeeds; show the API's reason
                raise RuntimeError(f"API rejected request: {e.read().decode(errors='ignore')}")
            err = e
        except Exception as e:  # network blips, timeouts, laptop waking from sleep
            err = e
        wait = min(5 * 2 ** i, 120)
        print(f"    {label} busy, retry {i + 1}/{tries} in {wait}s ({err})")
        time.sleep(wait)
    raise RuntimeError(f"archive kept refusing {url}")


class StopRequested(Exception):
    pass


def single_instance():
    """Refuse to start if another copy is already running: two copies would overwrite each
    other's progress. The OS releases the lock automatically if the window is closed."""
    DATA.mkdir(exist_ok=True)
    fh = open(DATA / ".running.lock", "a+")
    try:
        if os.name == "nt":
            import msvcrt
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        sys.exit("The downloader is already running in another window. Use that one, or stop it first.")
    return fh  # keep the handle open for the whole run


def load_state():
    return json.loads(STATE.read_text()) if STATE.exists() else {}


PROGRESS_LOG = DATA / "progress_log.csv"
_last_log = [0.0]


def save_state(state):
    with LOCK:
        tmp = STATE.with_suffix(".tmp")
        with open(tmp, "w") as fh:   # forced to disk before the swap, so even a power cut can't leave state.json empty
            fh.write(json.dumps(state, indent=1))
            fh.flush()
            os.fsync(fh.fileno())
        tmp.replace(STATE)
        # Every 10 minutes, note how much is done, so --status can measure speed and estimate time left
        if time.time() - _last_log[0] >= 600:
            _last_log[0] = time.time()
            posts = sum(v.get("rows", 0) for k, v in state.items() if k.endswith(":posts") and isinstance(v, dict))
            threads = sum(v.get("done", 0) for k, v in state.items() if k.endswith(":rich") and isinstance(v, dict))
            new = not PROGRESS_LOG.exists()
            with PROGRESS_LOG.open("a", encoding="utf-8") as f:
                if new:
                    f.write("ts,posts,rich_threads\n")
                f.write(f"{int(time.time())},{posts},{threads}\n")


def clean(s):
    return (s or "").replace("\r", " ").replace("\x00", "").strip()


def count_rows(path):
    """Rows in a CSV, read properly so text with line breaks counts once."""
    if not path.exists():
        return 0
    with path.open(encoding="utf-8", newline="") as f:
        return max(0, sum(1 for _ in csv.reader(f)) - 1)


def coverage(key, out, state):
    """State per sub+kind: {"from": oldest ts covered, "to": newest ts covered, "back": backfill
    position or None}. Upgrades the old single-number state by reading the CSV's oldest row."""
    cov = state.get(key)
    if isinstance(cov, int):
        with out.open(encoding="utf-8") as f:
            oldest = min((int(r["created_utc"]) for r in csv.DictReader(f)), default=cov)
        cov = {"from": oldest, "to": cov, "back": None}
        with LOCK:
            state[key] = cov
    return cov


def pull(sub, kind, since_ts, state, row_meta):
    """Fill one subreddit's posts or comments, oldest first, appending to CSV.
    If --since is earlier than what is already on disk, the gap is backfilled first,
    then new items after the newest one are topped up."""
    fields = POST_FIELDS if kind == "posts" else COMMENT_FIELDS
    key = f"{sub}:{kind}"
    out = DATA / sub / f"{kind}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    new_file = not out.exists() or out.stat().st_size == 0
    cov = None if new_file else coverage(key, out, state)
    if cov is None:
        with LOCK:
            cov = state[key] = {"from": since_ts, "to": since_ts, "back": None}
    # No duplicates after a crash: "bytes" is the file size at the last saved checkpoint.
    # Anything beyond it was written but never confirmed, so it is cut off and re-fetched.
    if not new_file and cov.get("bytes") and out.stat().st_size > cov["bytes"]:
        extra = out.stat().st_size - cov["bytes"]
        with out.open("r+b") as fb:
            fb.truncate(cov["bytes"])
        print(f"  {sub} {kind}: removed {extra} bytes of an unconfirmed batch; re-fetching it")
    if "rows" not in cov:  # older state files: count once, then keep a running total
        cov["rows"] = count_rows(out)
    ranges = []
    if since_ts < cov["from"]:
        ranges.append(("back", max(cov["back"] or since_ts, since_ts), cov["from"]))
    ranges.append(("to", cov["to"], None))
    total = 0
    with out.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["subreddit", "cohort", "sub_cohort", "date", "url"] + fields)
            f.flush()
            cov["bytes"] = f.tell()
            save_state(state)
        for mode, after, before in ranges:
            if mode == "back":
                print(f"  {sub} {kind}: backfilling from {datetime.fromtimestamp(after, timezone.utc):%Y-%m-%d}")
            ok, n = page(sub, kind, fields, after, before, w, f, state, key, mode, row_meta)
            total += n
            if not ok:
                return False
            if mode == "back":
                cov.update({"from": since_ts, "back": None, "back_edge": []})
                save_state(state)
    cov["checked"] = int(time.time())  # caught up to today at this moment
    save_state(state)
    print(f"  {sub} {kind}: done, {total} new rows")
    return True


def page(sub, kind, fields, after, before, w, f, state, key, mode, row_meta):
    # Items sharing a timestamp can straddle two pages, so each page re-asks from one second
    # before the last item and skips ids already saved at that edge. The edge ids are kept
    # in state.json, so this also holds across stop/start.
    cov = state[key]
    seen_ids = set(cov.get(f"{mode}_edge", []))
    total = 0
    while True:
        if STOP.exists():
            print(f"  {sub} {kind}: STOP file found, exiting after checkpoint.")
            return False, total
        params = {"subreddit": sub, "after": after, "sort": "asc",
                  "limit": "auto", "fields": ",".join(fields)}
        if before:
            params["before"] = before
        rows = get(f"{kind}/search", params, label=f"{sub} {kind}")
        if not rows:
            break
        fresh = batch = 0
        for r in rows:
            if r["id"] in seen_ids:
                continue
            seen_ids.add(r["id"])
            fresh += 1
            text = r.get("body") if kind == "comments" else f"{r.get('title', '')} {r.get('selftext', '')}"
            if r.get("author") in SKIP_AUTHORS or clean(text) in SKIP_TEXT:
                continue
            if kind == "posts" and clean(r.get("selftext")) in SKIP_TEXT:
                r["selftext"] = ""
            date = datetime.fromtimestamp(int(r["created_utc"]), timezone.utc).strftime("%Y-%m-%d")
            url = (f"https://reddit.com/r/{sub}/comments/{r['id']}" if kind == "posts" else
                   f"https://reddit.com/r/{sub}/comments/{r['link_id'][3:]}/_/{r['id']}")
            w.writerow([sub, *row_meta, date, url] + [clean(str(r.get(c, ""))) for c in fields])
            total += 1
            batch += 1
        last = int(rows[-1]["created_utc"])
        nxt = last - 1
        if nxt <= after and not fresh:  # page held nothing new: step past it
            nxt = last
        if nxt <= after and not fresh:
            break
        after = max(nxt, after)
        edge = [r["id"] for r in rows if int(r["created_utc"]) >= after]
        seen_ids = set(edge)
        f.flush()
        os.fsync(f.fileno())
        with LOCK:
            cov[mode] = after
            cov[f"{mode}_edge"] = edge
            cov["bytes"] = f.tell()
            cov["rows"] = cov.get("rows", 0) + batch
        save_state(state)
        print(f"  {sub} {kind}: +{len(rows)} (to {datetime.fromtimestamp(last, timezone.utc):%Y-%m-%d}) total {total}")
        time.sleep(1)  # be polite to a free service
    return True, total


# ---------- rich-thread comments (default since Oct 2026) ----------
# Downloading every comment is slow (a big beauty subreddit has ~800k). Tensions live in a few percent
# of posts, so after posts are in, comments are fetched only for "rich" posts: long ones with a
# contrast and something personal, or very busy threads with something personal (~4% of posts).
RICH_CONTRAST = re.compile(r"\bbut\b|\beven though\b|\balthough\b|\bstill\b|\bdespite\b|\bphir bhi\b|\blekin\b", re.I)
RICH_PERSONAL = re.compile(r"\bi feel\b|\bi felt\b|\bmy (?:mom|mother|dad|parents|family|husband|wife|partner|bf|gf|skin|hair|face|body|weight)\b"
                           r"|\bembarrass|\binsecur|\bconfiden|\bscared\b|\bafraid\b|\banxi|\bstress|\bashamed\b|\bjudg"
                           r"|\bpeople (?:say|think|keep)\b|\brelatives\b|\bmarriage\b|\bwedding\b|\bsalary\b|\bafford", re.I)


# Cap on rich threads per subreddit (25 Oct 2026): keep the busiest threads (most comments), skip the long tail.
# Threads already fetched count towards the cap.
RICH_CAP_DEFAULT = 3000
RICH_CAP = {"personalfinanceindia": 1500, "CarsIndia": 1500, "IndianBikes": 1500, "indianrelationships": 1500}


def rich_post_ids(sub, state):
    """Rich posts not yet fetched and not already covered by a full comment download, busiest first, up to the cap."""
    path = DATA / sub / "posts.csv"
    if not path.exists():
        return []
    cov = state.get(f"{sub}:comments") if isinstance(state.get(f"{sub}:comments"), dict) else None
    done_file = DATA / sub / "comments_rich_done.txt"
    done = set(done_file.read_text(encoding="utf-8").split()) if done_file.exists() else set()
    out = []
    with path.open(encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            text = f"{r.get('title', '')} {r.get('selftext', '')}"
            try:
                n = int(float(r.get("num_comments") or 0))
            except ValueError:
                n = 0
            rich = (len(text) >= 400 and RICH_CONTRAST.search(text) and RICH_PERSONAL.search(text)) or \
                   (n >= 40 and RICH_PERSONAL.search(text))
            if not rich or r["id"] in done:
                continue
            ts = int(r["created_utc"])
            if cov and cov["from"] <= ts and ts + 14 * 86400 <= cov["to"]:
                continue   # its comments are already in comments.csv from the full download
            out.append((n, r["id"]))
    room = max(0, RICH_CAP.get(sub, RICH_CAP_DEFAULT) - len(done))
    return [i for _, i in sorted(out, reverse=True)[:room]]


def rich_comments(sub, meta, state):
    """Fetch all comments of each rich post into comments_rich.csv; checkpoint per post."""
    ids = rich_post_ids(sub, state)
    key = f"{sub}:rich"
    with LOCK:
        st = state.setdefault(key, {"done": 0, "rows": 0})
        st["pending"] = len(ids)
    save_state(state)
    if not ids:
        print(f"  {sub} rich threads: nothing to fetch")
        return True
    print(f"  {sub} rich threads: {len(ids)} to fetch")
    out = DATA / sub / "comments_rich.csv"
    # Crash safety, as for posts: "bytes" is the file size after the last fully saved thread.
    # Anything beyond it is a half-written thread from an interrupted run; cut it off and refetch.
    if out.exists() and st.get("bytes") and out.stat().st_size > st["bytes"]:
        with out.open("r+b") as fb:
            fb.truncate(st["bytes"])
        print(f"  {sub} rich threads: removed a half-saved thread from the last run; refetching it")
    new_file = not out.exists() or out.stat().st_size == 0
    done_file = DATA / sub / "comments_rich_done.txt"
    # Exactly once: if the last run stopped after saving a thread's progress but before marking it done,
    # mark it now, so that thread is not fetched (and written) a second time.
    last = st.get("last_pid")
    if last and (not done_file.exists() or last not in done_file.read_text(encoding="utf-8").split()):
        with done_file.open("a", encoding="utf-8") as df:
            df.write(last + "\n")
        ids = [i for i in ids if i != last]
        print(f"  {sub} rich threads: marked {last} done (saved just before the last stop)")
    with out.open("a", newline="", encoding="utf-8") as f, done_file.open("a", encoding="utf-8") as df:
        w = csv.writer(f)
        if new_file:
            w.writerow(["subreddit", "cohort", "sub_cohort", "date", "url"] + COMMENT_FIELDS)
            f.flush()
            st["bytes"] = f.tell()
            save_state(state)
        for n, pid in enumerate(ids, 1):
            if STOP.exists():
                print(f"  {sub} rich threads: STOP file found, exiting after checkpoint.")
                save_state(state)
                return False
            after, seen, rows_written = None, set(), 0
            while True:   # a busy thread can exceed one page
                params = {"link_id": pid, "sort": "asc", "limit": "auto", "fields": ",".join(COMMENT_FIELDS)}
                if after:
                    params["after"] = after
                rows = get("comments/search", params, label=f"{sub} thread")
                fresh = [r for r in rows if r["id"] not in seen]
                if not fresh:
                    break
                for r in fresh:
                    seen.add(r["id"])
                    if r.get("author") in SKIP_AUTHORS or clean(r.get("body")) in SKIP_TEXT:
                        continue
                    date = datetime.fromtimestamp(int(r["created_utc"]), timezone.utc).strftime("%Y-%m-%d")
                    w.writerow([sub, *meta, date, f"https://reddit.com/r/{sub}/comments/{pid}/_/{r['id']}"]
                               + [clean(str(r.get(c, ""))) for c in COMMENT_FIELDS])
                    rows_written += 1
                after = max(int(r["created_utc"]) for r in fresh) - 1
                time.sleep(0.5)
            f.flush()
            os.fsync(f.fileno())
            with LOCK:   # order matters: rows on disk -> progress (bytes) saved -> thread marked done
                st["done"] += 1
                st["rows"] += rows_written
                st["pending"] = len(ids) - n
                st["bytes"] = f.tell()
                st["last_pid"] = pid   # lets the next run finish marking this thread if we stop right here
            save_state(state)
            df.write(pid + "\n")
            df.flush()
            os.fsync(df.fileno())
            if n % 25 == 0 or n == len(ids):
                print(f"  {sub} rich threads: {n}/{len(ids)} done, {st['rows']:,} comments so far")
    return True


def status(since_ts):
    """Progress per subreddit, read from state.json and the CSV sizes. Safe while a run is going."""
    state = load_state()
    now = int(time.time())
    fmt = lambda t: datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d") if t else "-"
    with (HERE / "subreddits.csv").open(encoding="utf-8") as f:
        subs = list(csv.DictReader(f))
    print(f"{'subreddit':24} {'pri':>3}  {'posts':38} {'comments':38}")
    totals = {"posts": 0, "comments": 0}
    started = done = 0
    total_mb = 0.0
    state.pop("_since", None)
    for s in sorted(subs, key=lambda r: (int(r["priority"]), r["subreddit"].lower())):
        cells = []
        sub_rows = 0
        for kind in ("posts", "comments"):
            cov = state.get(f"{s['subreddit']}:{kind}")
            if kind == "comments" and s["comments"] != "yes":
                cells.append("(not wanted)")
                continue
            if not cov or isinstance(cov, int):
                cells.append("not started")
                continue
            # Share of the time from --since to today that is on disk. A subreddit first pulled
            # from a later date still has an older gap to backfill, which counts as missing.
            start = min(cov["from"], since_ts)
            covered = cov["to"] - cov["from"]
            if cov["from"] > start:
                covered += (cov.get("back") or start) - start
            pct = 100 * covered / max(1, now - start)
            path = DATA / s["subreddit"] / f"{kind}.csv"
            mb = path.stat().st_size / 1e6 if path.exists() else 0
            total_mb += mb
            caught_up = cov.get("checked", 0) >= cov["to"] and now - cov["to"] < 3 * 86400
            rows = cov["rows"] if "rows" in cov else count_rows(path)
            totals[kind] += rows
            sub_rows += rows
            cells.append(f"{'DONE' if caught_up else f'{min(pct, 99.9):4.1f}%':>5} {rows:>9,} to {fmt(cov['to'])} {mb:6.1f}MB")
        if sub_rows:
            started += 1
            done += all(c.startswith("DONE") or c == "(not wanted)" for c in cells)
            print(f"{s['subreddit']:24} {s['priority']:>3}  {cells[0]:38} {cells[1]:38}")
    rich = [(k.split(":")[0], v) for k, v in state.items() if k.endswith(":rich") and isinstance(v, dict)]
    if rich:
        print("\nRich-thread comments (comments fetched only for rich posts):")
        for sub, v in sorted(rich):
            left = v.get("pending", 0)
            print(f"  {sub:24} threads done {v['done']:>6,}  still to fetch {left:>6,}  comments {v['rows']:>9,}"
                  f"{'  DONE' if left == 0 else ''}")
        totals["comments"] += sum(v["rows"] for _, v in rich)
    print()
    print(f"Total: {totals['posts']:,} posts and {totals['comments']:,} comments.")
    print(f"{started} of {len(subs)} subreddits started, {done} fully done, {total_mb:,.0f} MB on disk.")
    print("Not started yet: priority " + ", ".join(sorted({s['priority'] for s in subs
          if not (state.get(f"{s['subreddit']}:posts") or {}).get("rows")})) if started < len(subs)
          else "Everything listed has been started.")
    overall(state, subs, since_ts)


def overall(state, subs, since_ts):
    """Whole-job % complete and time left, all 103 subreddits: posts first, then rich threads.
    Totals are estimates: a subreddit's final size is projected from how far through its date range it is."""
    now = int(time.time())
    done_posts, est = 0, []
    for s in subs:
        cov = state.get(f"{s['subreddit']}:posts")
        if not isinstance(cov, dict) or not cov.get("rows"):
            est.append(None)
            continue
        start = min(cov["from"], since_ts)
        covered = cov["to"] - cov["from"] + ((cov.get("back") or start) - start if cov["from"] > start else 0)
        frac = 1.0 if (cov.get("checked", 0) >= cov["to"] and now - cov["to"] < 3 * 86400) else covered / max(1, now - start)
        done_posts += cov["rows"]
        est.append(cov["rows"] / frac if frac >= 0.05 else None)
    known = sorted(e for e in est if e)
    median = known[len(known) // 2] if known else 20000
    total_posts = sum(e if e else median for e in est)
    total_posts = max(total_posts, done_posts)

    # Rich threads are capped per subreddit (RICH_CAP) and only fetched for subs not marked "never", so the
    # total is what's done plus what's still pending under the cap (2 Nov 2026: the old posts x 4% estimate
    # and stale "pending" counts on "never" subs made a finished job look stalled).
    eligible = {s["subreddit"] for s in subs if s.get("comments") != "never"}
    rich = {k.split(":")[0]: v for k, v in state.items() if k.endswith(":rich") and isinstance(v, dict)}
    t_done = sum(v.get("done", 0) for v in rich.values())
    total_threads = t_done + sum(v.get("pending", 0) for s, v in rich.items() if s in eligible)

    # Speed from the progress log (last 6 hours of running)
    posts_rate = thread_rate = None
    if PROGRESS_LOG.exists():
        rows = [list(map(int, l.split(","))) for l in PROGRESS_LOG.read_text(encoding="utf-8").splitlines()[1:] if l]
        rows = [r for r in rows if r[0] >= now - 6 * 3600]
        if len(rows) >= 2 and rows[-1][0] - rows[0][0] >= 1200:
            hrs = (rows[-1][0] - rows[0][0]) / 3600
            posts_rate = (rows[-1][1] - rows[0][1]) / hrs or None
            thread_rate = (rows[-1][2] - rows[0][2]) / hrs or None
    thread_rate = thread_rate or 2500     # typical with 3 workers when the archive isn't busy; refined once measured
    rem_posts, rem_threads = total_posts - done_posts, total_threads - t_done
    print("\nOVERALL (all 103 subreddits; totals are estimates)")
    print(f"  Posts:        {done_posts:,} of ~{int(total_posts):,}  ({100 * done_posts / total_posts:.0f}%)")
    print(f"  Rich threads: {t_done:,} of ~{total_threads:,}  ({100 * t_done / max(1, total_threads):.0f}%)")
    if posts_rate:
        h_done = done_posts / posts_rate + t_done / thread_rate
        h_left = rem_posts / posts_rate + rem_threads / thread_rate
        print(f"  Whole job:    {100 * h_done / (h_done + h_left):.0f}% complete")
        print(f"  Time left:    ~{h_left:.0f} hours of the downloader running "
              f"(about {h_left / 8:.1f} working days at 8 hours a day), at {posts_rate:,.0f} posts/hour"
              f" and {thread_rate:,.0f} threads/hour")
    else:
        pct = 100 * (done_posts + t_done * 10) / (total_posts + total_threads * 10)   # a thread costs ~10 posts of time
        print(f"  Whole job:    ~{pct:.0f}% complete (rough)")
        print("  Time left:    shown once the downloader has run ~20 minutes with this version (it logs its speed)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--priority", type=int, default=1, help="include subs with priority <= this")
    ap.add_argument("--subs", nargs="*", help="only these subreddits (overrides --priority)")
    ap.add_argument("--since", default="2023-01-01", help="start date for a fresh subreddit")
    ap.add_argument("--comments", choices=["rich", "all", "none"], default="rich",
                    help="rich (default): posts for every subreddit first, then comments only for rich posts; "
                         "all: every comment (slow); none: posts only")
    ap.add_argument("--no-comments", action="store_true", help="same as --comments none")
    ap.add_argument("--workers", type=int, default=3, help="subreddits downloaded at the same time")
    ap.add_argument("--status", action="store_true", help="show progress and exit")
    a = ap.parse_args()

    since_ts = int(datetime.strptime(a.since, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    if a.status:
        return status(load_state().get("_since", since_ts))
    with (HERE / "subreddits.csv").open(encoding="utf-8") as f:
        subs = list(csv.DictReader(f))
    if a.subs:
        wanted = {s.lower() for s in a.subs}
        subs = [s for s in subs if s["subreddit"].lower() in wanted]
    else:
        subs = [s for s in subs if int(s["priority"]) <= a.priority]
    if not subs:
        sys.exit("No subreddits matched. Add them to subreddits.csv first.")
    # Rotate across cohorts within each priority (beauty, fashion, food, health, women…) so a
    # run that is stopped early is still balanced, instead of finishing one category first.
    queues = {}
    for s in subs:
        queues.setdefault((int(s["priority"]), s["cohort"]), []).append(s)
    ordered = []
    for pri in sorted({k[0] for k in queues}):
        groups = [q for k, q in queues.items() if k[0] == pri]
        while any(groups):
            for q in groups:
                if q:
                    ordered.append(q.pop(0))
    subs = ordered

    lock = single_instance()  # noqa: F841 (held open until exit)
    state = load_state()
    state["_since"] = min(state.get("_since", since_ts), since_ts)  # for --status

    def one(s):
        """Returns "ok", "stopped" or "failed"."""
        sub, meta = s["subreddit"], (s["cohort"], s["sub_cohort"])
        print(f"r/{sub} [{s['cohort']}/{s['sub_cohort']}] starting")
        try:
            if not pull(sub, "posts", since_ts, state, meta):
                return "stopped"
            if mode == "all" and s["comments"] == "yes":
                # comments_since (optional column) caps very large comment sections to recent years
                c_since = s.get("comments_since") or ""
                c_ts = max(since_ts, int(datetime.strptime(c_since, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc).timestamp())) if c_since else since_ts
                if not pull(sub, "comments", c_ts, state, meta):
                    return "stopped"
            return "ok"
        except StopRequested:
            print(f"  {sub}: stopped")
            return "stopped"
        except Exception as e:  # one bad subreddit shouldn't stop the others
            print(f"  {sub}: FAILED for now ({e}); will try again at the end")
            return "failed"

    mode = "none" if a.no_comments else a.comments

    def two(s):
        """Phase 2: comments for rich posts. Skips subreddits marked comments=never."""
        if s["comments"] == "never":
            return "ok"
        try:
            return "ok" if rich_comments(s["subreddit"], (s["cohort"], s["sub_cohort"]), state) else "stopped"
        except StopRequested:
            return "stopped"
        except Exception as e:
            print(f"  {s['subreddit']} rich threads: FAILED for now ({e}); run again to resume")
            return "failed"

    print(f"{len(subs)} subreddits, {a.workers} at a time, comments: {mode}")
    with ThreadPoolExecutor(max(1, a.workers)) as ex:
        # Phase 1: posts (and, in "all" mode, every comment) for every subreddit
        results = list(ex.map(one, subs))
        # Second chance for anything the archive kept refusing; it resumes from its checkpoint.
        retry = [s for s, r in zip(subs, results) if r == "failed"]
        if retry and not STOP.exists():
            print(f"Retrying {len(retry)} subreddit(s) that failed: {', '.join(s['subreddit'] for s in retry)}")
            for s, r in zip(retry, ex.map(one, retry)):
                results[subs.index(s)] = r
        # Phase 2: comments for rich posts only (subreddits whose posts finished)
        if mode == "rich" and "stopped" not in results and not STOP.exists():
            print("Posts phase finished. Now fetching comments for rich posts only.")
            ready = [s for s, r in zip(subs, results) if r == "ok"]
            r2 = dict(zip([s["subreddit"] for s in ready], ex.map(two, ready)))
            again = [s for s in ready if r2[s["subreddit"]] == "failed"]
            if again and not STOP.exists():
                print(f"Retrying rich threads for: {', '.join(s['subreddit'] for s in again)}")
                for s, r in zip(again, ex.map(two, again)):
                    r2[s["subreddit"]] = r
            save_state(state)
            results = [r2.get(s["subreddit"], r) for s, r in zip(subs, results)]
    failed = [s["subreddit"] for s, r in zip(subs, results) if r == "failed"]
    if "stopped" in results:
        print("Stopped. Progress is saved; run start.bat again to resume.")
    elif failed:
        print(f"Finished, but these still failed: {', '.join(failed)}. Run start.bat again later to resume them.")
    else:
        print("All done.")

if __name__ == "__main__":
    main()
