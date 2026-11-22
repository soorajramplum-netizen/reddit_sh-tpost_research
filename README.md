# Reddit Downloader

Download the full history of posts, and the discussion that matters, from a list of subreddits into simple CSV files tagged by cohort. Built for consumer research: finding what people actually struggle with, in their own words, with a date attached.

- **Free.** It uses the public [Arctic Shift](https://arctic-shift.photon-reddit.com) Reddit archive, so you need no Reddit account or API key.
- **Nothing to install** beyond Python 3.9+. It uses only the standard library.
- **Safe to stop at any time.** It saves after every batch and resumes exactly where it stopped.

---

## Why use it

Surveys and focus groups capture what people *say* they want. Reddit captures what they are frustrated by, unprompted, in their own words. This tool turns that into a dated dataset you can count, search, chart and trace back to the original post.

| Benefit | What it gives you |
| --- | --- |
| **Real consumer tensions** | Unprompted problems, the workarounds people already use, and the brands they name |
| **Product and app ideas** | Unmet needs that come up again and again point to things worth building |
| **Idea check** | Before building, search for the problem behind an idea: is it real, frequent and strongly felt? |
| **Trend tracking** | Every row is dated, so you can chart a topic by month, quarter or year and separate rising needs from one-off spikes |
| **Brand and competitor listening** | What people praise, complain about and switch to, and why |
| **Consumer language** | The exact words people use for a problem, for briefs, claims, copy and naming |
| **Cohort comparison** | Subreddits are tagged by cohort (beauty, women, parents, a city…), so you can compare how a need differs across groups |
| **Low cost** | Downloading is free; you spend effort only on reading and analysis |

---

## How it works

1. **Reads the subreddit list** from `subreddits.csv`. Each subreddit is tagged with a lens, a cohort and a priority (1-4).
2. **Downloads posts first, for every chosen subreddit.** It goes from the start date up to today, oldest first, in pages of about 100, through the Arctic Shift `/posts/search` endpoint. Several subreddits run in parallel (3 by default).
3. **Picks the "rich" posts.** A post counts as rich if it is long and contains:
   - a contrast (`but`, `even though`, `although`, `still`, `despite`, `phir bhi`, `lekin`), and
   - something personal (`I feel`, `my skin`, `my parents`…),
   - or it is a very busy thread.

   That is about 4% of posts, but it holds most of the useful discussion.
4. **Downloads comments for rich posts only**, busiest threads first, up to a cap per subreddit (`RICH_CAP_DEFAULT = 3000`, lower for a few very large subreddits). Every comment is optional (`--comments all`) and slow: one big subreddit can have about 800k.
5. **Saves its place after every batch** in `state.json`, so re-running adds only what is new. An earlier start date fills in older history.

### Reliability

- **Every batch is written to disk before progress is updated.** If the window closes, the laptop sleeps or the power goes mid-batch, the half-written batch is cut off on the next start and fetched again.
- **No gaps or duplicates at batch edges.** Items posted in the same second never fall between two batches or get saved twice.
- **Only one copy can run at a time.** A lock file (`.running.lock`) makes a second copy refuse to start.
- **A busy archive is handled.**
  - It waits and retries with back-off for about 12 minutes.
  - A subreddit that still fails is tried once more at the end of the run.
  - The next run picks up any subreddit that still failed.
- **Clean stop.** Create a file named `STOP`, or run `stop.bat`. The downloader finishes its current batch, saves and exits.

---

## Quick start (Windows)

1. Install Python 3.9 or newer from [python.org](https://www.python.org/downloads/) and tick **"Add python.exe to PATH"**.
2. Double-click **`start.bat`**.
3. Enter a priority (1 to 4) and a start date (e.g. `2023-01-01`), or press Enter to accept the defaults.
   - Priority 1 is categories only. Priority 2 adds cohorts. Priority 3 adds general subreddits and big cities. Priority 4 is everything.

| To… | Do this |
| --- | --- |
| Stop cleanly | Double-click `stop.bat` |
| Resume | Double-click `start.bat` again |
| See progress | Double-click `status.bat`; it works even while a download is running |
| Run unattended | Point Windows Task Scheduler at `start_auto.bat`, which runs without prompts |

## Command line (any OS)

```bash
python reddit_download.py                                   # priority 1 subreddits, since 2023-01-01
python reddit_download.py --priority 2                      # priorities 1 and 2
python reddit_download.py --subs IndianHaircare TwoXIndia   # only these subreddits
python reddit_download.py --since 2021-01-01 --no-comments  # posts only, from 2021
python reddit_download.py --workers 5                       # 5 subreddits at a time
python reddit_download.py --status                          # show progress
```

| Option | Default | What it does |
| --- | --- | --- |
| `--priority N` | `1` | Downloads every subreddit whose priority is N or lower |
| `--subs A B …` | | Downloads only the named subreddits (overrides `--priority`) |
| `--since YYYY-MM-DD` | `2023-01-01` | Earliest date to download |
| `--comments rich\|all\|none` | `rich` | `rich`: posts first, then comments for rich posts only. `all`: every comment for subreddits marked `yes` (slow). `none`: posts only |
| `--no-comments` | off | Same as `--comments none` |
| `--workers N` | `3` | How many subreddits to download at once |
| `--status` | | Shows progress and exits |

On the command line, delete the `STOP` file yourself before resuming. `start.bat` deletes it for you.

---

## Configuration

### The subreddit list: `subreddits.csv`

| Column | Meaning |
| --- | --- |
| `subreddit` | Name without `r/` |
| `lens` | `category` (a product area), `cohort` (a group of people) or `place` (a city or state) |
| `cohort`, `sub_cohort` | Your own grouping, e.g. `beauty` / `haircare`; written onto every row |
| `priority` | 1 = most important. `--priority 2` downloads everything with priority 1 or 2 |
| `comments` | `yes` / `no`: whether `--comments all` downloads every comment. `never` also skips rich-thread comments (e.g. a teen subreddit) |
| `comments_since` | Optional date (`YYYY-MM-DD`): comments only from then on, to cap very busy subreddits |
| `activity_90d` | Notes on how active the subreddit is; not used by the script |

Add, remove or re-prioritise rows freely. The list that ships with the repo covers about 90 Indian subreddits across beauty, fashion, food, health, life stages, shopping, work and cities.

### Output folder: `data_dir.txt`

By default, data goes to `data/` next to the script. A full run can reach several GB, so to keep it somewhere else (for example outside a synced folder such as OneDrive or Dropbox), put the folder path on the first line of a file named `data_dir.txt`.

### Rich-comment caps

`RICH_CAP_DEFAULT` and the `RICH_CAP` dictionary near the top of `reddit_download.py` set how many rich threads are fetched per subreddit. The `RICH_CONTRAST` and `RICH_PERSONAL` patterns define what counts as rich. Edit them to suit your research.

---

## Output

```
data/
  <subreddit>/posts.csv               every post
  <subreddit>/comments_rich.csv       comments on rich posts (default mode)
  <subreddit>/comments_rich_done.txt  threads already fetched
  <subreddit>/comments.csv            every comment (only with --comments all)
  state.json                          progress for each subreddit (what's covered, from and to)
  progress_log.csv                    progress over time
  .running.lock                       stops two copies running at once
```

**posts.csv:** `subreddit, cohort, sub_cohort, date, url, id, created_utc, author, title, selftext, score, num_comments, link_flair_text, over_18`

**comments_rich.csv / comments.csv:** `subreddit, cohort, sub_cohort, date, url, id, created_utc, author, body, score, link_id, parent_id`

- `link_id` is the post a comment belongs to (`t3_<post id>`).
- `parent_id` is what it replies to: the post (`t3_…`) or another comment (`t1_…`).
- Deleted and removed items, and AutoModerator, are skipped.
- Files are UTF-8 and open in Excel or pandas.

### Example: analyse the data

```python
import pandas as pd
posts = pd.read_csv("data/IndianHaircare/posts.csv", parse_dates=["date"])
hits = posts[posts["selftext"].str.contains("hair fall", case=False, na=False)]
print(hits.groupby(hits["date"].dt.to_period("Q")).size())   # mentions per quarter
```

---

## How big it gets

- About 2,000 posts a minute for each subreddit being downloaded.
- Several hours for a large subreddit's comments.
- Hundreds of MB to a few GB of CSV for a large list. A full run of about 90 subreddits since 2023 comes to roughly 7M posts.

Plan big runs overnight.

---

## Notes and limits

- **Be polite.** Arctic Shift is a free community service. Each worker pauses between requests and backs off when the server is busy. More than 3-5 workers tends to cause errors rather than speed.
- **"422" or timeout messages** mean the archive is busy. The script retries automatically.
- **Very recent posts** may show low scores and comment counts, because the archive captures them shortly after posting.
- **Use it to find needs, not to measure them.** Indian Reddit skews urban, young, male and English-speaking. Use it to discover tensions, not to measure how common they are.
- **Respect authors.** Reddit content belongs to its authors. Use it for research and quote people anonymously.

## Files

```
reddit_download.py   the downloader
subreddits.csv       the subreddit list with cohorts and priorities
start.bat            Windows: double-click to run (asks priority and start date)
start_auto.bat       Windows: unattended run, for Task Scheduler
stop.bat             Windows: double-click to stop cleanly
status.bat           Windows: double-click to see progress
data_dir.txt         optional: where to save the data
```
