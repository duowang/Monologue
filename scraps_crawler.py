import argparse
import csv
import re
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

WP_POSTS_API = "https://scrapsfromtheloft.com/wp-json/wp/v2/posts"

TAG_CONFIG = {
    1578: {
        "author": "John Oliver",
        "title_keywords": ["last week tonight"],
    },
    654: {
        "author": "Daily Show",
        "title_keywords": ["daily show"],
    },
    1628: {
        "author": "Seth Meyers",
        "title_keywords": ["late night with seth meyers", "a closer look"],
    },
    3325: {
        "author": "Jimmy Kimmel",
        "title_keywords": ["jimmy kimmel live", "jimmy kimmel delivers first monologue"],
    },
    4530: {
        "author": "Jimmy Kimmel",
        "title_keywords": ["jimmy kimmel live", "jimmy kimmel delivers first monologue"],
    },
    1382: {
        "author": "Jimmy Fallon",
        "title_keywords": ["the tonight show starring jimmy fallon"],
    },
    1821: {
        "author": "Stephen Colbert",
        "title_keywords": ["the late show with stephen colbert"],
    },
}

SPEAKER_ALIASES = {
    "john": "John Oliver",
    "john oliver": "John Oliver",
    "jon": "Jon Stewart",
    "jon stewart": "Jon Stewart",
    "seth": "Seth Meyers",
    "seth meyers": "Seth Meyers",
    "jimmy": "Jimmy Kimmel",
    "jimmy kimmel": "Jimmy Kimmel",
    "jimmy fallon": "Jimmy Fallon",
    "fallon": "Jimmy Fallon",
    "stephen": "Stephen Colbert",
    "stephen colbert": "Stephen Colbert",
    "colbert": "Stephen Colbert",
    "desi": "Desi Lydic",
    "desi lydic": "Desi Lydic",
    "jordan klepper": "Jordan Klepper",
    "michael kosta": "Michael Kosta",
    "ronny chieng": "Ronny Chieng",
    "conan": "Conan O'Brien",
    "conan o'brien": "Conan O'Brien",
    "trevor noah": "Trevor Noah",
}


def normalize_text(value):
    return re.sub(r"\s+", " ", value or "").strip()


def parse_date(value):
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.strftime("%Y-%m-%d")


def get_json_with_retry(session, url, params, retries=4, sleep_s=0.8):
    last_error = None
    for _ in range(retries):
        try:
            response = session.get(url, params=params, timeout=35)
            if response.status_code == 429:
                time.sleep(sleep_s)
                continue
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = exc
            time.sleep(sleep_s)
    raise last_error


def canonical_speaker(name):
    text = normalize_text(name).lower()
    text = re.sub(r"[^a-z' ]", "", text)
    text = normalize_text(text)
    if text in SPEAKER_ALIASES:
        return SPEAKER_ALIASES[text]
    if 1 <= len(text.split()) <= 3:
        return " ".join(part.capitalize() for part in text.split())
    return None


def is_relevant_post(title, link, title_keywords):
    lower_title = normalize_text(title).lower()
    lower_link = normalize_text(link).lower()
    if "transcript" not in lower_title and "transcript" not in lower_link:
        return False
    if not title_keywords:
        return True
    return any(keyword in lower_title for keyword in title_keywords)


def is_noise_paragraph(text):
    lower = text.lower()
    if len(text) < 40:
        return True
    noise_prefixes = [
        "aired on ",
        "main segment:",
        "other segments:",
        "the daily show ,",
        "the daily show,",
        "* * *",
    ]
    return any(lower.startswith(prefix) for prefix in noise_prefixes)


def extract_quotes(content_html, default_author):
    soup = BeautifulSoup(content_html, "html.parser")
    quotes = defaultdict(list)
    seen = set()

    for para in soup.select("p"):
        text = normalize_text(para.get_text(" ", strip=True))
        if not text:
            continue

        match = re.match(r"^([A-Za-z][A-Za-z .'-]{0,40}):\s+(.+)$", text)
        if match:
            speaker = canonical_speaker(match.group(1)) or default_author
            quote = normalize_text(match.group(2)).strip("“”\"")
            if len(quote) >= 20 and quote not in seen:
                quotes[speaker].append(quote)
                seen.add(quote)
            continue

        if is_noise_paragraph(text):
            continue

        quote = text.strip("“”\"")
        if quote not in seen:
            quotes[default_author].append(quote)
            seen.add(quote)

    return quotes


def fetch_posts_for_tag(session, tag_id):
    page = 1
    while True:
        params = {
            "tags": tag_id,
            "per_page": 100,
            "page": page,
            "_fields": "id,date,title,link,content",
        }
        response = get_json_with_retry(session, WP_POSTS_API, params)
        posts = response.json()
        if not posts:
            break
        yield from posts

        total_pages = int(response.headers.get("X-WP-TotalPages", "1"))
        if page >= total_pages:
            break
        page += 1


def write_day_csv(output_dir, date_value, by_author):
    path = Path(output_dir) / f"{date_value}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["name", "monologue"])
        writer.writeheader()
        for author, quotes in by_author.items():
            for quote in quotes:
                writer.writerow({"name": author, "monologue": quote})
    return path


def build_parser():
    parser = argparse.ArgumentParser(
        description="Crawl late-night transcript posts from scrapsfromtheloft.com."
    )
    parser.add_argument("--output-dir", default="scraps")
    parser.add_argument("--from-date", default="2017-01-01")
    parser.add_argument("--to-date", default=None)
    parser.add_argument("--skip-existing", action="store_true", default=True)
    parser.add_argument("--overwrite-existing", action="store_true")
    parser.add_argument(
        "--prune-stale",
        action="store_true",
        help=(
            "When set, remove existing CSV files inside --output-dir that are "
            "within [from-date, to-date] but no longer present in crawl output."
        ),
    )
    return parser


def parse_date_filename(path):
    try:
        return datetime.strptime(path.stem, "%Y-%m-%d").date()
    except ValueError:
        return None


def main():
    args = build_parser().parse_args()
    if args.overwrite_existing:
        args.skip_existing = False

    from_date = datetime.strptime(args.from_date, "%Y-%m-%d").date()
    to_date = (
        datetime.strptime(args.to_date, "%Y-%m-%d").date()
        if args.to_date
        else datetime.utcnow().date()
    )

    session = requests.Session()
    day_quotes = defaultdict(lambda: defaultdict(list))
    ignored_posts = 0
    scanned_posts = 0

    for tag_id, tag_config in TAG_CONFIG.items():
        default_author = tag_config["author"]
        title_keywords = tag_config.get("title_keywords", [])
        for post in fetch_posts_for_tag(session, tag_id):
            scanned_posts += 1
            date_value = parse_date(post["date"])
            date_obj = datetime.strptime(date_value, "%Y-%m-%d").date()
            if date_obj < from_date or date_obj > to_date:
                ignored_posts += 1
                continue

            title = normalize_text(post.get("title", {}).get("rendered", ""))
            link = normalize_text(post.get("link", ""))
            if not is_relevant_post(title, link, title_keywords):
                ignored_posts += 1
                continue

            quotes = extract_quotes(
                post.get("content", {}).get("rendered", ""),
                default_author=default_author,
            )
            if not quotes:
                ignored_posts += 1
                continue

            for author, entries in quotes.items():
                day_quotes[date_value][author].extend(entries)

    saved = 0
    skipped = 0
    for date_value in sorted(day_quotes):
        out_path = Path(args.output_dir) / f"{date_value}.csv"
        if args.skip_existing and out_path.exists():
            skipped += 1
            print(f"[skipped] date={date_value} file={out_path}")
            continue
        path = write_day_csv(args.output_dir, date_value, day_quotes[date_value])
        quote_count = sum(len(v) for v in day_quotes[date_value].values())
        print(
            f"[saved] date={date_value} authors={len(day_quotes[date_value])} "
            f"quotes={quote_count} file={path}"
        )
        saved += 1

    pruned = 0
    if args.prune_stale:
        output_dir = Path(args.output_dir)
        keep_paths = {output_dir / f"{date_value}.csv" for date_value in day_quotes}
        for existing in output_dir.glob("*.csv"):
            file_date = parse_date_filename(existing)
            if file_date is None:
                continue
            if not (from_date <= file_date <= to_date):
                continue
            if existing in keep_paths:
                continue
            existing.unlink()
            pruned += 1
            print(f"[pruned] file={existing}")

    print(
        f"Summary: scanned_posts={scanned_posts} saved={saved} "
        f"skipped={skipped} ignored_posts={ignored_posts} pruned={pruned}"
    )


if __name__ == "__main__":
    main()
