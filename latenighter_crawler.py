import argparse
import csv
import re
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

WP_POSTS_API = "https://latenighter.com/wp-json/wp/v2/posts"
MONOLOGUES_TAG_ID = 180

IGNORED_HEADINGS = {
    "Read More About",
    "More News",
    "Cancel reply",
    "Tonight's Lineups",
    "The Latest",
    "LateNighter Podcasts",
}

HOST_ALIASES = {
    "Stephen Colbert": ["stephen colbert", "colbert"],
    "Jimmy Kimmel": ["jimmy kimmel", "kimmel"],
    "Seth Meyers": ["seth meyers", "meyers"],
    "Jimmy Fallon": ["jimmy fallon", "fallon"],
    "Desi Lydic": ["desi lydic", "lydic"],
    "Jon Stewart": ["jon stewart", "stewart"],
    "Jordan Klepper": ["jordan klepper", "klepper"],
    "Ronny Chieng": ["ronny chieng", "chieng"],
    "Michael Kosta": ["michael kosta", "kosta"],
    "Taylor Tomlinson": ["taylor tomlinson", "tomlinson"],
}


def normalize_text(value):
    return re.sub(r"\s+", " ", value or "").strip()


def date_to_iso(value):
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.strftime("%Y-%m-%d")


def is_likely_host_heading(text):
    text = normalize_text(text).strip(":")
    if not text or text in IGNORED_HEADINGS:
        return False
    if len(text) > 40:
        return False
    if any(token in text.lower() for token in ["read more", "more news", "lineups", "latest"]):
        return False
    words = text.split()
    return 1 <= len(words) <= 5


def clean_host_name(text):
    text = normalize_text(text).strip(":")
    text = re.sub(r"^[Tt]he Daily Show(?:’s|'s)\s+", "", text)
    text = re.sub(r"^[Ww]ith\s+", "", text)
    return text


def infer_host_from_text(text):
    lower = normalize_text(text).lower()
    for canonical, aliases in HOST_ALIASES.items():
        for alias in aliases:
            if alias in lower:
                return canonical
    return None


def infer_host_from_tail(text):
    tail = normalize_text(text)
    if not tail:
        return None
    m = list(re.finditer(r'[”"]', tail))
    if m:
        tail = tail[m[-1].end():]
    host = infer_host_from_text(tail)
    if host:
        return host
    return None


def extract_inline_quotes(text):
    text = normalize_text(text)
    if not text:
        return []

    matches = re.findall(r"[“\"](.{20,400}?)[”\"]", text)
    cleaned = []
    seen = set()
    for quote in matches:
        q = normalize_text(quote)
        if len(q.split()) < 5:
            continue
        if q in seen:
            continue
        seen.add(q)
        cleaned.append(q)
    return cleaned


def parse_quote_text(raw_quote):
    quotes = extract_inline_quotes(raw_quote)
    if quotes:
        return quotes[0]
    return normalize_text(raw_quote).strip("“”\"")


def parse_monologue_quotes(content_html):
    soup = BeautifulSoup(content_html, "html.parser")
    host = None
    quotes = {}

    for node in soup.find_all(["h2", "h3", "h4", "blockquote"]):
        if node.name in {"h2", "h3", "h4"}:
            heading = normalize_text(node.get_text(" ", strip=True))
            inferred = infer_host_from_text(heading)
            if inferred:
                host = inferred
            continue

        raw_quote = normalize_text(node.get_text(" ", strip=True))
        quote = parse_quote_text(raw_quote)
        if len(quote) < 20:
            continue
        author = infer_host_from_tail(raw_quote) or host or infer_host_from_text(raw_quote) or "Unknown"
        quotes.setdefault(author, []).append(quote)

    # Fallback for feature-style posts that embed quotes in paragraph text.
    if not quotes:
        for node in soup.find_all(["p", "li"]):
            text = normalize_text(node.get_text(" ", strip=True))
            if len(text) < 30:
                continue
            inline_quotes = extract_inline_quotes(text)
            if not inline_quotes:
                continue
            inferred_host = infer_host_from_text(text) or "Unknown"
            for quote in inline_quotes:
                quotes.setdefault(inferred_host, []).append(quote)

    return quotes


def fetch_posts(session, tag_id, per_page=100):
    page = 1
    while True:
        params = {
            "tags": tag_id,
            "per_page": per_page,
            "page": page,
            "_fields": "id,date,link,title,content",
        }
        response = session.get(WP_POSTS_API, params=params, timeout=30)
        if response.status_code == 400:
            break
        response.raise_for_status()
        posts = response.json()
        if not posts:
            break
        yield from posts

        total_pages = int(response.headers.get("X-WP-TotalPages", "1"))
        if page >= total_pages:
            break
        page += 1


def write_csv(output_dir, date_value, quotes_by_host):
    output_path = Path(output_dir) / f"{date_value}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["name", "monologue"])
        writer.writeheader()
        for host, quotes in quotes_by_host.items():
            for quote in quotes:
                writer.writerow({"name": host, "monologue": quote})
    return output_path


def build_parser():
    parser = argparse.ArgumentParser(
        description="Crawl LateNighter Monologues Round-Up posts into daily CSV files."
    )
    parser.add_argument("--output-dir", default="latenighter")
    parser.add_argument("--from-date", default="2018-09-29")
    parser.add_argument("--to-date", default=None)
    parser.add_argument("--skip-existing", action="store_true", default=True)
    parser.add_argument("--overwrite-existing", action="store_true")
    return parser


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
    saved = 0
    skipped = 0
    ignored = 0

    for post in fetch_posts(session, tag_id=MONOLOGUES_TAG_ID):
        date_value = date_to_iso(post["date"])
        date_obj = datetime.strptime(date_value, "%Y-%m-%d").date()
        if date_obj < from_date or date_obj > to_date:
            ignored += 1
            continue

        output_path = Path(args.output_dir) / f"{date_value}.csv"
        if args.skip_existing and output_path.exists():
            skipped += 1
            print(f"[skipped] date={date_value} file={output_path}")
            continue

        content_html = post.get("content", {}).get("rendered", "")
        quotes_by_host = parse_monologue_quotes(content_html)
        if not quotes_by_host:
            ignored += 1
            print(f"[ignored] date={date_value} reason=no-quotes")
            continue

        write_csv(args.output_dir, date_value, quotes_by_host)
        saved += 1
        quote_count = sum(len(v) for v in quotes_by_host.values())
        print(
            f"[saved] date={date_value} hosts={len(quotes_by_host)} "
            f"quotes={quote_count} file={output_path}"
        )

    print(f"Summary: saved={saved} skipped={skipped} ignored={ignored}")


if __name__ == "__main__":
    main()
