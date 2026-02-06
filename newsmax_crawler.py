import argparse
import csv
import os
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests
from bs4 import BeautifulSoup

COMEDIAN_NAMES = {
    "Jay": "Jay Leno",
    "Meyers": "Seth Meyers",
    "Letterman": "David Letterman",
    "Kimmel": "Jimmy Kimmel",
    "Conan": "Conan O'Brian",
    "Fallon": "Jimmy Fallon",
    "Corden": "James Corden",
    "Colbert": "Stephen Colbert",
    "Ferguson": "Craig Ferguson",
}

DEFAULT_START_PAGE = 1756
DEFAULT_BASE_URL = "https://www.newsmax.com/jokes/{page}"
DEFAULT_ARCHIVE_URL = "https://www.newsmax.com/jokes/archive/"
DEFAULT_FALLBACK_WINDOW = 1000
BAD_NAME_TOKENS = {"newsmax", "jokes", "personalities"}
NAME_ALIASES = {
    "conan o'brien": "Conan O'Brian",
    "conan obrien": "Conan O'Brian",
}


def get_name(value):
    if not value:
        return None
    for key, canonical_name in COMEDIAN_NAMES.items():
        if re.search(key, value, flags=re.IGNORECASE):
            return canonical_name
    return None


def normalize_text(value):
    return re.sub(r"\s+", " ", value).strip()


def title_case_token(token):
    if "'" in token:
        return "'".join(part.capitalize() for part in token.split("'"))
    return token.capitalize()


def apply_alias(name):
    if not name:
        return None
    key = normalize_text(name).lower()
    return NAME_ALIASES.get(key, name)


def clean_candidate_name(value):
    if not value:
        return None
    raw = normalize_text(value)
    if "://" in raw or raw.startswith("/"):
        return None
    if re.search(r"\.(jpg|jpeg|png|gif|webp|svg)\b", raw, flags=re.IGNORECASE):
        return None

    text = raw
    text = re.sub(r"[^A-Za-z.' -]", " ", text)
    text = normalize_text(text)
    if not text:
        return None
    if any(token in text.lower() for token in BAD_NAME_TOKENS):
        return None
    if any(char.isdigit() for char in text):
        return None

    words = [w for w in re.split(r"\s+", text) if w]
    if len(words) < 2 or len(words) > 5:
        return None

    words = [title_case_token(w.strip(".'-")) for w in words if w.strip(".'-")]
    cleaned = normalize_text(" ".join(words))
    if len(cleaned) < 3:
        return None
    return apply_alias(cleaned)


def split_camel_case(value):
    if not value:
        return ""
    return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", value)


def infer_name_from_alt(alt):
    text = normalize_text(alt)
    if not text:
        return None

    patterns = [
        r"\bwith\s+([A-Za-z.'\- ]+)$",
        r"\bstarring(?:\s+with)?\s+([A-Za-z.'\- ]+)$",
        r"\bhosted by\s+([A-Za-z.'\- ]+)$",
        r"\bfeaturing\s+([A-Za-z.'\- ]+)$",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            candidate = clean_candidate_name(match.group(1))
            if candidate:
                return candidate

    candidate = clean_candidate_name(text)
    if candidate:
        return candidate
    return None


def infer_name_from_src(src):
    if not src:
        return None
    path = urlparse(src).path or src
    filename = os.path.basename(unquote(path))
    stem, _ = os.path.splitext(filename)
    if not stem:
        return None

    stem = re.sub(r"(?i)^newsmax_jokes_personalities_", "", stem)
    stem = re.sub(r"(?i)_?jokes?$", "", stem)
    stem = stem.replace("_", " ")
    stem = split_camel_case(stem)

    candidate = clean_candidate_name(stem)
    if candidate:
        return candidate
    return None


def fetch(session, url, timeout, retries):
    last_error = None
    for _ in range(retries):
        try:
            response = session.get(url, timeout=timeout)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = exc
            time.sleep(0.5)
    raise last_error


def discover_latest_page(session, archive_url, timeout, retries):
    response = fetch(session, archive_url, timeout=timeout, retries=retries)
    if response is None:
        raise RuntimeError(f"Archive endpoint returned 404: {archive_url}")

    page_match = re.search(r"/jokes/(\d+)/?$", response.url)
    if page_match:
        return int(page_match.group(1))

    canonical_match = re.search(
        r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']https?://[^"\']*/jokes/(\d+)/?["\']',
        response.text,
        flags=re.IGNORECASE,
    )
    if canonical_match:
        return int(canonical_match.group(1))

    body_match = re.search(r"/jokes/(\d+)", response.text)
    if body_match:
        return int(body_match.group(1))

    raise RuntimeError("Unable to infer latest page id from archive page.")


def parse_date(soup):
    date_node = soup.find("div", class_="jokesDate")
    if date_node is None:
        return None

    date_text = normalize_text(date_node.get_text(" ", strip=True))
    try:
        return datetime.strptime(date_text, "%A %b %d %Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def parse_comedian_name(header_node):
    img = header_node.find("img")
    alt = img.attrs.get("alt", "") if img is not None else ""
    src = img.attrs.get("src", "") if img is not None else ""

    # Historical quirk: some Seth entries were mislabeled in alt text.
    if alt == "Late Night With Seth Meyers":
        resolved_name = get_name(src) or get_name(alt)
    else:
        resolved_name = get_name(alt) or get_name(src)

    if resolved_name:
        return resolved_name

    inferred_name = infer_name_from_alt(alt)
    if inferred_name:
        return inferred_name

    inferred_name = infer_name_from_src(src)
    if inferred_name:
        return inferred_name

    # Final fallback for future template shifts.
    header_text = normalize_text(" ".join(header_node.stripped_strings))
    inferred_name = infer_name_from_alt(header_text)
    if inferred_name:
        return inferred_name
    return None


def extract_jokes(header_node):
    jokes = []
    node = header_node.find_next_sibling()
    while node is not None:
        node_classes = node.attrs.get("class", [])
        if node.name == "div" and "jokesHeader" in node_classes:
            break
        if node.name == "p":
            text = normalize_text(node.get_text(" ", strip=True))
            if len(text) > 10:
                jokes.append(text)
        node = node.find_next_sibling()
    return jokes


def parse_monologue_page(html):
    soup = BeautifulSoup(html, "html.parser")
    joke_page = soup.find("div", class_="jokespage")
    date_value = parse_date(soup)
    if joke_page is None or date_value is None:
        return None, {}

    monologue_dict = {}
    for header_node in joke_page.find_all("div", class_="jokesHeader"):
        comedian_name = parse_comedian_name(header_node)
        if comedian_name is None:
            continue
        jokes = extract_jokes(header_node)
        if jokes:
            monologue_dict.setdefault(comedian_name, []).extend(jokes)

    return date_value, monologue_dict


def write_csv(output_dir, date_value, monologue_dict):
    output_path = Path(output_dir) / f"{date_value}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["name", "monologue"])
        writer.writeheader()
        for name, jokes in monologue_dict.items():
            for joke in jokes:
                writer.writerow({"name": name, "monologue": joke})
    return output_path


def crawl_page(session, page, args):
    url = DEFAULT_BASE_URL.format(page=page)
    response = fetch(session, url, timeout=args.timeout, retries=args.retries)
    if response is None:
        return "missing", None, None

    date_value, monologue_dict = parse_monologue_page(response.text)
    if date_value is None or not monologue_dict:
        return "missing", None, None

    output_path = Path(args.output_dir) / f"{date_value}.csv"
    if args.skip_existing and output_path.exists():
        return "skipped", date_value, output_path

    write_csv(args.output_dir, date_value, monologue_dict)
    return "saved", date_value, output_path


def build_parser():
    parser = argparse.ArgumentParser(
        description="Crawl Newsmax late-night jokes and write daily CSV files."
    )
    parser.add_argument("--start-page", type=int, default=DEFAULT_START_PAGE)
    parser.add_argument("--end-page", type=int, default=None)
    parser.add_argument(
        "--auto-end",
        action="store_true",
        help="Infer latest page id from /jokes/archive when --end-page is omitted.",
    )
    parser.add_argument("--output-dir", default="newsmax")
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        default=True,
        help="Skip writing if target date CSV already exists (default).",
    )
    parser.add_argument(
        "--overwrite-existing",
        action="store_true",
        help="Overwrite existing CSV files.",
    )
    parser.add_argument(
        "--stop-after-miss",
        type=int,
        default=50,
        help="Stop after this many consecutive missing/invalid pages.",
    )
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument(
        "--user-agent",
        default="",
        help=(
            "Optional User-Agent header. Leave empty to use the default "
            "requests User-Agent (often more reliable for Newsmax)."
        ),
    )
    parser.add_argument(
        "--fallback-window",
        type=int,
        default=DEFAULT_FALLBACK_WINDOW,
        help=(
            "Page window used when --auto-end discovery fails or when "
            "--end-page is omitted without --auto-end."
        ),
    )
    parser.add_argument("--sleep", type=float, default=0.1)
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.overwrite_existing:
        args.skip_existing = False

    session = requests.Session()
    if args.user_agent:
        session.headers.update({"User-Agent": args.user_agent})

    if args.end_page is None and args.auto_end:
        try:
            args.end_page = discover_latest_page(
                session,
                archive_url=DEFAULT_ARCHIVE_URL,
                timeout=args.timeout,
                retries=args.retries,
            )
            print(f"Discovered latest page: {args.end_page}")
        except Exception as exc:  # noqa: BLE001
            args.end_page = args.start_page + args.fallback_window
            print(
                "[warn] auto-end discovery failed "
                f"(reason={type(exc).__name__}: {exc}); "
                f"falling back to [{args.start_page}, {args.end_page}]"
            )
    elif args.end_page is None:
        args.end_page = args.start_page + args.fallback_window
        print(
            "No --end-page given; defaulting to a bounded window "
            f"[{args.start_page}, {args.end_page}]"
        )

    if args.end_page < args.start_page:
        raise ValueError("--end-page must be >= --start-page")

    consecutive_misses = 0
    saved = 0
    skipped = 0
    missing = 0

    for page in range(args.start_page, args.end_page + 1):
        try:
            status, date_value, path = crawl_page(session, page, args)
        except Exception as exc:  # noqa: BLE001
            print(f"[error] page={page} reason={exc}")
            status = "missing"
            date_value = None
            path = None

        if status == "saved":
            consecutive_misses = 0
            saved += 1
            print(f"[saved] page={page} date={date_value} file={path}")
        elif status == "skipped":
            consecutive_misses = 0
            skipped += 1
            print(f"[skipped] page={page} date={date_value} file={path}")
        else:
            consecutive_misses += 1
            missing += 1
            print(f"[missing] page={page}")

        if consecutive_misses >= args.stop_after_miss:
            print(
                f"Stopping after {consecutive_misses} consecutive misses "
                f"(threshold={args.stop_after_miss})."
            )
            break

        if args.sleep > 0:
            time.sleep(args.sleep)

    print(
        "Summary:",
        f"saved={saved}",
        f"skipped={skipped}",
        f"missing={missing}",
        sep=" ",
    )


if __name__ == "__main__":
    main()
