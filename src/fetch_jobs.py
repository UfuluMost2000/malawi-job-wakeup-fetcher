import json
import re
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# Malawi job sources (light scraping; 1 run/day recommended)
SOURCES = [
    {
        "name": "OnlineJobMW",
        "url": "https://onlinejobmw.com/job-category/job_vacancies/",
    },
    {
        "name": "Ntchito",
        "url": "https://ntchito.com/",
    },
]

# Adjust keywords to your interests
KEYWORDS = [
    "IT", "ICT", "Network", "Systems", "System", "Administrator", "Sysadmin",
    "Cloud", "Security", "Cyber", "Firewall", "Support", "Engineer",
    "Data", "Analyst", "Developer", "Software", "Project", "M&E", "Monitoring"
]

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
OUT_DIR = ROOT / "outputs"
SEEN_FILE = DATA_DIR / "seen.json"
DIGEST_FILE = OUT_DIR / "daily_jobs.md"
LOG_FILE = OUT_DIR / "run.log"


def log(msg: str) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg.rstrip() + "\n")


def fetch_html(url: str) -> str:
    headers = {"User-Agent": "Mozilla/5.0 (MalawiJobWakeupFetcher/1.0)"}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text


def keyword_match(text: str) -> bool:
    t = text.lower()
    return any(k.lower() in t for k in KEYWORDS)


def load_seen() -> set[str]:
    if SEEN_FILE.exists():
        try:
            return set(json.loads(SEEN_FILE.read_text(encoding="utf-8")))
        except Exception:
            return set()
    return set()


def save_seen(seen: set[str]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SEEN_FILE.write_text(json.dumps(sorted(seen), indent=2), encoding="utf-8")


def parse_ntchito(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    jobs = []
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        title = a.get_text(" ", strip=True)

        if not title or len(title) < 6:
            continue
        if not href.startswith("https://ntchito.com/"):
            continue

        # Skip obvious nav links
        if title.lower() in {"home", "jobs", "contact", "register", "login"}:
            continue

        # Keep only likely “job post” links by keyword match
        if keyword_match(title):
            jobs.append({
                "id": f"ntchito::{href}",
                "title": title,
                "link": href,
                "source": "Ntchito",
                "meta": ""
            })

    # Deduplicate by id
    uniq = {}
    for j in jobs:
        uniq[j["id"]] = j
    return list(uniq.values())


def parse_onlinejobmw(html: str) -> list[dict]:
    """
    Onlinejobmw category pages can vary; we do a light heuristic:
    - collect page text lines
    - detect lines that start with 'Posted'
    - look backwards for title/company/location
    """
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

    jobs = []
    for i, ln in enumerate(lines):
        if ln.lower().startswith("posted "):
            posted = ln
            title = lines[i - 4] if i - 4 >= 0 else ""
            company = lines[i - 3] if i - 3 >= 0 else ""
            location = lines[i - 2] if i - 2 >= 0 else ""

            if not title or len(title) < 6:
                continue
            if not keyword_match(title):
                continue

            job_id = f"onlinejobmw::{title}::{company}::{posted}"
            meta_parts = [p for p in [company, location, posted] if p]
            meta = " | ".join(meta_parts)

            jobs.append({
                "id": job_id,
                "title": title,
                "link": "https://onlinejobmw.com/job-category/job_vacancies/",
                "source": "OnlineJobMW",
                "meta": meta
            })

    return jobs


def write_digest(new_jobs: list[dict]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")

    lines = [f"# Malawi Job Digest — {today}", ""]
    if not new_jobs:
        lines.append("No new matching jobs today.")
    else:
        for j in new_jobs:
            lines.append(f"- **{j['title']}** ({j['source']})")
            if j.get("meta"):
                lines.append(f"  - {j['meta']}")
            lines.append(f"  - {j['link']}")

    DIGEST_FILE.write_text("\n".join(lines), encoding="utf-8")


def main():
    log("=== Script started ===")

    seen = load_seen()
    collected = []

    for s in SOURCES:
        try:
            html = fetch_html(s["url"])
            if s["name"] == "Ntchito":
                collected.extend(parse_ntchito(html))
            elif s["name"] == "OnlineJobMW":
                collected.extend(parse_onlinejobmw(html))
        except Exception as e:
            log(f"ERROR fetching {s['name']}: {e}")

    # Only keep unseen items
    new_jobs = [j for j in collected if j["id"] not in seen]

    # Update seen (so tomorrow you only get new ones)
    for j in collected:
        seen.add(j["id"])
    save_seen(seen)

    write_digest(new_jobs)

    log(f"New items: {len(new_jobs)}")
    log(f"Digest: {DIGEST_FILE}")
    log("=== Script finished ===")


if __name__ == "__main__":
    main()
