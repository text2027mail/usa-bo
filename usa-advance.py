import requests
import json
import os
import ssl
import random
import asyncio
import aiohttp
from aiohttp_retry import RetryClient, ExponentialRetry
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from collections import defaultdict
import base64
from typing import Dict, List, Optional, Any, Tuple, Set

# ================= CONFIGURATION =================

TARGET_LANGUAGES = ["Hindi", "Tamil", "Telugu", "Malayalam", "Kannada"]

# --- Fetch tomorrow's shows (all movies with target languages) ---
FETCH_TOMORROW = True

# --- Dates for which we want ALL movies (target languages) ---
SCRAPE_DATES = [
    date(2026, 8, 6),
    date(2026, 8, 8),
]

# --- Custom movies with extra language options ---
# Each entry can have:
#   movie_id, date (required)
#   add_extra_langs_shows: "all" | "english" | "unknown" (optional)
#   extra_langs_for_all_dates: bool (default False)
CUSTOM_MOVIES = [
    {"movie_id": 244612, "date": date(2026, 8, 25), "add_extra_langs_shows": "all", "extra_langs_for_all_dates": True},
    {"movie_id": 244612, "date": date(2026, 8, 26), "add_extra_langs_shows": "all", "extra_langs_for_all_dates": True},
]

# --- File containing US zip codes (one per line) ---
ZIP_FILE = "zipcodes.txt"

# --- Fandango credentials (public) ---
AUTHORIZATION_TOKEN = "<your-auth-token>"
SESSION_ID = "<your-session-id>"

# --- Render proxy URL (from environment) ---
RENDER_SEATMAP_URL = os.getenv("RENDER_SEATMAP_URL")
if not RENDER_SEATMAP_URL:
    raise EnvironmentError("Environment variable RENDER_SEATMAP_URL is not set")

# --- GitHub PAT and repo info ---
GITHUB_TOKEN = os.getenv("GH_PAT")
if not GITHUB_TOKEN:
    raise EnvironmentError("Environment variable GH_PAT is not set")

REPO_OWNER = "text2027mail"          # <-- CHANGE to your GitHub username
REPO_NAME = "usadata2026"             # <-- CHANGE if repo name differs

# --- Concurrency ---
MAX_WORKERS = 50
CONCURRENCY = 45

# --- Language & format detection ---
KNOWN_LANGUAGES = [
    "English", "Hindi", "Tamil", "Telugu", "Kannada",
    "Malayalam", "Punjabi", "Gujarati", "Marathi", "Bengali"
]
FORMAT_KEYWORDS = [
    "RPX", "D-Box", "IMAX", "EMX", "Sony Digital Cinema",
    "4DX", "ScreenX", "Cinemark XD", "Dolby Cinema"
]

# --- Debug logging (set to True for extra diagnostics) ---
DEBUG = False

# ================= SPOOFING HELPERS =================

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:{version}) Gecko/20100101 Firefox/{version}",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_{minor}_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_{minor}_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/{safari_ver} Safari/605.1.15",
]

def get_random_user_agent():
    template = random.choice(USER_AGENTS)
    return template.format(
        version=f"{random.randint(70,120)}.0.{random.randint(1000,5000)}.{random.randint(0,150)}",
        minor=random.randint(12,15),
        safari_ver=f"{random.randint(13,17)}.0.{random.randint(1,3)}"
    )

def get_random_ip():
    return ".".join(str(random.randint(1,255)) for _ in range(4))

# ================= HEADER BUILDERS =================

def get_headers2(zip_code, date_str):
    random_ip = get_random_ip()
    return {
        "User-Agent": get_random_user_agent(),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://www.fandango.com",
        "Referer": f"https://www.fandango.com/{zip_code}_movietimes?date={date_str}",
        "X-Forwarded-For": random_ip,
        "Client-IP": random_ip,
        "Connection": "keep-alive",
    }

def get_seatmap_headers():
    random_ip = get_random_ip()
    return {
        "User-Agent": get_random_user_agent(),
        "Origin": "https://fandango.com",
        "Referer": "https://tickets.fandango.com/mobileexpress/seatselection",
        "Connection": "keep-alive",
        "Authorization": AUTHORIZATION_TOKEN,
        "X-Fd-Sessionid": SESSION_ID,
        "authority": "tickets.fandango.com",
        "accept": "application/json",
        "X-Forwarded-For": random_ip,
        "Client-IP": random_ip,
    }

# ================= PARSERS =================

def extract_language(amenities: List[str]) -> str:
    """
    Robust language detection.
    First, look for explicit patterns like "Language: Hindi" or "Hindi Language".
    Then fallback to simple substring match over known languages.
    Returns the first match, or "Unknown".
    """
    for item in amenities:
        lower_item = item.lower()
        # Check for explicit patterns
        for lang in KNOWN_LANGUAGES:
            lang_lower = lang.lower()
            # Pattern: "Language: Hindi", "Audio: Hindi", "Hindi Language", etc.
            if f"{lang_lower} language" in lower_item or f"language: {lang_lower}" in lower_item or f"audio: {lang_lower}" in lower_item:
                return lang
    # Fallback to simple contains
    for item in amenities:
        lower_item = item.lower()
        for lang in KNOWN_LANGUAGES:
            if lang.lower() in lower_item:
                return lang
    return "Unknown"

def extract_format(amenities: List[str], default_format: str) -> str:
    for keyword in FORMAT_KEYWORDS:
        if any(keyword.lower() in a.lower() for a in amenities):
            return keyword
    return default_format

def prepare_showtimes(movie: Dict) -> List[Dict]:
    """Extract showtimes from a movie object with language and format."""
    out = []
    movie_title = movie.get("title", "Unknown")
    movie_id = movie.get("id")
    for variant in movie.get("variants", []):
        fmt = variant.get("formatName", "Standard")
        for ag in variant.get("amenityGroups", []):
            amenities = [a.get("name", "") for a in ag.get("amenities", [])]
            lang = extract_language(amenities)
            fmt_final = extract_format(amenities, fmt)
            for show in ag.get("showtimes", []):
                sid = show.get("id")
                if not sid:
                    continue
                out.append({
                    "showtime_id": sid,
                    "date": show.get("ticketingDate"),
                    "format": fmt_final,
                    "language": lang,
                    "movie_title": movie_title,
                    "movie_id": movie_id,
                })
    return out

# ================= THEATER SCRAPER (MULTIPROCESSING) =================

def get_theaters(zip_code: str, date_str: str) -> Dict:
    url = "https://www.fandango.com/napi/theaterswithshowtimes"
    params = {
        "zipCode": zip_code,
        "date": date_str,
        "page": 1,
        "limit": 40,
        "filter": "open-theaters",
        "filterEnabled": "true",
    }
    try:
        r = requests.get(url, headers=get_headers2(zip_code, date_str), params=params, timeout=10)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        if DEBUG:
            print(f"❌ Error fetching theaters for ZIP {zip_code}: {e}")
    return {}

def process_zip(args: Tuple[str, str]) -> List[Dict]:
    zip_code, date_str = args
    data = get_theaters(zip_code, date_str)
    theaters = []
    for theater in data.get("theaters", []):
        for movie in theater.get("movies", []):
            showtimes = prepare_showtimes(movie)
            if showtimes:
                theaters.append({
                    "theater_name": theater.get("name"),
                    "city": theater.get("city"),
                    "state": theater.get("state"),
                    "zip": theater.get("zip"),
                    "chainName": theater.get("chainName"),
                    "chainCode": theater.get("chainCode"),
                    "showtimes": showtimes,
                })
    return theaters

def scrape_all_shows_for_date(zip_list: List[str], date_str: str) -> List[Dict]:
    args = [(z, date_str) for z in zip_list]
    all_theaters = []
    with ProcessPoolExecutor(MAX_WORKERS) as exe:
        futures = [exe.submit(process_zip, a) for a in args]
        for f in tqdm(as_completed(futures), total=len(futures), desc=f"ZIP scan {date_str}"):
            try:
                res = f.result()
                if res:
                    all_theaters.extend(res)
            except Exception:
                pass
    flat = []
    for t in all_theaters:
        for s in t["showtimes"]:
            flat.append({
                **s,
                "theater_name": t["theater_name"],
                "city": t["city"],
                "state": t["state"],
                "chainName": t["chainName"],
            })
    return flat

# ================= SEATMAP FETCHING (ASYNC) via Render proxy =================

async def fetch_seat(session: aiohttp.ClientSession, show: Dict) -> None:
    sid = str(show["showtime_id"])
    params = {"showtime_id": sid}
    headers = get_seatmap_headers()
    try:
        async with session.get(RENDER_SEATMAP_URL, params=params, headers=headers, timeout=10) as resp:
            if resp.status != 200:
                show["error"] = {"status": resp.status}
                return
            text = await resp.text()
            text = text.strip()
            if text.startswith('e'):
                try:
                    status_code = int(text[1:])
                except ValueError:
                    status_code = 500
                show["error"] = {"status": status_code}
                return
            parts = text.split(',')
            if len(parts) != 3:
                show["error"] = {"status": 500, "reason": "Invalid response format"}
                return
            try:
                total = int(parts[0].strip())
                available = int(parts[1].strip())
                price = float(parts[2].strip())
            except ValueError:
                show["error"] = {"status": 500, "reason": "Invalid numeric values"}
                return
            if total == 0:
                show["error"] = {"status": 500, "reason": "No seats"}
                return
            if price == 0.0:
                show["error"] = {"status": 500, "reason": "Ticket price 0"}
                return
            sold = total - available
            show["totalSeatSold"] = sold
            show["totalSeatCount"] = total
            show["occupancy"] = round((sold / total) * 100, 2) if total else 0.0
            show["adultTicketPrice"] = price
            show["grossRevenueUSD"] = round(price * sold, 2)
            # Remove any previous error
            show.pop("error", None)
    except Exception as e:
        show["error"] = {"exception": str(e)}

async def run_seatmap_fetch(shows: List[Dict]) -> None:
    if not shows:
        return
    connector = aiohttp.TCPConnector(ssl=ssl.create_default_context())
    retry = ExponentialRetry(attempts=3)
    async with RetryClient(connector=connector, retry_options=retry) as session:
        sem = asyncio.Semaphore(CONCURRENCY)
        async def bound(s):
            async with sem:
                await fetch_seat(session, s)
        tasks = [bound(s) for s in shows]
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Seatmaps"):
            await f

# ================= MERGING AND DEDUPLICATION =================

def merge_show_metadata(old: Optional[Dict], new: Dict) -> Dict:
    """
    Merge two show dictionaries, preferring the one with better data.
    Rules:
    - Prefer show with successful seatmap (no error) over one with error.
    - If both have seatmap, prefer higher totalSeatSold.
    - Keep the best language/format/metadata.
    """
    if old is None:
        return new.copy()

    old_has_seatmap = "totalSeatSold" in old and "error" not in old
    new_has_seatmap = "totalSeatSold" in new and "error" not in new

    # If one has error and the other doesn't, pick the successful one
    if old_has_seatmap and not new_has_seatmap:
        chosen = old.copy()
    elif new_has_seatmap and not old_has_seatmap:
        chosen = new.copy()
    elif old_has_seatmap and new_has_seatmap:
        # Both have seatmap, compare sold
        if new.get("totalSeatSold", 0) > old.get("totalSeatSold", 0):
            chosen = new.copy()
        else:
            chosen = old.copy()
    else:
        # Neither has seatmap, or both have errors
        # Prefer the one with more complete metadata (language not Unknown, etc.)
        # Fallback: keep old
        chosen = old.copy()
        # But if new has better language, update fields
        if new.get("language") != "Unknown" and old.get("language") == "Unknown":
            chosen["language"] = new["language"]
        # Similarly format
        if new.get("format") != "Standard" and old.get("format") == "Standard":
            chosen["format"] = new["format"]
        # Keep error if present (new might have fresh error, but we keep old error if exists)
        if "error" in new and "error" not in chosen:
            chosen["error"] = new["error"]

    # Ensure we have the latest metadata from new if new is better
    # For fields like movie_title, theater_name, etc., we can take from new if it has non-empty
    for field in ["movie_title", "theater_name", "city", "state", "chainName", "format", "language"]:
        if new.get(field) and not chosen.get(field):
            chosen[field] = new[field]
        elif new.get(field) and chosen.get(field) and new[field] != "Unknown" and chosen[field] == "Unknown":
            chosen[field] = new[field]

    # Recalculate occupancy and gross based on chosen sold/total/price
    if "totalSeatSold" in chosen and "totalSeatCount" in chosen:
        sold = chosen["totalSeatSold"]
        total = chosen["totalSeatCount"]
        if total > 0:
            chosen["occupancy"] = round((sold / total) * 100, 2)
        else:
            chosen["occupancy"] = 0.0
        price = chosen.get("adultTicketPrice", 0.0)
        chosen["grossRevenueUSD"] = round(price * sold, 2)
    return chosen

def deduplicate_shows(shows: List[Dict]) -> Dict[str, Dict]:
    """
    Deduplicate list of shows by showtime_id, merging metadata.
    Returns dict {showtime_id: merged_show}.
    """
    unique = {}
    for s in shows:
        sid = str(s.get("showtime_id"))
        if not sid:
            continue
        if sid in unique:
            unique[sid] = merge_show_metadata(unique[sid], s)
        else:
            unique[sid] = s.copy()
    return unique

# ================= GITHUB API HELPERS =================

def github_get_file(path: str) -> Tuple[Optional[str], Optional[str]]:
    """Return (content_as_string, sha) if file exists, else (None, None)."""
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        data = resp.json()
        content = base64.b64decode(data["content"]).decode("utf-8")
        return content, data["sha"]
    elif resp.status_code == 404:
        return None, None
    else:
        raise Exception(f"GitHub GET error {resp.status_code}: {resp.text}")

def github_put_file(path: str, content: str, sha: Optional[str] = None) -> bool:
    """Create or update a file. Returns True on success."""
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    payload = {
        "message": f"Update {path}",
        "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
        "sha": sha,
    }
    resp = requests.put(url, headers=headers, json=payload)
    if resp.status_code in (200, 201):
        return True
    else:
        raise Exception(f"GitHub PUT error {resp.status_code}: {resp.text}")

# ================= LOAD / SAVE ADVANCE HELPERS (remote) =================

def load_existing_advance_file(date_obj: date) -> Tuple[Dict[str, Dict], List[Dict]]:
    """
    Load existing advance file from remote repository.
    Returns a dict: showtime_id -> show dict, and a list of errors.
    """
    year = date_obj.strftime("%Y")
    filename = date_obj.strftime("%d-%m.json")
    path = f"usa-advance/{year}/{filename}"
    content, _ = github_get_file(path)
    shows = {}
    errors = []
    if content:
        try:
            data = json.loads(content)
            if "shows" in data and isinstance(data["shows"], list):
                for arr in data["shows"]:
                    if len(arr) >= 15:
                        d = {
                            "showtime_id": arr[0],
                            "date": arr[1],
                            "format": arr[2],
                            "language": arr[3],
                            "movie_title": arr[4],
                            "movie_id": arr[5],
                            "theater_name": arr[6],
                            "city": arr[7],
                            "state": arr[8],
                            "chainName": arr[9],
                            "totalSeatSold": arr[10],
                            "totalSeatCount": arr[11],
                            "occupancy": arr[12],
                            "adultTicketPrice": arr[13],
                            "grossRevenueUSD": arr[14],
                        }
                        shows[str(arr[0])] = d
        except Exception as e:
            print(f"⚠️ Could not parse remote advance file {path}: {e}")

    # Load errors file (if exists)
    error_path = f"usa-advance/{year}/{date_obj.strftime('%d-%m')}_errors.json"
    err_content, _ = github_get_file(error_path)
    if err_content:
        try:
            err_data = json.loads(err_content)
            errors = err_data.get("errors", [])
        except Exception:
            pass
    return shows, errors

def write_advance_file(date_obj: date, merged_dict: Dict[str, Dict], error_shows: List[Dict]) -> None:
    """
    Write merged shows to remote usa-advance/YYYY/DD-MM.json,
    errors to DD-MM_errors.json, logs to DD-MM_logs.json.
    """
    if not merged_dict and not error_shows:
        print(f"No data for {date_obj}, skipping.")
        return

    shows = list(merged_dict.values())

    # Build compact list (same order as before)
    compact = []
    for s in shows:
        compact.append([
            s.get("showtime_id"),
            s.get("date"),
            s.get("format", "Standard"),
            s.get("language", "Unknown"),
            s.get("movie_title", "Unknown"),
            s.get("movie_id"),
            s.get("theater_name"),
            s.get("city"),
            s.get("state"),
            s.get("chainName"),
            s.get("totalSeatSold", 0),
            s.get("totalSeatCount", 0),
            s.get("occupancy", 0.0),
            s.get("adultTicketPrice", 0.0),
            s.get("grossRevenueUSD", 0.0),
        ])

    # Movie summary (for main file)
    movie_summary = defaultdict(lambda: {
        "shows": 0,
        "tickets": 0,
        "seats": 0,
        "gross": 0.0,
        "occupancy_sum": 0.0,
    })
    for s in shows:
        if "error" in s:
            continue
        movie_id = s.get("movie_id")
        movie_title = s.get("movie_title", "Unknown")
        key = (movie_id, movie_title)
        summary = movie_summary[key]
        summary["shows"] += 1
        summary["tickets"] += s.get("totalSeatSold", 0)
        summary["seats"] += s.get("totalSeatCount", 0)
        summary["gross"] += s.get("grossRevenueUSD", 0)
        summary["occupancy_sum"] += s.get("occupancy", 0.0)

    summary_list = []
    for (movie_id, movie_title), data in sorted(movie_summary.items(), key=lambda x: x[1]["gross"], reverse=True):
        occupancy_avg = round(data["occupancy_sum"] / data["shows"], 2) if data["shows"] else 0.0
        summary_list.append([
            movie_title,
            movie_id,
            data["shows"],
            round(data["gross"], 2),
            occupancy_avg,
            data["tickets"],
            data["seats"],
        ])

    output = {
        "shows": compact,
        "summary": summary_list
    }

    year = date_obj.strftime("%Y")
    base_path = f"usa-advance/{year}"

    # 1. Main file
    filename = date_obj.strftime("%d-%m.json")
    path = f"{base_path}/{filename}"
    _, sha = github_get_file(path)
    github_put_file(path, json.dumps(output, separators=(',', ':')), sha)
    print(f"💾 Saved {len(compact)} shows to {path}")

    # 2. Errors file
    error_path = f"{base_path}/{date_obj.strftime('%d-%m')}_errors.json"
    _, sha = github_get_file(error_path)
    error_payload = {
        "last_updated": datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %I:%M:%S %p"),
        "errors": error_shows if error_shows else []
    }
    github_put_file(error_path, json.dumps(error_payload, indent=2, ensure_ascii=False), sha)
    print(f"⚠️ Error file saved to {error_path}")

    # 3. Logs file (per‑movie logs)
    logs_path = f"{base_path}/{date_obj.strftime('%d-%m')}_logs.json"
    existing_logs = []
    content, sha = github_get_file(logs_path)
    if content:
        try:
            existing_logs = json.loads(content)
            if not isinstance(existing_logs, list):
                existing_logs = []
        except Exception:
            existing_logs = []

    # Build per‑movie log entries
    movie_logs = []
    for (movie_id, movie_title), data in sorted(movie_summary.items(), key=lambda x: x[1]["gross"], reverse=True):
        occupancy_avg = round(data["occupancy_sum"] / data["shows"], 2) if data["shows"] else 0.0
        movie_logs.append({
            "movie_id": movie_id,
            "movie_title": movie_title,
            "shows": data["shows"],
            "tickets_sold": data["tickets"],
            "total_seats": data["seats"],
            "gross_usd": round(data["gross"], 2),
            "avg_occupancy": occupancy_avg,
        })

    log_entry = {
        "time": datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %I:%M:%S %p"),
        "errors_count": len(error_shows) if error_shows else 0,
        "movies": movie_logs,
    }
    existing_logs.append(log_entry)
    github_put_file(logs_path, json.dumps(existing_logs, indent=2, ensure_ascii=False), sha)
    print(f"📝 Per‑movie log entries appended to {logs_path}")

# ================= DATE PLANNING =================

def build_date_filter_map() -> Tuple[Dict[date, Optional[Set[int]]], Dict[Tuple[date, int], str]]:
    """
    Returns:
      movie_filter: dict[date] -> set of movie_ids or None (all movies)
      extra_langs_map: dict[(date, movie_id)] -> str ("all", "english", "unknown")
    """
    movie_filter = {}
    extra_langs_map = {}
    custom_entries = []  # store (movie_id, date, extra_rule, apply_to_all)

    # 1. Tomorrow if enabled
    if FETCH_TOMORROW:
        eastern = ZoneInfo("America/New_York")
        tomorrow = (datetime.now(eastern) + timedelta(days=1)).date()
        movie_filter[tomorrow] = None

    # 2. All dates from SCRAPE_DATES
    for d in SCRAPE_DATES:
        movie_filter[d] = None

    # 3. Collect custom movies
    for custom in CUSTOM_MOVIES:
        movie_id = custom.get("movie_id")
        d = custom.get("date")
        extra = custom.get("add_extra_langs_shows")
        apply_to_all = custom.get("extra_langs_for_all_dates", False)
        if movie_id and d:
            # Add date to filter
            if d not in movie_filter:
                movie_filter[d] = {movie_id}
            elif movie_filter[d] is not None:
                movie_filter[d].add(movie_id)
            # Store for later processing of extra rules
            if extra in ("all", "english", "unknown"):
                custom_entries.append((movie_id, d, extra, apply_to_all))

    # Now we have all dates. Apply extra rules.
    for movie_id, d, extra, apply_to_all in custom_entries:
        if apply_to_all:
            # Apply to every date in movie_filter
            for date_key in movie_filter.keys():
                # Add movie to filter for this date if not None
                if movie_filter[date_key] is not None:
                    movie_filter[date_key].add(movie_id)
                # Set extra rule
                extra_langs_map[(date_key, movie_id)] = extra
        else:
            # Apply only to its own date
            extra_langs_map[(d, movie_id)] = extra
            # (movie already added to filter for its own date)

    return movie_filter, extra_langs_map

# ================= MAIN =================

def main() -> None:
    movie_filter, extra_langs_map = build_date_filter_map()
    if not movie_filter:
        print("No dates to scrape. Enable FETCH_TOMORROW, set SCRAPE_DATES, or add CUSTOM_MOVIES.")
        return

    print("📅 Scraping plan:")
    for d, filt in sorted(movie_filter.items()):
        if filt is None:
            print(f"  {d.strftime('%Y-%m-%d')}: ALL movies (target languages)")
        else:
            print(f"  {d.strftime('%Y-%m-%d')}: movies {filt}")
        # Show extra language rules for that date
        for (date_key, movie_id), langs in extra_langs_map.items():
            if date_key == d:
                print(f"    -> Extra langs for movie {movie_id}: {langs}")

    # Load zip codes from local file
    if not os.path.exists(ZIP_FILE):
        print(f"❌ Missing {ZIP_FILE}")
        return
    zipcodes = open(ZIP_FILE).read().splitlines()
    print(f"✅ {len(zipcodes)} ZIPs loaded.")

    for scrape_date, filt in sorted(movie_filter.items()):
        date_str = scrape_date.strftime("%Y-%m-%d")
        print(f"\n=== Processing date: {date_str} ===")

        # 1. Load existing advance data from remote
        existing_shows, existing_errors = load_existing_advance_file(scrape_date)
        print(f"📂 Loaded {len(existing_shows)} existing shows from advance data (remote).")

        # 2. Scrape fresh showtimes
        raw_shows = scrape_all_shows_for_date(zipcodes, date_str)
        if DEBUG:
            print(f"Raw shows scraped: {len(raw_shows)}")
            # Language distribution
            lang_counts = defaultdict(int)
            for s in raw_shows:
                lang_counts[s.get("language", "Unknown")] += 1
            print("Language distribution (raw):", dict(lang_counts))

        # 3. Deduplicate raw shows intelligently
        unique_raw = deduplicate_shows(raw_shows)
        if DEBUG:
            print(f"After dedup: {len(unique_raw)} unique showtime_ids")

        # 4. Apply language rules and movie filter
        filtered = []
        for sid, s in unique_raw.items():
            mid = s.get("movie_id")
            lang = s.get("language")
            extra = extra_langs_map.get((scrape_date, mid))

            # Determine if this show passes language filter
            lang_ok = False
            if extra == "all":
                lang_ok = True
            elif extra == "english":
                lang_ok = (lang in TARGET_LANGUAGES or lang == "English")
            elif extra == "unknown":
                lang_ok = (lang in TARGET_LANGUAGES or lang == "Unknown")
            else:
                lang_ok = (lang in TARGET_LANGUAGES)

            if not lang_ok:
                continue

            # Apply movie filter
            if filt is not None and mid not in filt:
                continue

            filtered.append(s)

        if DEBUG:
            print(f"After language + movie filter: {len(filtered)} shows")
            # Language distribution after filter
            lang_counts = defaultdict(int)
            for s in filtered:
                lang_counts[s.get("language", "Unknown")] += 1
            print("Language distribution (filtered):", dict(lang_counts))

        # 5. Merge discovered shows into existing dict
        merged_dict = existing_shows.copy()
        for s in filtered:
            sid = str(s.get("showtime_id"))
            merged_dict[sid] = merge_show_metadata(merged_dict.get(sid), s)

        # 6. Determine which shows need seatmap fetch
        to_fetch = []
        for sid, s in merged_dict.items():
            # Fetch if we don't have seatmap data (no totalSeatSold) or we have an error
            if "totalSeatSold" not in s or "error" in s:
                to_fetch.append(s)

        if DEBUG:
            print(f"Shows needing seatmap fetch: {len(to_fetch)}")

        if to_fetch:
            # 7. Fetch seatmaps for those shows
            asyncio.run(run_seatmap_fetch(to_fetch))

            # 8. Update merged_dict with fetched results (merge again)
            for s in to_fetch:
                sid = str(s.get("showtime_id"))
                merged_dict[sid] = merge_show_metadata(merged_dict.get(sid), s)

        # 9. Separate errors
        error_shows = [s for s in merged_dict.values() if "error" in s]
        if DEBUG:
            successful = len(merged_dict) - len(error_shows)
            print(f"Seatmap success: {successful}, failures: {len(error_shows)}")
            print(f"Total shows in merged dict: {len(merged_dict)}")

        # 10. Write merged data to remote
        write_advance_file(scrape_date, merged_dict, error_shows)

    print("\n✅ All dates processed.")

if __name__ == "__main__":
    main()
