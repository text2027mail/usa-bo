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

# ================= CONFIGURATION =================

TARGET_LANGUAGES = ["Hindi", "Tamil", "Telugu", "Malayalam", "Kannada"]

# --- Fetch tomorrow's shows (all movies with target languages) ---
FETCH_TOMORROW = True

# --- Dates for which we want ALL movies (target languages) ---
SCRAPE_DATES = [
#     date(2026, 9, 4),
]

# --- Custom movies with extra language options ---
CUSTOM_MOVIES = [
#   {"movie_id": 244612, "date": date(2026, 8, 25)},
#   {"movie_id": 244612, "date": date(2026, 8, 26), "add_extra_langs_shows": "english"},
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
    "Hindi", "Tamil", "Telugu", "Kannada",
    "Malayalam", "Punjabi", "Gujarati", "Marathi", "Bengali", "English"
]
FORMAT_KEYWORDS = [
    "RPX", "D-Box", "IMAX", "EMX", "Sony Digital Cinema",
    "4DX", "ScreenX", "Cinemark XD", "Dolby Cinema"
]

# ============================================================
# MOVIE TITLE MERGE / ALIAS CONFIGURATION
# ============================================================

# NEW: You can now specify a custom title for each master movie.
# Format:
#   master_id: {
#       "ids": [list_of_aliases],
#       "title": "Your Custom Title"   # optional
#   }
# Or keep the old format: master_id: [list_of_aliases] (no custom title)

MERGE_MOVIE_IDS = {
    244612: {
        "ids": [244612, 246785],
        "title": "Toxic (2026)"   # <-- Custom title
    },
    # 250001: [250001, 250002, 250003],   # old format still works
}

DEBUG_MOVIE_TITLE_MERGE = True   # set True for verbose alias logging

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

def extract_language(amenities):
    lang_priority = []
    for item in amenities:
        lowered = item.lower()
        for lang in KNOWN_LANGUAGES:
            if f"{lang.lower()} language" in lowered:
                return lang
            if lang.lower() in lowered:
                lang_priority.append((lang, lowered.find(lang.lower())))
    if lang_priority:
        lang_priority.sort(key=lambda x: x[1])
        return lang_priority[0][0]
    return "Unknown"

def extract_format(amenities, default_format):
    for keyword in FORMAT_KEYWORDS:
        if any(keyword.lower() in a.lower() for a in amenities):
            return keyword
    return default_format

def prepare_showtimes(movie):
    out = []

    movie_title = movie.get("title", "Unknown")
    movie_id = movie.get("id")

    for variant in movie.get("variants", []):

        # Fandango's official format field
        fmt = (
            variant.get("filmFormatHeader")
            or variant.get("formatName")
            or "Standard"
        )

        for ag in variant.get("amenityGroups", []):

            amenities = [
                a.get("name", "")
                for a in ag.get("amenities", [])
            ]

            lang = extract_language(amenities)

            for show in ag.get("showtimes", []):

                sid = show.get("id")
                if not sid:
                    continue

                out.append({
                    "showtime_id": sid,
                    "date": show.get("ticketingDate"),
                    "format": fmt,
                    "language": lang,
                    "movie_title": movie_title,
                    "movie_id": movie_id,
                })

    return out

# ================= THEATER SCRAPER (MULTIPROCESSING) =================

def get_theaters(zip_code, date_str):
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
        print(f"❌ Error fetching theaters for ZIP {zip_code}: {e}")
    return {}

def process_zip(args):
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

def scrape_all_shows_for_date(zip_list, date_str):
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

async def fetch_seat(session, show):
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
    except Exception as e:
        show["error"] = {"exception": str(e)}

async def run_seatmap_fetch(shows):
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

# ================= MERGING LOGIC =================

def merge_show(old, new):

    if old is None:
        return new

    old_error = "error" in old
    new_error = "error" in new

    # both failed
    if old_error and new_error:
        return old

    # old failed, new succeeded
    if old_error and not new_error:
        return new

    # old succeeded, new failed
    if not old_error and new_error:
        return old

    # both succeeded

    if new.get("totalSeatSold", 0) >= old.get("totalSeatSold", 0):
        return new

    return old

# ================= GITHUB API HELPERS =================

def github_get_file(path):
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

def github_put_file(path, content, sha=None):
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

# ============================================================
# MOVIE TITLE ALIAS / MERGE HELPERS (UPDATED)
# ============================================================

MASTER_MOVIE_TITLES = {}
MASTER_CUSTOM_TITLES = {}   # <-- NEW: store custom titles from config

def build_title_master_map(merge_config):
    """
    Validates and builds master_map and master_set from MERGE_MOVIE_IDS.
    Also fills MASTER_CUSTOM_TITLES with any custom titles.
    Returns (master_map, master_set).
    """
    master_map = {}
    master_set = set()
    alias_to_master = {}

    for master, value in merge_config.items():
        # Parse the value – can be list or dict
        if isinstance(value, list):
            aliases = list(set(value))
            custom_title = None
        elif isinstance(value, dict):
            aliases = list(set(value.get("ids", [])))
            custom_title = value.get("title")
        else:
            raise ValueError(f"Invalid value for master {master}: {value}")

        # Ensure master is in the alias list
        if master not in aliases:
            aliases.append(master)

        # Validate aliases
        for alias in aliases:
            if alias in alias_to_master and alias_to_master[alias] != master:
                raise ValueError(
                    f"Alias {alias} already assigned to master {alias_to_master[alias]}, cannot also assign to {master}"
                )
            alias_to_master[alias] = master
            master_map[alias] = master

        master_set.add(master)

        # Store custom title if provided
        if custom_title:
            MASTER_CUSTOM_TITLES[master] = custom_title

    return master_map, master_set

# Build global maps
MOVIE_TITLE_MASTER_MAP, MASTER_IDS_SET = build_title_master_map(MERGE_MOVIE_IDS)

def get_master_id(movie_id):
    """Return the master ID for the given movie_id."""
    return MOVIE_TITLE_MASTER_MAP.get(movie_id, movie_id)

def update_master_title(movie_id, title):
    """Update master title if movie_id is a master ID, but never override a custom title."""
    if movie_id in MASTER_IDS_SET and title:
        # If a custom title exists for this master, keep it
        if movie_id in MASTER_CUSTOM_TITLES:
            if DEBUG_MOVIE_TITLE_MERGE:
                print(f"🔒 Keeping custom title for {movie_id}: '{MASTER_CUSTOM_TITLES[movie_id]}'")
            return
        # Otherwise update normally
        if DEBUG_MOVIE_TITLE_MERGE:
            old = MASTER_MOVIE_TITLES.get(movie_id)
            if old and old != title:
                print(f"🔁 Updating master title for {movie_id}: '{old}' -> '{title}'")
        MASTER_MOVIE_TITLES[movie_id] = title

def get_canonical_movie_title(movie_id, current_title=None):
    """
    Return the canonical title for the given movie_id.
    If master title is known, use it; otherwise return current_title.
    """
    master_id = MOVIE_TITLE_MASTER_MAP.get(movie_id, movie_id)
    if master_id in MASTER_MOVIE_TITLES:
        return MASTER_MOVIE_TITLES[master_id]
    return current_title if current_title is not None else ""

def normalize_show_title(show):
    """In-place normalization of movie_title."""
    movie_id = show.get("movie_id")
    if movie_id is not None:
        canonical = get_canonical_movie_title(movie_id, show.get("movie_title"))
        show["movie_title"] = canonical

def expand_movie_ids(movie_ids_set):
    """Expand a set of movie IDs to include all aliases of their master groups."""
    expanded = set()
    for mid in movie_ids_set:
        master = get_master_id(mid)
        for alias, m in MOVIE_TITLE_MASTER_MAP.items():
            if m == master:
                expanded.add(alias)
    return expanded

# ================= LOAD / SAVE ADVANCE HELPERS (remote) =================

def load_existing_advance_file(date_obj):
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
                        # Register master title from existing data
                        update_master_title(d["movie_id"], d["movie_title"])
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

def write_advance_file(date_obj, merged_dict, error_shows):
    """
    Write merged shows to remote usa-advance/YYYY/DD-MM.json,
    errors to DD-MM_errors.json, logs to DD-MM_logs.json.
    """
    if not merged_dict and not error_shows:
        print(f"No data for {date_obj}, skipping.")
        return

    shows = list(merged_dict.values())

    # Deduplicate
    seen = set()
    unique = []
    for s in shows:
        sid = str(s.get("showtime_id"))
        if sid not in seen:
            seen.add(sid)
            unique.append(s)

    # Build compact list
    compact = []
    for s in unique:
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
    for s in unique:
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
    print(f"💾 Saved {len(unique)} shows to {path}")

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

def build_date_filter_map():

    movie_filter = {}

    # Tomorrow
    if FETCH_TOMORROW:
        eastern = ZoneInfo("America/New_York")
        tomorrow = (datetime.now(eastern) + timedelta(days=1)).date()
        movie_filter[tomorrow] = None

    # Manual scrape dates
    for d in SCRAPE_DATES:
        movie_filter.setdefault(d, None)

    # ------------------------------------------------
    # FIRST add every custom movie date (expanded to all aliases)
    # ------------------------------------------------

    for custom in CUSTOM_MOVIES:

        d = custom.get("date")
        mid = custom.get("movie_id")

        if not d or not mid:
            continue

        expanded = expand_movie_ids({mid})

        if d not in movie_filter:

            movie_filter[d] = expanded

        elif movie_filter[d] is not None:

            movie_filter[d].update(expanded)

    # ------------------------------------------------
    # NOW create extra_langs_map, expanded to all aliases
    # ------------------------------------------------

    extra_langs_map = {}

    all_dates = list(movie_filter.keys())

    for custom in CUSTOM_MOVIES:

        movie_id = custom.get("movie_id")
        extra = custom.get("add_extra_langs_shows")

        if extra not in ("all","english","unknown"):
            continue

        # Get master for this movie
        master_id = get_master_id(movie_id)
        # All aliases under this master
        aliases = [aid for aid, m in MOVIE_TITLE_MASTER_MAP.items() if m == master_id]

        if custom.get("extra_langs_for_all_dates", False):
            dates_to_apply = all_dates
        else:
            dates_to_apply = [custom["date"]]

        for d in dates_to_apply:
            for alias in aliases:
                extra_langs_map[(d, alias)] = extra

    return movie_filter, extra_langs_map

# ================= MAIN =================

def main():
    # Apply custom titles upfront (NEW)
    for master, title in MASTER_CUSTOM_TITLES.items():
        MASTER_MOVIE_TITLES[master] = title
        if DEBUG_MOVIE_TITLE_MERGE:
            print(f"📌 Custom title set for master {master}: '{title}'")

    # --- NEW: Get today's date (Eastern Time) ---
    eastern = ZoneInfo("America/New_York")
    today = datetime.now(eastern).date()
    print(f"📅 Today's date (Eastern): {today}")

    movie_filter, extra_langs_map = build_date_filter_map()

    # --- NEW: Filter out dates that are not in the future (<= today) ---
    future_filter = {}
    for d, filt in movie_filter.items():
        if d > today:
            future_filter[d] = filt
        else:
            print(f"⏭️  Skipping date {d} – it is not in the future (today is {today})")
    movie_filter = future_filter

    # Also filter extra_langs_map to only future dates
    extra_langs_map = {
        (d, mid): rule
        for (d, mid), rule in extra_langs_map.items()
        if d in movie_filter
    }

    if not movie_filter:
        print("No future dates to scrape. Exiting.")
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

        # 3. Filter by language (respect extra_langs_map, now expanded)
        lang_filtered = []
        for s in raw_shows:
            mid = s.get("movie_id")
            lang = s.get("language")
            extra = extra_langs_map.get((scrape_date, mid))

            if extra == "all":
                # Include regardless of language
                lang_filtered.append(s)
            elif extra == "english":
                # Include if language is in TARGET_LANGUAGES or "English"
                if lang in TARGET_LANGUAGES or lang == "English":
                    lang_filtered.append(s)
            elif extra == "unknown":
                # Include if language is in TARGET_LANGUAGES or "Unknown"
                if lang in TARGET_LANGUAGES or lang == "Unknown":
                    lang_filtered.append(s)
            else:
                # Default: only TARGET_LANGUAGES
                if lang in TARGET_LANGUAGES:
                    lang_filtered.append(s)

        print(f"  Raw shows: {len(raw_shows)}, after language filter: {len(lang_filtered)}")

        # 4. Deduplicate fresh shows by showtime_id
        unique_fresh = {}
        for s in lang_filtered:
            sid = str(s.get("showtime_id"))
            if sid not in unique_fresh:
                unique_fresh[sid] = s
        lang_filtered = list(unique_fresh.values())

        from collections import Counter
        print()
        print("Language Counts")
        print(Counter(x["language"] for x in raw_shows))
        print(Counter(x["language"] for x in lang_filtered))
        print()
        print(f"  After dedup: {len(lang_filtered)}")

        # 5. Filter by movie_filter (if not None)
        if filt is not None:
            filtered = [s for s in lang_filtered if s.get("movie_id") in filt]
            print(f"  After movie filter (only {filt}): {len(filtered)}")
        else:
            filtered = lang_filtered
            print(f"  No movie filter (all target languages + extra langs): {len(filtered)}")

        if not filtered:
            print("  No shows match criteria. Skipping seatmap fetch.")
            # We keep existing data (do not overwrite)
            continue

        # 6. Register master titles from fresh shows (for master IDs)
        for s in filtered:
            update_master_title(s.get("movie_id"), s.get("movie_title"))

        # 7. Fetch seatmap data (modifies shows in-place)
        existing_ids = set(existing_shows.keys())
        print()
        print("=" * 70)
        print(f"Filtered discovered : {len(filtered)}")
        already = [s for s in filtered if str(s["showtime_id"]) in existing_ids]
        new = [s for s in filtered if str(s["showtime_id"]) not in existing_ids]
        print(f"Already in DB      : {len(already)}")
        print(f"New showtimes      : {len(new)}")
        print("=" * 70)
        print()

        asyncio.run(run_seatmap_fetch(filtered))

        # 8. Merge: start with existing shows
        merged_dict = existing_shows.copy()

        # STEP 1: Insert EVERY discovered show first.
        for fresh in filtered:
            sid = str(fresh["showtime_id"])
            if sid not in merged_dict:
                merged_dict[sid] = fresh

        # STEP 2: Merge seatmap results.
        for fresh in filtered:
            sid = str(fresh["showtime_id"])
            merged_dict[sid] = merge_show(merged_dict[sid], fresh)

        # 9. Final title normalization: apply canonical titles to all shows
        for sid, show in merged_dict.items():
            normalize_show_title(show)

        # 10. Build error list (and normalize their titles)
        error_shows = []
        for s in merged_dict.values():
            if "error" in s:
                # normalize the error show's title as well
                normalize_show_title(s)
                error_shows.append(s)

        print(f"  Successful shows: {len(merged_dict) - len(error_shows)}, Errors: {len(error_shows)}")

        # 11. Write merged data to remote
        write_advance_file(scrape_date, merged_dict, error_shows)

    print("\n✅ All dates processed.")

if __name__ == "__main__":
    main()
