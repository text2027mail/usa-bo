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

# ================= CONFIGURATION =================

# Target languages (case‑insensitive)
TARGET_LANGUAGES = ["Hindi", "Tamil", "Telugu", "Malayalam", "Kannada"]

# --- Fetch tomorrow's shows (all movies with target languages) ---
FETCH_TOMORROW = True

# --- Dates for which we want ALL movies (target languages) ---
SCRAPE_DATES = [
    # date(2026, 7, 20),
]

# --- Custom movies: list of {movie_id, date} ---
CUSTOM_MOVIES = [
{"movie_id": 243375, "date": date(2026, 7, 23)},
]

# --- File containing US zip codes (one per line) ---
ZIP_FILE = "zipcodes.txt"

# --- Fandango credentials ---
AUTHORIZATION_TOKEN = "<your-auth-token>"
SESSION_ID = "<your-session-id>"

# --- Render proxy URL ---
RENDER_SEATMAP_URL = "https://usa-render.onrender.com/api/seatmap"

# --- Concurrency ---
MAX_WORKERS = 50          # process pool for zip scanning
CONCURRENCY = 45          # async seatmap requests

# --- Language & format detection ---
KNOWN_LANGUAGES = [
    "English", "Hindi", "Tamil", "Telugu", "Kannada",
    "Malayalam", "Punjabi", "Gujarati", "Marathi", "Bengali"
]
FORMAT_KEYWORDS = [
    "RPX", "D-Box", "IMAX", "EMX", "Sony Digital Cinema",
    "4DX", "ScreenX", "Cinemark XD", "Dolby Cinema"
]

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

# ================= HEADER BUILDERS (exact same as boxoffice) =================

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

# ================= PARSERS (exact same as boxoffice) =================

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
            data = await resp.json()
            total = data.get("totalSeatCount", 0)
            available = data.get("totalAvailableSeatCount", 0)
            price = data.get("adultTicketPrice", 0.0)

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

# ================= MERGING LOGIC (same as boxoffice) =================

def merge_show(old, new):
    """
    Merge two show records.
    - If new has error, keep old entirely.
    - Otherwise, keep the higher totalSeatSold (to avoid losing sales).
    - Recompute derived fields based on chosen sold.
    """
    if not old:
        return new
    if "error" in new:
        return old  # keep old if new fetch failed

    # Determine which sold count to use (max)
    new_sold = new.get("totalSeatSold", 0)
    old_sold = old.get("totalSeatSold", 0)
    if new_sold > old_sold:
        chosen = new.copy()
        chosen_sold = new_sold
    else:
        chosen = old.copy()
        chosen_sold = old_sold

    # Update derived fields based on chosen sold
    total = chosen.get("totalSeatCount", 0)
    if total and total > 0:
        chosen["occupancy"] = round((chosen_sold / total) * 100, 2)
    else:
        chosen["occupancy"] = 0.0

    price = chosen.get("adultTicketPrice", 0.0)
    chosen["grossRevenueUSD"] = round(price * chosen_sold, 2)
    chosen["totalSeatSold"] = chosen_sold

    return chosen

# ================= LOAD / SAVE ADVANCE HELPERS =================

def load_existing_advance_file(date_obj):
    """
    Load existing advance file for the given date.
    Returns a dict: showtime_id -> show dict, and a list of errors (if any).
    """
    year = date_obj.strftime("%Y")
    filename = date_obj.strftime("%d-%m.json")
    filepath = os.path.join("usa-advance", year, filename)
    if not os.path.exists(filepath):
        return {}, []

    try:
        with open(filepath, "r") as f:
            data = json.load(f)
        shows = {}
        errors = []
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
        # Also load errors from errors file if exists
        error_file = os.path.join("usa-advance", year, f"{date_obj.strftime('%d-%m')}_errors.json")
        if os.path.exists(error_file):
            try:
                with open(error_file, "r") as f:
                    err_data = json.load(f)
                errors = err_data.get("errors", [])
            except Exception:
                pass
        return shows, errors
    except Exception as e:
        print(f"⚠️ Could not load existing advance file {filepath}: {e}")
        return {}, []

def write_advance_file(date_obj, merged_dict, error_shows):
    """
    Write merged shows to usa-advance/YYYY/DD-MM.json,
    and errors to DD-MM_errors.json, logs to DD-MM_logs.json.
    """
    if not merged_dict and not error_shows:
        print(f"No data for {date_obj}, skipping.")
        return

    # Convert merged dict to list
    shows = list(merged_dict.values())

    # Deduplicate by showtime_id (just in case)
    seen = set()
    unique = []
    for s in shows:
        sid = str(s.get("showtime_id"))
        if sid not in seen:
            seen.add(sid)
            unique.append(s)

    # Build compact show list
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

    # Movie-wise summary (only successful)
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
    dir_path = os.path.join("usa-advance", year)
    os.makedirs(dir_path, exist_ok=True)

    # Save main file
    filename = date_obj.strftime("%d-%m.json")
    filepath = os.path.join(dir_path, filename)
    with open(filepath, "w") as f:
        json.dump(output, f, separators=(',', ':'))

    print(f"💾 Saved {len(unique)} shows to {filepath}")

    # Save errors
    error_file = os.path.join(dir_path, f"{date_obj.strftime('%d-%m')}_errors.json")
    error_payload = {
        "last_updated": datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %I:%M:%S %p"),
        "errors": error_shows if error_shows else []
    }
    with open(error_file, "w") as f:
        json.dump(error_payload, f, indent=2, ensure_ascii=False)
    print(f"⚠️ Error file saved to {error_file}")

    # Save logs
    logs_file = os.path.join(dir_path, f"{date_obj.strftime('%d-%m')}_logs.json")
    total_gross = 0.0
    total_shows = 0
    total_sold = 0
    total_capacity = 0
    venues = set()

    for s in unique:
        if "error" in s:
            continue
        total_gross += s.get("grossRevenueUSD", 0.0)
        total_shows += 1
        total_sold += s.get("totalSeatSold", 0)
        total_capacity += s.get("totalSeatCount", 0)
        venues.add(s.get("theater_name"))

    avg_occupancy = round((total_sold / total_capacity) * 100, 2) if total_capacity else 0.0

    log_entry = {
        "time": datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %I:%M:%S %p"),
        "total_gross_usd": round(total_gross, 2),
        "total_shows": total_shows,
        "avg_occupancy": avg_occupancy,
        "tickets_sold": total_sold,
        "unique_venues": len(venues),
        "errors_count": len(error_shows) if error_shows else 0,
    }

    existing_logs = []
    if os.path.exists(logs_file):
        try:
            existing_logs = json.load(open(logs_file))
            if not isinstance(existing_logs, list):
                existing_logs = []
        except Exception:
            existing_logs = []
    existing_logs.append(log_entry)
    with open(logs_file, "w") as f:
        json.dump(existing_logs, f, indent=2, ensure_ascii=False)

    print(f"📝 Log entry appended to {logs_file}")

# ================= DATE PLANNING =================

def build_date_filter_map():
    """
    Returns a dict: date -> set of movie_ids (or None for all movies).
    If multiple sources give the same date, the most inclusive wins:
      - if any source says "all movies" (None), set to None.
      - otherwise merge sets of movie_ids.
    """
    date_filter = {}

    # 1. Tomorrow if enabled
    if FETCH_TOMORROW:
        eastern = ZoneInfo("America/New_York")
        tomorrow = (datetime.now(eastern) + timedelta(days=1)).date()
        date_filter[tomorrow] = None  # all movies

    # 2. All dates from SCRAPE_DATES
    for d in SCRAPE_DATES:
        date_filter[d] = None

    # 3. Custom movies
    for custom in CUSTOM_MOVIES:
        movie_id = custom.get("movie_id")
        d = custom.get("date")
        if not movie_id or not d:
            continue
        if d not in date_filter:
            date_filter[d] = {movie_id}
        elif date_filter[d] is not None:
            # already a set, add
            date_filter[d].add(movie_id)
        # else: if it's None, keep None (all movies already)

    return date_filter

# ================= MAIN =================

def main():
    date_filter = build_date_filter_map()
    if not date_filter:
        print("No dates to scrape. Enable FETCH_TOMORROW, set SCRAPE_DATES, or add CUSTOM_MOVIES.")
        return

    print("📅 Scraping plan:")
    for d, filt in sorted(date_filter.items()):
        if filt is None:
            print(f"  {d.strftime('%Y-%m-%d')}: ALL movies (target languages)")
        else:
            print(f"  {d.strftime('%Y-%m-%d')}: movies {filt}")

    # Load zip codes
    if not os.path.exists(ZIP_FILE):
        print(f"❌ Missing {ZIP_FILE}")
        return
    zipcodes = open(ZIP_FILE).read().splitlines()
    print(f"✅ {len(zipcodes)} ZIPs loaded.")

    for scrape_date, movie_filter in sorted(date_filter.items()):
        date_str = scrape_date.strftime("%Y-%m-%d")
        print(f"\n=== Processing date: {date_str} ===")

        # 1. Load existing advance data
        existing_shows, existing_errors = load_existing_advance_file(scrape_date)
        print(f"📂 Loaded {len(existing_shows)} existing shows from advance data.")

        # 2. Scrape fresh showtimes
        raw_shows = scrape_all_shows_for_date(zipcodes, date_str)

        # 3. Filter by target languages
        lang_filtered = [s for s in raw_shows if s.get("language") in TARGET_LANGUAGES]
        print(f"  Raw shows: {len(raw_shows)}, after language filter: {len(lang_filtered)}")

        # 4. Deduplicate fresh shows by showtime_id
        unique_fresh = {}
        for s in lang_filtered:
            sid = str(s.get("showtime_id"))
            if sid not in unique_fresh:
                unique_fresh[sid] = s
        lang_filtered = list(unique_fresh.values())
        print(f"  After dedup: {len(lang_filtered)}")

        # 5. Filter by movie_filter (if not None)
        if movie_filter is not None:
            filtered = [s for s in lang_filtered if s.get("movie_id") in movie_filter]
            print(f"  After movie filter (only {movie_filter}): {len(filtered)}")
        else:
            filtered = lang_filtered
            print(f"  No movie filter (all target languages): {len(filtered)}")

        if not filtered:
            print("  No shows match criteria. Skipping seatmap fetch.")
            # Still need to keep existing data? We'll just keep existing.
            # Save merged (which is just existing) - but if we do nothing, we keep the file.
            # However, we might want to update errors/logs? We'll skip.
            continue

        # 6. Fetch seatmap data (modifies shows in-place)
        asyncio.run(run_seatmap_fetch(filtered))

        # 7. Merge: start with existing shows
        merged_dict = existing_shows.copy()

        # Process fresh shows
        for fresh in filtered:
            sid = str(fresh.get("showtime_id"))
            if sid in merged_dict:
                merged_dict[sid] = merge_show(merged_dict[sid], fresh)
            else:
                if "error" not in fresh:
                    merged_dict[sid] = fresh
                # else: skip because no data

        # Separate errors from merged
        error_shows = [s for s in merged_dict.values() if "error" in s]
        print(f"  Successful shows: {len(merged_dict) - len(error_shows)}, Errors: {len(error_shows)}")

        # 8. Write merged data
        write_advance_file(scrape_date, merged_dict, error_shows)

    print("\n✅ All dates processed.")

if __name__ == "__main__":
    main()
