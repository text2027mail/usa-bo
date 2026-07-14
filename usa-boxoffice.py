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
from datetime import datetime, date
from zoneinfo import ZoneInfo
from collections import defaultdict

# ================= CONFIGURATION =================
TARGET_LANGUAGES = ["Hindi", "Tamil", "Telugu", "Malayalam", "Kannada"]
ZIP_FILE = "zipcodes.txt"
MAX_WORKERS = 50
CONCURRENCY = 45
RENDER_SEATMAP_URL = "https://usa-render.onrender.com/api/seatmap"

# Authorization and Session (unchanged from original scraper)
AUTHORIZATION_TOKEN = "<your-auth-token>"
SESSION_ID = "<your-session-id>"

KNOWN_LANGUAGES = [
    "English", "Hindi", "Tamil", "Telugu", "Kannada",
    "Malayalam", "Punjabi", "Gujarati", "Marathi", "Bengali"
]
FORMAT_KEYWORDS = [
    "RPX", "D-Box", "IMAX", "EMX", "Sony Digital Cinema",
    "4DX", "ScreenX", "Cinemark XD", "Dolby Cinema"
]

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

# -------- Header builders (exact same as perfectheadersandmethod) ----------
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

# -------- Parsers (exact same) ----------
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

# -------- Theater scraping (multiprocessing) ----------
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

# -------- Seatmap fetching (async) using Render proxy ----------
async def fetch_seat(session, show):
    sid = str(show["showtime_id"])
    params = {"showtime_id": sid}
    headers = get_seatmap_headers()   # needed, should-not be removed
    try:
        async with session.get(RENDER_SEATMAP_URL, params=params, headers=headers, timeout=10) as resp:
            # 1. HTTP-level error
            if resp.status != 200:
                show["error"] = {"status": resp.status}
                return

            # 2. Read plain text response
            text = await resp.text()
            text = text.strip()

            # 3. Error indicator: "e" + status code (e.g., e404)
            if text.startswith('e'):
                try:
                    status_code = int(text[1:])
                except ValueError:
                    status_code = 500
                show["error"] = {"status": status_code}
                return

            # 4. Success: three comma-separated values
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

            # 5. Sanity checks
            if total == 0:
                show["error"] = {"status": 500, "reason": "No seats"}
                return
            if price == 0.0:
                show["error"] = {"status": 500, "reason": "Ticket price 0"}
                return

            # 6. Compute derived fields
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

# -------- Merging logic: keep higher sold, prefer old on error ----------
def merge_show(old, new):
    if not old:
        return new
    if "error" in new:
        return old

    new_sold = new.get("totalSeatSold", 0)
    old_sold = old.get("totalSeatSold", 0)
    if new_sold > old_sold:
        chosen = new.copy()
        chosen_sold = new_sold
    else:
        chosen = old.copy()
        chosen_sold = old_sold

    total = chosen.get("totalSeatCount", 0)
    if total and total > 0:
        chosen["occupancy"] = round((chosen_sold / total) * 100, 2)
    else:
        chosen["occupancy"] = 0.0

    price = chosen.get("adultTicketPrice", 0.0)
    chosen["grossRevenueUSD"] = round(price * chosen_sold, 2)
    chosen["totalSeatSold"] = chosen_sold

    return chosen

# -------- Load / save helpers ----------
def load_advance_file(date_obj):
    year = date_obj.strftime("%Y")
    filename = date_obj.strftime("%d-%m.json")
    filepath = os.path.join("usa-advance", year, filename)
    if not os.path.exists(filepath):
        return []
    try:
        with open(filepath, "r") as f:
            data = json.load(f)
        if "shows" in data and isinstance(data["shows"], list):
            show_dicts = []
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
                    show_dicts.append(d)
            return show_dicts
    except Exception as e:
        print(f"⚠️ Failed to load advance file {filepath}: {e}")
    return []

def load_boxoffice_file(date_obj):
    year = date_obj.strftime("%Y")
    filename = date_obj.strftime("%d-%m.json")
    filepath = os.path.join("usa-boxoffice", year, filename)
    if not os.path.exists(filepath):
        return []
    try:
        with open(filepath, "r") as f:
            data = json.load(f)
        if "shows" in data and isinstance(data["shows"], list):
            show_dicts = []
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
                    show_dicts.append(d)
            return show_dicts
    except Exception as e:
        print(f"⚠️ Failed to load boxoffice file {filepath}: {e}")
    return []

def save_boxoffice_file(date_obj, shows_dict, error_shows=None):
    if not shows_dict:
        print(f"No shows for {date_obj}, skipping boxoffice file.")
        return

    seen = set()
    unique = []
    for s in shows_dict:
        sid = str(s.get("showtime_id"))
        if sid not in seen:
            seen.add(sid)
            unique.append(s)

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
    dir_path = os.path.join("usa-boxoffice", year)
    os.makedirs(dir_path, exist_ok=True)

    filename = date_obj.strftime("%d-%m.json")
    filepath = os.path.join(dir_path, filename)

    with open(filepath, "w") as f:
        json.dump(output, f, separators=(',', ':'))

    # Save error file
    error_file = os.path.join(dir_path, f"{date_obj.strftime('%d-%m')}_errors.json")
    error_payload = {
        "last_updated": datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %I:%M:%S %p"),
        "errors": error_shows if error_shows else []
    }
    with open(error_file, "w") as f:
        json.dump(error_payload, f, indent=2, ensure_ascii=False)

    # Save logs.json
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
    print(f"💾 Saved {len(unique)} shows to {filepath}")

# -------- Main ----------
def main():
    eastern = ZoneInfo("America/New_York")
    today = datetime.now(eastern).date()
    date_str = today.strftime("%Y-%m-%d")
    print(f"📅 Box Office for today: {date_str}")

    # 1. Load advance data
    advance_shows = load_advance_file(today)
    print(f"📂 Loaded {len(advance_shows)} shows from advance data.")

    # 2. Load existing boxoffice data
    boxoffice_shows = load_boxoffice_file(today)
    print(f"📂 Loaded {len(boxoffice_shows)} shows from existing boxoffice data.")

    # 3. Build base merged dict: advance + boxoffice (boxoffice overrides advance)
    merged_dict = {}
    for s in advance_shows:
        sid = str(s.get("showtime_id"))
        merged_dict[sid] = s
    for s in boxoffice_shows:
        sid = str(s.get("showtime_id"))
        merged_dict[sid] = s

    print(f"🔄 Base merged (advance + boxoffice): {len(merged_dict)} shows.")

    # 4. Load zip codes
    if not os.path.exists(ZIP_FILE):
        print(f"❌ Missing {ZIP_FILE}")
        return
    zipcodes = open(ZIP_FILE).read().splitlines()
    print(f"✅ {len(zipcodes)} ZIPs loaded.")

    # 5. Scrape fresh showtimes
    print("🎬 Scraping fresh showtimes...")
    raw_shows = scrape_all_shows_for_date(zipcodes, date_str)

    # 6. Filter by target languages
    lang_filtered = [s for s in raw_shows if s.get("language") in TARGET_LANGUAGES]
    print(f"🎟️ Fresh shows (raw): {len(raw_shows)}, after language filter: {len(lang_filtered)}")

    # ---------- NEW: Deduplicate fresh shows by showtime_id ----------
    unique_fresh = {}
    for s in lang_filtered:
        sid = str(s.get("showtime_id"))
        if sid not in unique_fresh:
            unique_fresh[sid] = s
    lang_filtered = list(unique_fresh.values())
    print(f"🎟️ Unique fresh shows after dedup: {len(lang_filtered)}")
    # ----------------------------------------------------------------

    if lang_filtered:
        print("💺 Fetching seatmaps for fresh shows...")
        asyncio.run(run_seatmap_fetch(lang_filtered))
        fresh_shows = lang_filtered
    else:
        fresh_shows = []

    # 7. Merge fresh shows
    for fresh in fresh_shows:
        sid = str(fresh.get("showtime_id"))
        if sid in merged_dict:
            merged_dict[sid] = merge_show(merged_dict[sid], fresh)
        else:
            if "error" not in fresh:
                merged_dict[sid] = fresh

    merged_shows = list(merged_dict.values())
    print(f"🔄 After merging fresh data: {len(merged_shows)} shows.")

    # 8. Separate errors for logging
    error_shows = [s for s in merged_shows if "error" in s]

    # 9. Save to boxoffice
    save_boxoffice_file(today, merged_shows, error_shows)

    print("\n✅ Done.")

if __name__ == "__main__":
    main()
