import requests
import json
import os
import random
import asyncio
import aiohttp
from aiohttp_retry import RetryClient, ExponentialRetry
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict

# ================= CONFIGURATION =================
# Same as main scraper
TARGET_LANGUAGES = ["Hindi", "Tamil", "Telugu", "Malayalam", "Kannada"]
ZIP_FILE = "zipcodes.txt"
AUTHORIZATION_TOKEN = "<your-auth-token>"
SESSION_ID = "<your-session-id>"
MAX_WORKERS = 50
CONCURRENCY = 15

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
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/{version} Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:{version}) Gecko/20100101 Firefox/{version}",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_{minor}_0) AppleWebKit/537.36 Chrome/{version} Safari/537.36",
]

def get_random_user_agent():
    template = random.choice(USER_AGENTS)
    return template.format(
        version=f"{random.randint(70,120)}.0.{random.randint(1000,5000)}.{random.randint(0,150)}",
        minor=random.randint(12,15)
    )

def get_random_ip():
    return ".".join(str(random.randint(1,255)) for _ in range(4))

# ================= HEADER BUILDERS =================
def get_theater_headers(zip_code, date_str):
    ip = get_random_ip()
    return {
        "User-Agent": get_random_user_agent(),
        "Accept": "application/json",
        "Referer": f"https://www.fandango.com/{zip_code}_movietimes?date={date_str}",
        "X-Forwarded-For": ip,
        "Client-IP": ip,
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }

def get_seatmap_headers():
    ip = get_random_ip()
    return {
        "User-Agent": get_random_user_agent(),
        "Origin": "https://fandango.com",
        "Referer": "https://tickets.fandango.com/mobileexpress/seatselection",
        "Connection": "keep-alive",
        "Authorization": AUTHORIZATION_TOKEN,
        "X-Fd-Sessionid": SESSION_ID,
        "authority": "tickets.fandango.com",
        "accept": "application/json",
        "X-Forwarded-For": ip,
        "Client-IP": ip,
        "Accept-Encoding": "gzip, deflate, br",
    }

# ================= PARSERS =================
def extract_language(amenities):
    for item in amenities:
        for lang in KNOWN_LANGUAGES:
            if lang.lower() in item.lower():
                return lang
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

# ================= THEATER SCRAPER =================
def get_theaters(zip_code, date_str):
    url = "https://www.fandango.com/napi/theaterswithshowtimes"
    params = {"zipCode": zip_code, "date": date_str, "page": 1, "limit": 40}
    try:
        r = requests.get(url, headers=get_theater_headers(zip_code, date_str), params=params, timeout=10)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
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

# ================= SEATMAP FETCHING =================
def seatmap_url(showtime_id):
    return f"https://tickets.fandango.com/checkoutapi/showtimes/v2/{showtime_id}/seat-map/"

async def fetch_seat(session, show):
    sid = str(show["showtime_id"])
    try:
        async with session.get(seatmap_url(sid), headers=get_seatmap_headers(), timeout=10) as resp:
            if resp.status != 200:
                show["error"] = {"status": resp.status}
                return
            data = await resp.json()
            d = data.get("data", {})
            available = d.get("totalAvailableSeatCount", 0)
            total = d.get("totalSeatCount", 0)
            sold = total - available
            show["totalSeatSold"] = sold
            show["totalSeatCount"] = total
            show["occupancy"] = round((sold / total) * 100, 2) if total else 0

            # Extract adult ticket price
            price = 0
            areas = d.get("areas", [])
            for area in areas:
                for tinfo in area.get("ticketInfo", []):
                    if "adult" in tinfo.get("desc", "").lower():
                        try:
                            price = float(tinfo.get("price", 0))
                            break
                        except:
                            pass
                if price:
                    break
            if price == 0:
                for area in areas:
                    ti = area.get("ticketInfo", [])
                    if ti:
                        try:
                            price = float(ti[0].get("price", 0))
                            break
                        except:
                            pass
            show["adultTicketPrice"] = price
            show["grossRevenueUSD"] = round(price * sold, 2)
    except Exception as e:
        show["error"] = {"exception": str(e)}

async def run_seatmap_fetch(shows):
    connector = aiohttp.TCPConnector(ssl=False)
    retry = ExponentialRetry(attempts=3)
    async with RetryClient(connector=connector, retry_options=retry) as session:
        sem = asyncio.Semaphore(CONCURRENCY)
        async def bound(s):
            async with sem:
                await fetch_seat(session, s)
        tasks = [bound(s) for s in shows]
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Seatmaps"):
            await f

# ================= MERGE LOGIC =================
def merge_show(old, new):
    """
    Merge two show records.
    - Keep the higher totalSeatSold (to avoid losing sales due to errors).
    - Update other fields (totalSeatCount, occupancy, gross) based on chosen sold.
    - If new has error, keep old entirely.
    - If new is valid but old has higher sold, keep old's sold and recompute derived fields.
    """
    if not old:
        return new
    if "error" in new:
        return old  # keep old if new fetch failed

    # Determine which sold count to use (max)
    new_sold = new.get("totalSeatSold", 0)
    old_sold = old.get("totalSeatSold", 0)
    if new_sold > old_sold:
        chosen = new
        chosen_sold = new_sold
    else:
        # Keep old sold, but we might still want some fields from new (like totalSeatCount if changed?)
        chosen = old.copy()
        chosen_sold = old_sold

    # Update derived fields based on chosen sold
    total = chosen.get("totalSeatCount", 0)
    if total and total > 0:
        chosen["occupancy"] = round((chosen_sold / total) * 100, 2)
    else:
        chosen["occupancy"] = 0.0

    # Gross revenue: use chosen_sold * adultTicketPrice
    price = chosen.get("adultTicketPrice", 0.0)
    chosen["grossRevenueUSD"] = round(price * chosen_sold, 2)

    # Ensure totalSeatSold is the chosen value
    chosen["totalSeatSold"] = chosen_sold

    # Keep any other fields from new that might be useful (like format, language, etc.)
    # but old already has them; no need to override unless they change.
    # However, if new has a different date? should not happen.
    return chosen

# ================= LOAD / SAVE HELPERS =================
def load_advance_file(date_obj):
    """
    Load the advance JSON file for a given date from usa-advance/YYYY/DD-MM.json
    Returns a dict with 'shows' (list of dicts) and 'summary' (list) or None if not found.
    """
    year = date_obj.strftime("%Y")
    filename = date_obj.strftime("%d-%m.json")
    filepath = os.path.join("usa-advance", year, filename)
    if not os.path.exists(filepath):
        return None
    try:
        with open(filepath, "r") as f:
            data = json.load(f)
        # data has {"shows": [...], "summary": [...]}
        # shows are compact arrays; we need to convert back to dicts.
        # But we stored compact arrays; we need to parse them.
        # We'll reconstruct the show dicts using the same field order.
        if "shows" in data and isinstance(data["shows"], list):
            show_dicts = []
            for arr in data["shows"]:
                # order: showtime_id, date, format, language, movie_title, movie_id,
                # theater_name, city, state, chainName, totalSeatSold, totalSeatCount,
                # occupancy, adultTicketPrice, grossRevenueUSD
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
            return {"shows": show_dicts, "summary": data.get("summary", [])}
    except Exception as e:
        print(f"Failed to load advance file {filepath}: {e}")
    return None

def save_boxoffice_file(date_obj, shows_dict):
    """
    Save the merged shows (list of dicts) to usa-boxoffice/YYYY/DD-MM.json
    in the same compact format with summary.
    """
    if not shows_dict:
        print(f"No shows for {date_obj}, skipping boxoffice file.")
        return

    # Deduplicate by showtime_id (just in case)
    seen = set()
    unique = []
    for s in shows_dict:
        sid = str(s.get("showtime_id"))
        if sid not in seen:
            seen.add(sid)
            unique.append(s)

    # Build compact show list (same order as advance)
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

    # Movie-wise summary
    movie_summary = defaultdict(lambda: {
        "shows": 0,
        "tickets": 0,
        "seats": 0,
        "gross": 0.0,
        "occupancy_sum": 0.0,
    })
    for s in unique:
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

    print(f"Saved {len(compact)} shows to {filepath}")

# ================= MAIN =================
def main():
    # Determine today in US Eastern Time
    eastern = ZoneInfo("America/New_York")
    today = datetime.now(eastern).date()
    date_str = today.strftime("%Y-%m-%d")
    print(f"Box Office for today: {date_str}")

    # Load advance data if available
    advance_data = load_advance_file(today)
    advance_shows = advance_data.get("shows", []) if advance_data else []
    print(f"Loaded {len(advance_shows)} shows from advance data.")

    # Load zip codes
    if not os.path.exists(ZIP_FILE):
        print(f"Error: {ZIP_FILE} not found.")
        return
    zipcodes = open(ZIP_FILE).read().splitlines()
    if not zipcodes:
        print("No zip codes found.")
        return

    # Fetch fresh data for today
    print("Fetching fresh theater data...")
    raw_shows = scrape_all_shows_for_date(zipcodes, date_str)

    # Filter by target languages
    lang_filtered = [s for s in raw_shows if s.get("language") in TARGET_LANGUAGES]
    print(f"Fresh shows (raw): {len(raw_shows)}, after language filter: {len(lang_filtered)}")

    if not lang_filtered:
        print("No fresh shows found for today. Using only advance data.")
        fresh_shows = []
    else:
        # Fetch seatmaps for fresh shows
        print("Fetching seatmaps for fresh shows...")
        asyncio.run(run_seatmap_fetch(lang_filtered))
        # Keep only successful (no error) but we also need to keep errors? Actually we need to merge; if error, we might still keep advance data.
        fresh_shows = lang_filtered  # includes those with errors

    # Merge: start with advance shows
    merged_dict = {}
    for s in advance_shows:
        sid = str(s.get("showtime_id"))
        merged_dict[sid] = s  # start with advance

    # Now process fresh shows
    for fresh in fresh_shows:
        sid = str(fresh.get("showtime_id"))
        if sid in merged_dict:
            # Merge: keep highest sales
            merged_dict[sid] = merge_show(merged_dict[sid], fresh)
        else:
            # New show not in advance
            if "error" not in fresh:
                # Only add if no error
                merged_dict[sid] = fresh
            else:
                # If error, we don't add (no data)
                pass

    # Convert to list
    merged_shows = list(merged_dict.values())
    print(f"After merge: {len(merged_shows)} shows.")

    # Save to boxoffice
    save_boxoffice_file(today, merged_shows)

if __name__ == "__main__":
    main()
