import os
import json
import time
import math
import csv
import datetime as dt
from typing import Dict, List, Optional, Tuple

import requests
import pytz
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from ratelimit import limits, sleep_and_retry
import typer
from rich.progress import Progress


APP = typer.Typer()


DATA_DIR = os.path.join("data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
CACHE_DIR = os.path.join("cache")
GEOCODE_CACHE_PATH = os.path.join(CACHE_DIR, "geocode_cache.json")
DISTRICTS_GEOCODED_CSV = os.path.join(DATA_DIR, "districts_geocoded.csv")
MASTER_CSV = os.path.join(DATA_DIR, "master.csv")

OWM_ONECALL_URL = "https://api.openweathermap.org/data/3.0/onecall"
OWM_WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
OWM_AIR_URL = "https://api.openweathermap.org/data/2.5/air_pollution"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
WIKI_DISTRICTS_URL = "https://en.wikipedia.org/wiki/Districts_of_Bangladesh"


CANONICAL_DIVISIONS = [
    "Barishal", "Chattogram", "Dhaka", "Khulna", "Mymensingh", "Rajshahi", "Rangpur", "Sylhet"
]

# Explicit, authoritative district → division mapping (English, canonical spellings)
DISTRICT_TO_DIVISION_EN: Dict[str, str] = {
    # Barishal
    "Barguna": "Barishal", "Barishal": "Barishal", "Bhola": "Barishal", "Jhalokathi": "Barishal",
    "Patuakhali": "Barishal", "Pirojpur": "Barishal",
    # Chattogram
    "Bandarban": "Chattogram", "Brahmanbaria": "Chattogram", "Chandpur": "Chattogram",
    "Chattogram": "Chattogram", "Cumilla": "Chattogram", "Cox's Bazar": "Chattogram",
    "Feni": "Chattogram", "Khagrachhari": "Chattogram", "Lakshmipur": "Chattogram",
    "Noakhali": "Chattogram", "Rangamati": "Chattogram",
    # Dhaka
    "Dhaka": "Dhaka", "Faridpur": "Dhaka", "Gazipur": "Dhaka", "Gopalganj": "Dhaka",
    "Kishoreganj": "Dhaka", "Madaripur": "Dhaka", "Manikganj": "Dhaka", "Munshiganj": "Dhaka",
    "Narayanganj": "Dhaka", "Narsingdi": "Dhaka", "Rajbari": "Dhaka", "Shariatpur": "Dhaka",
    "Tangail": "Dhaka",
    # Khulna
    "Bagerhat": "Khulna", "Chuadanga": "Khulna", "Jashore": "Khulna", "Jhenaidah": "Khulna",
    "Khulna": "Khulna", "Kushtia": "Khulna", "Magura": "Khulna", "Meherpur": "Khulna",
    "Narail": "Khulna", "Satkhira": "Khulna",
    # Mymensingh
    "Jamalpur": "Mymensingh", "Mymensingh": "Mymensingh", "Netrokona": "Mymensingh", "Sherpur": "Mymensingh",
    # Rajshahi
    "Bogura": "Rajshahi", "Chapai Nawabganj": "Rajshahi", "Joypurhat": "Rajshahi", "Naogaon": "Rajshahi",
    "Natore": "Rajshahi", "Pabna": "Rajshahi", "Rajshahi": "Rajshahi", "Sirajganj": "Rajshahi",
    # Rangpur
    "Dinajpur": "Rangpur", "Gaibandha": "Rangpur", "Kurigram": "Rangpur", "Lalmonirhat": "Rangpur",
    "Nilphamari": "Rangpur", "Panchagarh": "Rangpur", "Rangpur": "Rangpur", "Thakurgaon": "Rangpur",
    # Sylhet
    "Habiganj": "Sylhet", "Moulvibazar": "Sylhet", "Sunamganj": "Sylhet", "Sylhet": "Sylhet",
}

def normalize_division_en(district: str, raw_division: str) -> str:
    # Primary: explicit mapping
    mapped = DISTRICT_TO_DIVISION_EN.get(district.strip())
    if mapped:
        return mapped
    # Secondary: normalize any incoming text (including Bangla or legacy names) to canonical
    val = (raw_division or "").strip()
    if not val:
        return ""
    # Remove language-specific suffix like "Division" or Bangla "বিভাগ"
    for suffix in [" Division", " বিভাগ"]:
        if val.endswith(suffix):
            val = val[: -len(suffix)]
    x = val.lower()
    variants = {
        "barisal": "Barishal", "barishal": "Barishal",
        "chittagong": "Chattogram", "chattogram": "Chattogram",
        "dhaka": "Dhaka",
        "khulna": "Khulna",
        "mymensingh": "Mymensingh",
        "rajshahi": "Rajshahi",
        "rangpur": "Rangpur",
        "sylhet": "Sylhet",
        # Bangla
        "বরিশাল": "Barishal", "চট্টগ্রাম": "Chattogram", "ঢাকা": "Dhaka", "খুলনা": "Khulna",
        "ময়মনসিংহ": "Mymensingh", "রাজশাহী": "Rajshahi", "রংপুর": "Rangpur", "সিলেট": "Sylhet",
    }
    return variants.get(x, val.title())

def ensure_dirs() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(CACHE_DIR, exist_ok=True)


def get_date_in_dhaka(target_date: Optional[str]) -> str:
    tz = pytz.timezone("Asia/Dhaka")
    if target_date:
        # Validate format
        try:
            dt.datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            raise typer.BadParameter("--date must be YYYY-MM-DD")
        return target_date
    return dt.datetime.now(tz).strftime("%Y-%m-%d")


def load_env_api_key() -> Optional[str]:
    load_dotenv()
    return os.getenv("OWM_API_KEY")


def read_json(path: str) -> Dict:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: str, data: Dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def scrape_districts_and_divisions() -> List[Dict[str, str]]:
    resp = requests.get(WIKI_DISTRICTS_URL, headers={"User-Agent": "bd-districts-collector/1.0"}, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    results: List[Dict[str, str]] = []
    # The page contains a list/table of districts per division. We'll parse tables with headers containing Division and District
    tables = soup.find_all("table", {"class": ["wikitable", "sortable", "plainrowheaders"]})
    seen = set()
    for table in tables:
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if not headers:
            continue
        # Heuristic: table that contains 'district' in headers
        if not any("district" in h for h in headers):
            continue
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            text_cells = [c.get_text(" ", strip=True) for c in cells]
            # Try to locate district and division columns heuristically
            district_name = None
            division_name = None
            # If there is a header cell in row[0] with district name
            if row.find("th") and row.find("th").get_text(strip=True):
                district_name = row.find("th").get_text(" ", strip=True)
            # Find division text if present in row
            for c in text_cells:
                if c.endswith(" Division") or c in [
                    "Dhaka", "Chattogram", "Rajshahi", "Khulna", "Barishal", "Sylhet", "Rangpur", "Mymensingh",
                    "Dhaka Division", "Chittagong Division", "Chattogram Division", "Barisal Division", "Sylhet Division",
                    "Rangpur Division", "Mymensingh Division", "Rajshahi Division", "Khulna Division"
                ]:
                    division_name = c.replace(" Division", "")
            if district_name:
                # Clean district name parentheses
                district_name = district_name.replace("District", "").strip()
                district_name = district_name.replace("(city)", "").strip()
                key = (district_name.lower(), (division_name or "").lower())
                if key not in seen and len(district_name) > 2:
                    results.append({
                        "district": district_name,
                        "division": division_name or "",
                    })
                    seen.add(key)
    # Deduplicate by district; Wikipedia page structure may lead to extras
    dedup: Dict[str, Dict[str, str]] = {}
    for r in results:
        d = r["district"].strip()
        if d not in dedup:
            dedup[d] = r
    results = list(dedup.values())
    # If not 64, fallback to a static list of district names; fill division later via Nominatim address
    if len(results) < 60 or len(results) > 80:
        static_districts = [
            "Bagerhat","Bandarban","Barguna","Barishal","Bhola","Bogura","Brahmanbaria","Chandpur","Chapai Nawabganj","Chattogram","Chuadanga","Cox's Bazar","Cumilla","Dhaka","Dinajpur","Faridpur","Feni","Gaibandha","Gazipur","Gopalganj","Habiganj","Jamalpur","Jashore","Jhalokathi","Jhenaidah","Joypurhat","Khagrachhari","Khulna","Kishoreganj","Kurigram","Kushtia","Lakshmipur","Lalmonirhat","Madaripur","Magura","Manikganj","Meherpur","Moulvibazar","Munshiganj","Mymensingh","Naogaon","Narail","Narayanganj","Narsingdi","Natore","Netrokona","Nilphamari","Noakhali","Pabna","Panchagarh","Patuakhali","Pirojpur","Rajbari","Rajshahi","Rangamati","Rangpur","Satkhira","Shariatpur","Sherpur","Sirajganj","Sunamganj","Sylhet","Tangail","Thakurgaon"
        ]
        results = [{"district": d, "division": ""} for d in static_districts]
    # Normalize some common alternative spellings
    for r in results:
        if r["district"] == "Chittagong":
            r["district"] = "Chattogram"
        if r["district"] == "Comilla":
            r["district"] = "Cumilla"
        if r["district"] == "Jessore":
            r["district"] = "Jashore"
        if r["district"] == "Barisal":
            r["district"] = "Barishal"
    return results


def load_geocode_cache() -> Dict[str, Dict]:
    return read_json(GEOCODE_CACHE_PATH)


def save_geocode_cache(cache: Dict[str, Dict]) -> None:
    write_json(GEOCODE_CACHE_PATH, cache)


@sleep_and_retry
@limits(calls=1, period=1)  # Nominatim courtesy rate
def geocode_district(district: str) -> Optional[Tuple[float, float, str]]:
    params = {
        "q": f"{district} District, Bangladesh",
        "format": "json",
        "limit": 1,
        "addressdetails": 1,
    }
    resp = requests.get(NOMINATIM_URL, params=params, headers={"User-Agent": "bd-districts-collector/1.0"}, timeout=30)
    if resp.status_code == 429:
        time.sleep(2)
        return geocode_district(district)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        return None
    item = data[0]
    lat = float(item.get("lat"))
    lon = float(item.get("lon"))
    addr = item.get("address", {})
    division = addr.get("state") or addr.get("region") or ""
    # Normalize division naming (remove "Division")
    division = division.replace(" Division", "") if division else ""
    return lat, lon, division


def build_or_load_geocoded_districts(rebuild: bool = False) -> pd.DataFrame:
    ensure_dirs()
    if (not rebuild) and os.path.exists(DISTRICTS_GEOCODED_CSV):
        return pd.read_csv(DISTRICTS_GEOCODED_CSV)

    districts = scrape_districts_and_divisions()
    cache = load_geocode_cache()
    rows: List[Dict] = []
    with Progress() as progress:
        task = progress.add_task("Geocoding districts...", total=len(districts))
        for item in districts:
            dist = item["district"].strip()
            cached = cache.get(dist.lower())
            if cached:
                lat, lon, division = cached["lat"], cached["lon"], cached.get("division", item.get("division", ""))
            else:
                result = geocode_district(dist)
                if not result:
                    progress.advance(task)
                    continue
                lat, lon, division = result
                cache[dist.lower()] = {"lat": lat, "lon": lon, "division": division}
                save_geocode_cache(cache)
            division_final = division or item.get("division", "")
            rows.append({
                "district": dist,
                "division": normalize_division_en(dist, division_final),
                "lat": lat,
                "lon": lon,
            })
            progress.advance(task)

    df = pd.DataFrame(rows).sort_values("district").reset_index(drop=True)
    df.to_csv(DISTRICTS_GEOCODED_CSV, index=False)
    return df


class TransientAPIError(Exception):
    pass


@sleep_and_retry
@limits(calls=60, period=60)  # 60/min
@retry(reraise=True, stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=16), retry=retry_if_exception_type(TransientAPIError))
def call_owm(url: str, params: Dict) -> Dict:
    resp = requests.get(url, params=params, timeout=30)
    if resp.status_code in (429, 500, 502, 503, 504):
        raise TransientAPIError(f"Transient error {resp.status_code}")
    resp.raise_for_status()
    return resp.json()


def fetch_weather(lat: float, lon: float, api_key: str) -> Dict:
    # Use Current Weather API (2.5) for reliable current conditions
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": "metric",
    }
    return call_owm(OWM_WEATHER_URL, params)


def fetch_air(lat: float, lon: float, api_key: str) -> Dict:
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
    }
    return call_owm(OWM_AIR_URL, params)


def extract_row(date_str: str, district: str, division: str, lat: float, lon: float, weather: Dict, air: Dict) -> Dict:
    # Support both OneCall 'current' and Current Weather 2.5 structure
    rain = 0.0
    if "current" in weather:
        current = weather.get("current", {})
        temp_c = current.get("temp")
        humidity = current.get("humidity")
        pressure = current.get("pressure")
        wind_speed = current.get("wind_speed")
        clouds = current.get("clouds")
        if "rain" in current:
            if isinstance(current["rain"], dict):
                rain = float(current["rain"].get("1h", 0.0))
            else:
                rain = float(current.get("rain", 0.0))
        # snow intentionally ignored
    else:
        main = weather.get("main", {})
        wind = weather.get("wind", {})
        clouds_obj = weather.get("clouds", {})
        temp_c = main.get("temp")
        humidity = main.get("humidity")
        pressure = main.get("pressure")
        wind_speed = wind.get("speed")
        clouds = clouds_obj.get("all")
        rain = float((weather.get("rain", {}) or {}).get("1h", 0.0) or 0.0)
        # snow intentionally ignored

    aqi = pm2_5 = pm10 = o3 = no2 = so2 = co = None
    try:
        list_items = air.get("list", [])
        if list_items:
            comp = list_items[0].get("components", {})
            aqi = list_items[0].get("main", {}).get("aqi")
            pm2_5 = comp.get("pm2_5")
            pm10 = comp.get("pm10")
            o3 = comp.get("o3")
            no2 = comp.get("no2")
            so2 = comp.get("so2")
            co = comp.get("co")
    except Exception:
        pass

    return {
        "date": date_str,
        "district": district,
        "division": division,
        "lat": round(float(lat), 6),
        "lon": round(float(lon), 6),
        "temp_c": temp_c,
        "humidity": humidity,
        "pressure": pressure,
        "wind_speed": wind_speed,
        "clouds": clouds,
        "rain": rain,
        "aqi": aqi,
        "pm2_5": pm2_5,
        "pm10": pm10,
        "o3": o3,
        "no2": no2,
        "so2": so2,
        "co": co,
    }


def write_daily_csv(date_str: str, rows: List[Dict]) -> str:
    ensure_dirs()
    out_path = os.path.join(RAW_DIR, f"{date_str}.csv")
    cols = [
        "date","district","division","lat","lon","temp_c","humidity","pressure","wind_speed","clouds","rain","aqi","pm2_5","pm10","o3","no2","so2","co"
    ]
    df = pd.DataFrame(rows, columns=cols)
    df.to_csv(out_path, index=False)
    # Rebuild master from raw files to normalize schema (drop legacy 'snow' if present)
    frames: List[pd.DataFrame] = []
    for fname in sorted(os.listdir(RAW_DIR)):
        if not fname.lower().endswith('.csv'):
            continue
        fpath = os.path.join(RAW_DIR, fname)
        try:
            df_i = pd.read_csv(fpath)
            if 'snow' in df_i.columns:
                df_i = df_i.drop(columns=['snow'])
            # Ensure column order
            missing = [c for c in cols if c not in df_i.columns]
            for m in missing:
                df_i[m] = pd.NA
            df_i = df_i[cols]
            frames.append(df_i)
        except Exception:
            continue
    if frames:
        combined = pd.concat(frames, ignore_index=True)
        combined.to_csv(MASTER_CSV, index=False)
    else:
        df.to_csv(MASTER_CSV, index=False)
    return out_path


@APP.command()
def main(
    date: Optional[str] = typer.Option(None, help="Date in YYYY-MM-DD for Asia/Dhaka"),
    api_key: Optional[str] = typer.Option(None, help="OpenWeatherMap API key"),
    rebuild_geocode: bool = typer.Option(False, "--rebuild-geocode", help="Rebuild geocoded districts cache"),
    limit: Optional[int] = typer.Option(None, help="Limit number of districts to collect (for testing)"),
):
    ensure_dirs()
    date_str = get_date_in_dhaka(date)
    owm_key = api_key or load_env_api_key()
    if not owm_key:
        raise typer.BadParameter("Provide --api-key or set OWM_API_KEY in .env")

    df_geo = build_or_load_geocoded_districts(rebuild=rebuild_geocode)
    if limit is not None and limit > 0:
        df_geo = df_geo.head(limit)
    rows: List[Dict] = []
    with Progress() as progress:
        task = progress.add_task(f"Collecting {len(df_geo)} districts for {date_str}...", total=len(df_geo))
        for _, r in df_geo.iterrows():
            lat, lon = float(r["lat"]), float(r["lon"])
            district = str(r["district"])
            division = str(r.get("division", ""))
            try:
                weather = fetch_weather(lat, lon, owm_key)
                air = fetch_air(lat, lon, owm_key)
                row = extract_row(date_str, district, division, lat, lon, weather, air)
                rows.append(row)
            except Exception as e:
                # Record a minimal row with NaNs to keep shape
                rows.append({
                    "date": date_str,
                    "district": district,
                    "division": division,
                    "lat": round(lat, 6),
                    "lon": round(lon, 6),
                    "temp_c": None,
                    "humidity": None,
                    "pressure": None,
                    "wind_speed": None,
                    "clouds": None,
                    "rain": 0.0,
                    "aqi": None,
                    "pm2_5": None,
                    "pm10": None,
                    "o3": None,
                    "no2": None,
                    "so2": None,
                    "co": None,
                })
            progress.advance(task)

    out_path = write_daily_csv(date_str, rows)
    typer.echo(f"Wrote {len(rows)} rows to {out_path}")


if __name__ == "__main__":
    APP()


