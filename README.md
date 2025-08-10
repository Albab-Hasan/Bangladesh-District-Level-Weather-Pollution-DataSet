# Bangladesh District Weather & Air Quality Dataset

This project collects daily weather and air quality metrics for Bangladesh's 64 districts using OpenWeatherMap APIs. It automatically builds a geocoded districts cache (names, divisions, lat, lon) from Wikipedia and OpenStreetMap Nominatim, then fetches weather and air quality in a rate-limited, retry-safe manner, and writes a daily CSV.

### Data sources
- Weather: OpenWeatherMap One Call API 3.0 (`/data/3.0/onecall`)
- Air Quality: OpenWeatherMap Air Pollution API (`/data/2.5/air_pollution`)
- Districts & Divisions: Wikipedia (scraped) + OSM Nominatim geocoding

### Output schema
CSV columns per row (one per district per day):
`date,district,division,lat,lon,temp_c,humidity,pressure,wind_speed,clouds,rain,snow,aqi,pm2_5,pm10,o3,no2,so2,co`

### Quickstart
1) Create a virtual environment and install deps

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2) Provide your API key

- Create a `.env` file with:

```
OWM_API_KEY=YOUR_OPENWEATHERMAP_API_KEY
```

3) Run the daily collector

```powershell
python scripts/collect_daily.py
```

On the first run, the script will:
- Scrape districts and divisions from Wikipedia
- Geocode each district via Nominatim (with caching)
- Save the cache at `data/districts_geocoded.csv`
- Collect weather and AQ data for all 64 districts
- Write the daily CSV to `data/raw/YYYY-MM-DD.csv` and append to `data/master.csv`

### Notes
- Rate limits respected: ~1 request/second, retries with exponential backoff for 429/5xx
- Units: Metric (°C, m/s). Rain/snow default to 0.0 if missing
- Date is recorded in Asia/Dhaka local date (YYYY-MM-DD)

### CLI options
```powershell
python scripts/collect_daily.py --date 2025-08-10 --api-key YOUR_KEY --rebuild-geocode
```

Flags:
- `--date`: force a specific collection date (YYYY-MM-DD). Defaults to today in Asia/Dhaka
- `--api-key`: override `OWM_API_KEY`
- `--rebuild-geocode`: refresh the geocoded districts cache

### Folder structure
- `scripts/collect_daily.py`: main entrypoint
- `data/`
  - `districts_geocoded.csv`: cached district/division/lat/lon
  - `raw/YYYY-MM-DD.csv`: daily dataset
  - `master.csv`: cumulative dataset
- `cache/geocode_cache.json`: raw geocoder cache

### Disclaimer
Respect the terms of use for Wikipedia and OSM Nominatim. This project keeps geocoding calls minimal and caches results locally.

### Automate daily runs (GitHub Actions)
- Add the following repository secrets:
  - `OWM_API_KEY`: your OpenWeatherMap API key
- The workflow `.github/workflows/daily.yml` runs every day at 12:00 Dhaka time (06:00 UTC), collects data, and commits updates to the repository.

If you prefer running locally on Windows without GitHub:
- Use Task Scheduler to run `python scripts/collect_daily.py` daily at 12:00.
  - Program: `C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe`
  - Arguments: `-NoProfile -ExecutionPolicy Bypass -Command "cd '<repo path>'; .\.venv\\Scripts\\Activate.ps1; python scripts/collect_daily.py"`
