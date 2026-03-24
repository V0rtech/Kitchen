#!/usr/bin/env python3
"""
fetch_ads.py
Concurrent Facebook Ads Library scraper — one browser per brand, running in parallel.

Saves to: campaigns/{slug}/ads.json
                          snapshot_urls.txt
                          images/

Edit the CONFIG block and BRANDS list at the top, then run:
    python fetch_ads.py
"""

import json
import re
import sys
import time
import threading
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# Force UTF-8 output on Windows (avoids charmap UnicodeEncodeError)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ─── CONFIG ───────────────────────────────────────────────────────────────────
WORKERS      = 3                # Concurrent browsers (keep <= 4 to avoid detection)
MAX_IMAGES   = 150              # Max images downloaded per brand (ads.json has everything)
MAX_SCROLLS  = 80               # Safety cap on scroll iterations
SCROLL_PAUSE = 2.5              # Seconds between scrolls
HEADLESS     = True             # False = visible browser (handy for debugging)
COUNTRY      = "US"             # Only ads reached in this country
START_DATE   = "2023-01-01"     # Only ads started on or after this date
MIN_DATE     = datetime(2023, 1, 1)
# ──────────────────────────────────────────────────────────────────────────────

# Brand list.
# page_id: paste the Facebook Page ID if you have it (fastest, most accurate).
#          Leave "" to fall back to keyword search filtered by page_name.
BRANDS = [
    {"name": "Caraway",         "slug": "caraway",         "page_id": "2290435917939387"},
    {"name": "Our Place",       "slug": "our-place",       "page_id": "247732222787053"},
    {"name": "HexClad",         "slug": "hexclad",         "page_id": ""},
    {"name": "Made In",         "slug": "made-in",         "page_id": "1360608127355960"},
    {"name": "Great Jones",     "slug": "great-jones",     "page_id": "1826080967456334"},
    {"name": "Misen",           "slug": "misen",           "page_id": ""},
    {"name": "Williams Sonoma", "slug": "williams-sonoma", "page_id": ""},
    {"name": "Sur La Table",    "slug": "sur-la-table",    "page_id": ""},
    {"name": "Lodge Cast Iron", "slug": "lodge",           "page_id": ""},
    {"name": "Wayfair",         "slug": "wayfair",         "page_id": "215686331786877"},
    {"name": "Crate and Barrel","slug": "crate-barrel",    "page_id": "7769066516"},
    {"name": "OXO",             "slug": "oxo",             "page_id": "78294151872"},
    {"name": "Food52",          "slug": "food52",          "page_id": "133148554015"},
    {"name": "Bellroy",         "slug": "bellroy",         "page_id": ""},
    {"name": "Allbirds",        "slug": "allbirds",        "page_id": "778794852137593"},
]

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

_lock = threading.Lock()


def log(brand: str, msg: str):
    with _lock:
        print(f"[{brand:<20}] {msg}", flush=True)


# ─── URL BUILDER ──────────────────────────────────────────────────────────────

def build_url(brand: dict) -> str:
    """Build the Ads Library URL for a brand, with date + country filters baked in."""
    params = [
        ("active_status", "all"),           # active AND previously active
        ("ad_type",       "all"),
        ("country",       COUNTRY),         # US only
        ("is_targeted_country", "false"),
        ("media_type",    "all"),
        ("start_date[min]", START_DATE),    # Jan 2023 floor
        ("sort_data[direction]", "desc"),
        ("sort_data[mode]",      "total_impressions"),
    ]
    if brand.get("page_id"):
        params += [("search_type", "page"), ("view_all_page_id", brand["page_id"])]
    else:
        params += [("search_type", "keyword_unordered"), ("q", brand["name"])]

    qs = "&".join(f"{k}={quote(str(v), safe='')}" for k, v in params)
    return "https://www.facebook.com/ads/library/?" + qs


# ─── EXTRACTION JS (runs inside the browser) ──────────────────────────────────

SCRAPE_JS = """
() => {
    const results = [];

    // Each ad card reliably contains exactly one text node "Library ID: XXXXXXXX"
    const idSpans = [...document.querySelectorAll('span, div')]
        .filter(el => el.childElementCount === 0
                   && el.textContent.trim().startsWith('Library ID:'));

    idSpans.forEach((span, idx) => {
        // Walk up the DOM to find the card container (first ancestor taller than 200px)
        let card = span;
        for (let i = 0; i < 15; i++) {
            card = card.parentElement;
            if (!card) break;
            if (card.offsetHeight > 200) break;
        }
        if (!card) return;

        const ad = {
            index: idx + 1,
            id: '',
            body: '',
            images: [],
            snapshot_url: '',
            started: '',
        };

        // Library / Ad ID
        const m = span.textContent.match(/Library ID:\\s*(\\d+)/);
        ad.id = m ? m[1] : ('card_' + String(idx + 1).padStart(4, '0'));

        // Body text — Facebook wraps ad copy in style="white-space: pre-wrap"
        const bodyEls = card.querySelectorAll('[style*="white-space: pre-wrap"]');
        ad.body = [...bodyEls].map(el => el.innerText.trim()).filter(Boolean).join(' | ');

        // Full text fallback (truncated)
        ad.text = card.innerText.slice(0, 600).trim();

        // Images — Facebook serves creatives from the scontent CDN
        ad.images = [...card.querySelectorAll('img')]
            .map(img => img.src)
            .filter(src => src && src.includes('scontent') && src.length > 60);

        // Snapshot / detail link
        const links = [...card.querySelectorAll('a[href*="/ads/"]')];
        ad.snapshot_url = links.length ? links[0].href : '';

        // Start date (parsed from card text)
        const dateM = card.innerText.match(/Started running on (.+)/);
        ad.started = dateM ? dateM[1].trim() : '';

        results.push(ad);
    });

    return results;
}
"""


# ─── DATE FILTER ──────────────────────────────────────────────────────────────

def parse_started(ad: dict) -> datetime | None:
    text = (ad.get("started") or "") + " " + (ad.get("text") or "")
    m = re.search(r"Started running on (\w+ \d+, \d{4})", text)
    if m:
        try:
            return datetime.strptime(m.group(1), "%B %d, %Y")
        except ValueError:
            pass
    return None


# ─── IMAGE DOWNLOAD ───────────────────────────────────────────────────────────

def detect_ext(url: str) -> str:
    suffix = Path(url.split("?")[0]).suffix.lower()
    return ".png" if suffix == ".png" else ".jpg"


def download_image(url: str, dest: Path) -> bool:
    try:
        r = requests.get(
            url, timeout=20, stream=True,
            headers={"User-Agent": UA, "Referer": "https://www.facebook.com/"}
        )
        if r.status_code != 200:
            return False
        ct = r.headers.get("Content-Type", "")
        if "image" not in ct and "octet" not in ct:
            return False
        with open(dest, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        if dest.stat().st_size < 3_000:
            dest.unlink(missing_ok=True)
            return False
        return True
    except Exception as exc:
        warnings.warn(str(exc))
        if dest.exists():
            dest.unlink(missing_ok=True)
        return False


# ─── POPUP DISMISSAL ──────────────────────────────────────────────────────────

def dismiss_popups(page):
    for sel in [
        'button:has-text("Allow all cookies")',
        'button:has-text("Decline optional cookies")',
        '[data-testid="cookie-policy-dialog-button"]',
        'button:has-text("Accept")',
        '[aria-label="Close"]',
    ]:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=400):
                btn.click()
                time.sleep(0.4)
        except Exception:
            pass


# ─── SCRAPE ONE BRAND ─────────────────────────────────────────────────────────

def scrape_brand(brand: dict) -> list[dict]:
    name = brand["name"]
    url  = build_url(brand)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        ctx = browser.new_context(
            user_agent=UA,
            viewport={"width": 1440, "height": 900},
            locale="en-US",
        )
        page = ctx.new_page()
        page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )

        log(name, "Opening Ads Library…")
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        except PWTimeout:
            log(name, "Load timed out — continuing anyway")

        time.sleep(4)
        dismiss_popups(page)
        time.sleep(1)

        # ── Scroll until MAX_ADS reached or page exhausted ────────────────────
        prev_count  = 0
        stable      = 0

        for i in range(MAX_SCROLLS):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(SCROLL_PAUSE)
            dismiss_popups(page)

            count = page.locator('div:has-text("Library ID:")').count()
            log(name, f"Scroll {i+1}: ~{count} cards visible")

            if count == prev_count:
                stable += 1
                if stable >= 3:
                    break
            else:
                stable = 0
            prev_count = count

        # ── Stabilise the page before extracting ──────────────────────────────
        try:
            page.wait_for_load_state("networkidle", timeout=10_000)
        except Exception:
            pass
        time.sleep(1.5)

        # If Facebook redirected us away (login wall, consent) the context dies.
        # Check we're still on the ads library before extracting.
        current_url = page.url
        if "ads/library" not in current_url:
            log(name, f"Redirected to {current_url[:60]} — navigating back")
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=20_000)
                time.sleep(3)
                dismiss_popups(page)
                time.sleep(1)
                page.wait_for_load_state("networkidle", timeout=8_000)
            except Exception:
                pass

        # ── Extract card data via JS ───────────────────────────────────────────
        raw: list = []
        for attempt in range(3):
            try:
                raw = page.evaluate(SCRAPE_JS) or []
                break
            except Exception as exc:
                log(name, f"JS attempt {attempt+1} failed: {exc} — waiting 3s")
                time.sleep(3)

        browser.close()

    # ── Post-process: date filter + cap ───────────────────────────────────────
    out = []
    for ad in raw:
        # Date gate: skip ads older than MIN_DATE (when date is parseable)
        dt = parse_started(ad)
        if dt and dt < MIN_DATE:
            continue

        out.append(ad)

    log(name, f"Collected {len(out)} ads after filters.")
    return out


# ─── SAVE ONE BRAND ───────────────────────────────────────────────────────────

def save_brand(brand: dict, ads: list[dict]):
    name    = brand["name"]
    out_dir = Path("campaigns") / brand["slug"]
    img_dir = out_dir / "images"
    out_dir.mkdir(parents=True, exist_ok=True)
    img_dir.mkdir(parents=True, exist_ok=True)

    # ads.json
    with open(out_dir / "ads.json", "w", encoding="utf-8") as f:
        json.dump(ads, f, indent=2, ensure_ascii=False)

    # snapshot_urls.txt
    with open(out_dir / "snapshot_urls.txt", "w", encoding="utf-8") as f:
        for ad in ads:
            f.write(f"{ad.get('id','?')} | {ad.get('snapshot_url','')}\n")

    # Images — download up to MAX_IMAGES per brand (ads.json contains all)
    downloaded = skipped = failed = 0
    img_count = 0
    for ad in ads:
        if img_count >= MAX_IMAGES:
            break
        ad_id = ad.get("id", f"card_{ad['index']:04d}")
        for j, img_url in enumerate(ad.get("images", [])):
            if img_count >= MAX_IMAGES:
                break
            suffix   = f"_{j+1}" if j > 0 else ""
            existing = list(img_dir.glob(f"{ad_id}{suffix}.*"))
            if existing:
                skipped += 1
                img_count += 1
                continue
            dest = img_dir / f"{ad_id}{suffix}{detect_ext(img_url)}"
            if download_image(img_url, dest):
                downloaded += 1
                img_count += 1
            else:
                failed += 1

    log(name, f"Images: {downloaded} downloaded, {skipped} skipped, {failed} failed → {out_dir}")


# ─── PER-BRAND ENTRY POINT ────────────────────────────────────────────────────

def run_brand(brand: dict):
    try:
        ads = scrape_brand(brand)
        save_brand(brand, ads)
        log(brand["name"], "[OK] Complete")
    except Exception as exc:
        log(brand["name"], f"[FAIL] Error: {exc}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*60}")
    print(f"  Facebook Ads Library — Concurrent Scraper")
    print(f"  Brands  : {len(BRANDS)}")
    print(f"  Workers : {WORKERS}  (browsers in parallel)")
    print(f"  Max imgs: {MAX_IMAGES} per brand (ads.json is unlimited)")
    print(f"  Since   : {START_DATE}")
    print(f"  Country : {COUNTRY}  (active + historical)")
    print(f"{'='*60}\n")

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(run_brand, b): b["name"] for b in BRANDS}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                fut.result()
            except Exception as exc:
                log(name, f"Unhandled exception: {exc}")

    print("\n[DONE] All brands complete.")


if __name__ == "__main__":
    main()
