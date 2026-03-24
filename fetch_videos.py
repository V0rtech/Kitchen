#!/usr/bin/env python3
"""
fetch_videos.py
Concurrent Facebook Ads Library video scraper — one browser per brand.
Uses Playwright network interception to capture video files as they stream
through the browser (bypasses CDN session-auth issues).

Saves to: campaigns/{slug}/videos/{ad_id}.mp4
                           video_ads.json
                           video_snapshot_urls.txt

Edit CONFIG and BRANDS, then run:
    python fetch_videos.py
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

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# Force UTF-8 output on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ─── CONFIG ───────────────────────────────────────────────────────────────────
WORKERS      = 3                # Concurrent browsers
MAX_VIDEOS   = 150              # Max video files saved per brand
MAX_SCROLLS  = 80               # Safety cap on scroll iterations
SCROLL_PAUSE = 3.0              # Slightly longer pause (videos are heavier)
HEADLESS     = True
COUNTRY      = "US"
START_DATE   = "2023-01-01"
MIN_DATE     = datetime(2023, 1, 1)
MIN_SIZE_KB  = 50               # Skip intercepted files smaller than this (likely thumbnails)
# ──────────────────────────────────────────────────────────────────────────────

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
    {"name": "Allbirds",        "slug": "allbirds",        "page_id": ""},
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
    params = [
        ("active_status", "all"),
        ("ad_type",       "all"),
        ("country",       COUNTRY),
        ("is_targeted_country", "false"),
        ("media_type",    "video"),           # <-- videos only
        ("start_date[min]", START_DATE),
        ("sort_data[direction]", "desc"),
        ("sort_data[mode]",      "total_impressions"),
    ]
    if brand.get("page_id"):
        params += [("search_type", "page"), ("view_all_page_id", brand["page_id"])]
    else:
        params += [("search_type", "keyword_unordered"), ("q", brand["name"])]

    qs = "&".join(f"{k}={quote(str(v), safe='')}" for k, v in params)
    return "https://www.facebook.com/ads/library/?" + qs


# ─── CARD METADATA EXTRACTION JS ──────────────────────────────────────────────

SCRAPE_JS = """
() => {
    const results = [];
    const idSpans = [...document.querySelectorAll('span, div')]
        .filter(el => el.childElementCount === 0
                   && el.textContent.trim().startsWith('Library ID:'));

    idSpans.forEach((span, idx) => {
        let card = span;
        for (let i = 0; i < 15; i++) {
            card = card.parentElement;
            if (!card) break;
            if (card.offsetHeight > 200) break;
        }
        if (!card) return;

        const ad = { index: idx + 1, id: '', body: '', snapshot_url: '', started: '', active: null };

        const m = span.textContent.match(/Library ID:\\s*(\\d+)/);
        ad.id = m ? m[1] : ('card_' + String(idx + 1).padStart(4, '0'));

        const bodyEls = card.querySelectorAll('[style*="white-space: pre-wrap"]');
        ad.body = [...bodyEls].map(el => el.innerText.trim()).filter(Boolean).join(' | ');
        ad.text = card.innerText.slice(0, 600).trim();

        // Grab video src URLs directly from <video> elements
        ad.video_srcs = [...card.querySelectorAll('video')]
            .flatMap(v => [
                v.src,
                ...[...v.querySelectorAll('source')].map(s => s.src)
            ])
            .filter(src => src && src.startsWith('http'));

        const links = [...card.querySelectorAll('a[href*="/ads/"]')];
        ad.snapshot_url = links.length ? links[0].href : '';

        // Active status — appears as a standalone line before Library ID
        const activeM = card.innerText.match(/\n(Active|Inactive)\n/);
        ad.active = activeM ? activeM[1] === 'Active' : null;

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

def scrape_brand(brand: dict, video_dir: Path) -> list[dict]:
    name        = brand["name"]
    url         = build_url(brand)
    saved_count = [0]           # mutable int for use inside closure
    seen_urls   = set()
    ad_map      = {}            # video_url -> ad_id (filled in after card scrape)

    # ── Network interception: capture video responses as they stream ───────────
    def handle_response(response):
        if saved_count[0] >= MAX_VIDEOS:
            return
        resp_url  = response.url
        ct        = response.headers.get("content-type", "")
        if "video" not in ct and not resp_url.endswith(".mp4"):
            return
        if resp_url in seen_urls:
            return
        seen_urls.add(resp_url)

        try:
            body = response.body()
            if len(body) < MIN_SIZE_KB * 1024:
                return                          # skip tiny thumbnails

            # Use ad_id if we've already mapped this URL, else generate a name
            ad_id    = ad_map.get(resp_url, f"video_{saved_count[0]+1:04d}")
            dest     = video_dir / f"{ad_id}.mp4"
            if dest.exists():
                return

            dest.write_bytes(body)
            saved_count[0] += 1
            log(name, f"  Intercepted video {saved_count[0]}: {dest.name} ({len(body)//1024} KB)")
        except Exception as exc:
            warnings.warn(f"Failed to save intercepted video: {exc}")

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
        page.on("response", handle_response)

        log(name, "Opening Ads Library (video filter)...")
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        except PWTimeout:
            log(name, "Load timed out — continuing anyway")

        time.sleep(4)
        dismiss_popups(page)
        time.sleep(1)

        # ── Scroll to trigger video lazy-loads ────────────────────────────────
        prev_count = 0
        stable     = 0

        for i in range(MAX_SCROLLS):
            if saved_count[0] >= MAX_VIDEOS:
                log(name, f"Reached {MAX_VIDEOS} video cap — stopping scroll.")
                break

            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(SCROLL_PAUSE)
            dismiss_popups(page)

            count = page.locator('div:has-text("Library ID:")').count()
            log(name, f"Scroll {i+1}: ~{count} cards, {saved_count[0]} videos captured")

            if count == prev_count:
                stable += 1
                if stable >= 3:
                    break
            else:
                stable = 0
            prev_count = count

        # ── Final: extract card metadata + map video srcs to ad IDs ───────────
        try:
            page.wait_for_load_state("networkidle", timeout=10_000)
        except Exception:
            pass
        time.sleep(1.5)

        current_url = page.url
        if "ads/library" not in current_url:
            log(name, f"Redirected — navigating back")
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=20_000)
                time.sleep(3)
                dismiss_popups(page)
                page.wait_for_load_state("networkidle", timeout=8_000)
            except Exception:
                pass

        raw: list = []
        for attempt in range(3):
            try:
                raw = page.evaluate(SCRAPE_JS) or []
                break
            except Exception as exc:
                log(name, f"JS attempt {attempt+1} failed: {exc}")
                time.sleep(3)

        # Map video_src -> ad_id for better file naming (best effort)
        for ad in raw:
            for vsrc in ad.get("video_srcs", []):
                ad_map[vsrc] = ad["id"]

        browser.close()

    # ── Date filter ───────────────────────────────────────────────────────────
    out = []
    for ad in raw:
        dt = parse_started(ad)
        if dt and dt < MIN_DATE:
            continue
        out.append(ad)

    log(name, f"Collected {len(out)} video ad cards, {saved_count[0]} videos saved.")
    return out


# ─── SAVE METADATA ────────────────────────────────────────────────────────────

def save_brand(brand: dict, ads: list[dict]):
    out_dir = Path("campaigns") / brand["slug"]
    out_dir.mkdir(parents=True, exist_ok=True)

    with open(out_dir / "video_ads.json", "w", encoding="utf-8") as f:
        json.dump(ads, f, indent=2, ensure_ascii=False)

    with open(out_dir / "video_snapshot_urls.txt", "w", encoding="utf-8") as f:
        for ad in ads:
            f.write(f"{ad.get('id','?')} | {ad.get('snapshot_url','')}\n")

    log(brand["name"], f"Metadata saved to {out_dir}")


# ─── PER-BRAND ENTRY POINT ────────────────────────────────────────────────────

def run_brand(brand: dict):
    name      = brand["name"]
    video_dir = Path("campaigns") / brand["slug"] / "videos"
    video_dir.mkdir(parents=True, exist_ok=True)

    try:
        ads = scrape_brand(brand, video_dir)
        save_brand(brand, ads)
        log(name, "[OK] Complete")
    except Exception as exc:
        log(name, f"[FAIL] Error: {exc}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*60}")
    print(f"  Facebook Ads Library — Video Scraper")
    print(f"  Brands    : {len(BRANDS)}")
    print(f"  Workers   : {WORKERS}  (browsers in parallel)")
    print(f"  Max videos: {MAX_VIDEOS} per brand")
    print(f"  Since     : {START_DATE}")
    print(f"  Country   : {COUNTRY}")
    print(f"  Strategy  : Network interception (no CDN auth issues)")
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
