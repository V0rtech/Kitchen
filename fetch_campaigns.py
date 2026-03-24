#!/usr/bin/env python3
"""
fetch_campaigns.py
Fetches real campaign ad images for all 18 analyzed kitchen/lifestyle brands
using the Meta Ad Library API. For brands with limited Meta presence,
falls back to manually curated public image URLs.

Usage:
    python fetch_campaigns.py

    You will be prompted for a Meta access token.
    Get one free at: https://developers.facebook.com/tools/explorer/
    (Generate User Token with default permissions — no special scope needed)

    Or set env var:  set META_TOKEN=your_token_here

Output:
    campaigns/campaigns.json   — data file for the gallery
    campaigns/<brand>/         — downloaded image files (for API brands)
"""

import os
import sys
import json
import time
import re
import requests
from pathlib import Path
from urllib.parse import urlparse, urlencode

# ─── CONFIG ───────────────────────────────────────────────────────────────────
OUTPUT_DIR = Path("campaigns")
JSON_PATH = OUTPUT_DIR / "campaigns.json"
ADS_PER_BRAND = 8          # how many ads to try to fetch per brand
API_VERSION = "v19.0"
API_BASE = f"https://graph.facebook.com/{API_VERSION}/ads_archive"
REQUEST_DELAY = 1.2        # seconds between API calls (rate limit courtesy)
DOWNLOAD_TIMEOUT = 20
# ─────────────────────────────────────────────────────────────────────────────

# Brand definitions: name, slug, accent color, Meta search term, and page_name hint
# page_name is used to filter results so we only get ads from the real brand page
BRANDS_META = [
    {
        "name": "Caraway",
        "slug": "caraway",
        "color": "#7b2fbe",
        "search": "Caraway Home cookware",
        "page_name_contains": "Caraway",
    },
    {
        "name": "Our Place",
        "slug": "our-place",
        "color": "#d94f3b",
        "search": "Our Place Always Pan",
        "page_name_contains": "Our Place",
    },
    {
        "name": "HexClad",
        "slug": "hexclad",
        "color": "#c8922a",
        "search": "HexClad cookware hybrid",
        "page_name_contains": "HexClad",
    },
    {
        "name": "Made In",
        "slug": "made-in",
        "color": "#2a4a9e",
        "search": "Made In cookware professional",
        "page_name_contains": "Made In",
    },
    {
        "name": "Great Jones",
        "slug": "great-jones",
        "color": "#e8832a",
        "search": "Great Jones cooking pots",
        "page_name_contains": "Great Jones",
    },
    {
        "name": "Misen",
        "slug": "misen",
        "color": "#0F6E56",
        "search": "Misen kitchen knives cookware",
        "page_name_contains": "Misen",
    },
    {
        "name": "Material",
        "slug": "material",
        "color": "#6B7A99",
        "search": "Material kitchen cookware",
        "page_name_contains": "Material Kitchen",
    },
    {
        "name": "Williams Sonoma",
        "slug": "williams-sonoma",
        "color": "#1a6e4a",
        "search": "Williams Sonoma cookware kitchen",
        "page_name_contains": "Williams-Sonoma",
    },
    {
        "name": "Sur La Table",
        "slug": "sur-la-table",
        "color": "#3B5BDB",
        "search": "Sur La Table cooking classes",
        "page_name_contains": "Sur La Table",
    },
    {
        "name": "Lodge",
        "slug": "lodge",
        "color": "#8B4513",
        "search": "Lodge cast iron skillet",
        "page_name_contains": "Lodge",
    },
    {
        "name": "Wayfair",
        "slug": "wayfair",
        "color": "#7340cc",
        "search": "Wayfair kitchen cookware home",
        "page_name_contains": "Wayfair",
    },
    {
        "name": "Crate & Barrel",
        "slug": "crate-barrel",
        "color": "#2d6a4f",
        "search": "Crate and Barrel kitchen",
        "page_name_contains": "Crate",
    },
    {
        "name": "OXO",
        "slug": "oxo",
        "color": "#333333",
        "search": "OXO kitchen tools cooking",
        "page_name_contains": "OXO",
    },
    {
        "name": "Nordic Ware",
        "slug": "nordic-ware",
        "color": "#c0392b",
        "search": "Nordic Ware bakeware bundt",
        "page_name_contains": "Nordic Ware",
    },
    {
        "name": "Food52",
        "slug": "food52",
        "color": "#e67e22",
        "search": "Food52 kitchen shop recipes",
        "page_name_contains": "Food52",
    },
]

# Brands covered by manually curated public image URLs (no Meta API needed)
# Sources: Ads of the World, brand press pages, official campaign pages
BRANDS_MANUAL = [
    {
        "name": "Bellroy",
        "slug": "bellroy",
        "color": "#1a6e4a",
        "ads": [
            {
                "img": "https://image.adsoftheworld.com/hs64ppkimfkbfwrhidri8a5gbjjk",
                "caption": "Bellroy · \"Built for All Lives\" — illustrated multiverse campaign showing modular system with infinite remix potential. 2025.",
                "platform": "curated",
            },
            {
                "img": "https://image.adsoftheworld.com/uxjmo2twt1nwnm72rvmbfe39rffs",
                "caption": "Bellroy · \"Built for All Lives\" — character-driven durability scenes across life stages. Agency: triciclo.",
                "platform": "curated",
            },
            {
                "img": "https://image.adsoftheworld.com/j2zx4yrtgbzx0du5z1g0p8rfwfud",
                "caption": "Bellroy · Systematic illustration approach — same characters, different settings. Scales infinitely.",
                "platform": "curated",
            },
            {
                "img": "https://image.adsoftheworld.com/cyjmumf495kl00sxrdqlh9bpji5o",
                "caption": "Bellroy · Paraguay market localisation — same brand ethos, culturally adapted visual language.",
                "platform": "curated",
            },
            {
                "img": "https://image.adsoftheworld.com/bmzu4xwmtzcgotolj6z1x9oi00wy",
                "caption": "Bellroy · Modular design system enabling endless creative combinations. CCO: Martín Gauto.",
                "platform": "curated",
            },
        ],
    },
    {
        "name": "Allbirds",
        "slug": "allbirds",
        "color": "#2a4a9e",
        "ads": [
            {
                "img": "https://image.adsoftheworld.com/fv7rivl0hly7tdm7clgftz8qt97b",
                "caption": "Allbirds · \"Effortless by Nature\" — Tree Glider launch. Wearers float through outdoor scenes. Agency: Sid Lee. Director: Fred de Poncharra. 2024.",
                "platform": "curated",
            },
            {
                "img": "https://images.ctfassets.net/hhv516v5f7sj/5oNjGW0i1aBT1TmqmhFWl8/f09a826e2eee22b5c3a92a48a0ea4b26/allbirds-tree-runner-go-social.jpg",
                "caption": "Allbirds · Tree Runner Go lifestyle shot — natural materials, minimal aesthetic. 2024.",
                "platform": "curated",
            },
        ],
    },
    {
        "name": "Hestan",
        "slug": "hestan",
        "color": "#8B0000",
        "ads": [
            {
                "img": "https://www.hestan.com/media/wysiwyg/hestan-culinary-home-page-hero-1440.jpg",
                "caption": "Hestan Culinary · Pro-grade cookware endorsed by Thomas Keller — hero campaign imagery from hestan.com.",
                "platform": "curated",
            },
        ],
    },
]


def get_token():
    token = os.environ.get("META_TOKEN", "").strip()
    if token:
        print(f"✓ Using META_TOKEN from environment variable.")
        return token
    print("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  Meta Ad Library API — Access Token Setup")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  1. Go to: https://developers.facebook.com/tools/explorer/")
    print("  2. Click 'Generate Access Token' (no extra permissions needed)")
    print("  3. Paste the token below\n")
    token = input("EAANS6GZAQWF4BQwjnTVWpxiH8ZCPmV1i9TZBhqtNdMABoXcQFMJv9fyt41pZBwzV2PjQSWJ6HkuOI4KkToAD3jprxlMYuvx4SyUUhGmsat0VNRCsg3ZAlr8MU2evXrQyVSXfzE4MsXDELpffgRdpxoSQyHsTzvIvu8tls3bjtz439pVNY8nEnk3yHw83yWZA0MKXy9eU1r0C8ZCTjZARByY89dyxzTtZCD9AlUfAVDMNQetkzdTnqK64vrepp2ZCGoZAo6RWZBNcMxfFruP3").strip()
    return token


def fetch_ads_for_brand(brand, token, session):
    """Call Meta Ad Library API and return list of ad objects."""
    params = {
        "access_token": token,
        "search_terms": brand["search"],
        "ad_type": "ALL",
        "ad_reached_countries": "['US']",
        "fields": "id,page_name,ad_snapshot_url,ad_creative_bodies",
        "limit": ADS_PER_BRAND * 3,  # fetch extra to filter
    }
    try:
        r = session.get(API_BASE, params=params, timeout=15)
        data = r.json()
        if "error" in data:
            print(f"    ⚠ API error: {data['error'].get('message', 'unknown')}")
            return []
        ads = data.get("data", [])
        # Filter to ads from pages whose name contains our brand hint
        hint = brand.get("page_name_contains", "").lower()
        if hint:
            ads = [a for a in ads if hint.lower() in a.get("page_name", "").lower()]
        print(f"    → {len(ads)} ads found (page name filtered)")
        return ads[:ADS_PER_BRAND]
    except Exception as e:
        print(f"    ✗ Request failed: {e}")
        return []


def get_image_from_snapshot(snapshot_url, token, session):
    """
    Download the ad snapshot HTML page and extract the first usable image URL.
    Meta snapshot pages embed the ad creative in the HTML.
    """
    # Append token to snapshot URL if not already present
    full_url = snapshot_url
    if "access_token" not in snapshot_url and token:
        sep = "&" if "?" in snapshot_url else "?"
        full_url = f"{snapshot_url}{sep}access_token={token}"

    try:
        r = session.get(
            full_url,
            timeout=15,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml",
            },
        )
        html = r.text
        # Look for CDN image URLs in the snapshot HTML
        # Priority: scontent (Facebook CDN) > external > og:image
        patterns = [
            r'src="(https://scontent[^"]+\.(?:jpg|jpeg|png|webp)[^"]*)"',
            r'content="(https://[^"]+\.(?:jpg|jpeg|png|webp)[^"]*?)"',
            r'"(https://[^\s"\'<>]+\.(?:jpg|jpeg|png)[^\s"\'<>]*)"',
        ]
        for pat in patterns:
            matches = re.findall(pat, html)
            for url in matches:
                # Skip tiny icons, tracking pixels, profile pictures
                skip_keywords = ["icon", "pixel", "emoji", "profile_pic", "s60x60", "s32x32", "s40x40"]
                if not any(k in url.lower() for k in skip_keywords):
                    if len(url) > 40:  # not a trivially short URL
                        return url
    except Exception as e:
        print(f"      ✗ Snapshot fetch failed: {e}")
    return None


def download_image(url, dest_path, session):
    """Download an image from URL to dest_path. Returns True on success."""
    try:
        r = session.get(
            url,
            timeout=DOWNLOAD_TIMEOUT,
            stream=True,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://www.facebook.com/",
            },
        )
        if r.status_code != 200:
            return False
        content_type = r.headers.get("Content-Type", "")
        if "image" not in content_type and "octet" not in content_type:
            return False
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        size = dest_path.stat().st_size
        if size < 5000:  # less than 5KB — probably an error page or icon
            dest_path.unlink(missing_ok=True)
            return False
        return True
    except Exception as e:
        print(f"      ✗ Download error: {e}")
        return False


def get_ext_from_url(url):
    path = urlparse(url).path
    ext = os.path.splitext(path)[-1].lower()
    if ext not in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
        ext = ".jpg"
    return ext


def process_meta_brand(brand, token, session):
    """Fetch API ads for one brand, download images, return ads list for JSON."""
    brand_dir = OUTPUT_DIR / brand["slug"]
    brand_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n  [{brand['name']}]")
    raw_ads = fetch_ads_for_brand(brand, token, session)

    ads_out = []
    for i, ad in enumerate(raw_ads):
        snapshot_url = ad.get("ad_snapshot_url", "")
        if not snapshot_url:
            continue

        print(f"    Ad {i+1}/{len(raw_ads)}: extracting image...", end=" ", flush=True)
        img_url = get_image_from_snapshot(snapshot_url, token, session)
        if not img_url:
            print("no image found, skipping")
            continue

        ext = get_ext_from_url(img_url)
        filename = f"ad_{i+1:03d}{ext}"
        dest = brand_dir / filename

        ok = download_image(img_url, dest, session)
        if ok:
            rel_path = f"campaigns/{brand['slug']}/{filename}"
            # Build caption from ad body text if available
            bodies = ad.get("ad_creative_bodies", [])
            caption_text = bodies[0][:120] if bodies else ""
            caption = f"{brand['name']} · Meta Ad · {caption_text}{'…' if len(caption_text) == 120 else ''}"
            ads_out.append({
                "img": rel_path,
                "caption": caption,
                "platform": "meta",
                "ad_id": ad.get("id", ""),
                "snapshot": snapshot_url,
            })
            print(f"✓ saved {filename}")
        else:
            print("download failed, skipping")

        time.sleep(REQUEST_DELAY)

    if not ads_out:
        print(f"    ⚠ No images downloaded for {brand['name']}")

    return {
        "name": brand["name"],
        "slug": brand["slug"],
        "color": brand["color"],
        "ads": ads_out,
    }


def process_manual_brand(brand):
    """Return the manual brand entry as-is (images are remote URLs)."""
    print(f"\n  [{brand['name']}] → {len(brand['ads'])} curated images")
    return {
        "name": brand["name"],
        "slug": brand["slug"],
        "color": brand["color"],
        "ads": brand["ads"],
    }


def main():
    print("\n╔══════════════════════════════════════════╗")
    print("║   Kitchen CRO — Campaign Image Fetcher  ║")
    print("╚══════════════════════════════════════════╝\n")

    OUTPUT_DIR.mkdir(exist_ok=True)

    token = get_token()
    all_results = []

    # Session with retry-friendly settings
    session = requests.Session()
    session.headers.update({"Accept-Encoding": "gzip, deflate"})

    use_api = bool(token)

    if use_api:
        print(f"\n── Meta Ad Library API ({'token provided'}) ──")
        for brand in BRANDS_META:
            result = process_meta_brand(brand, token, session)
            all_results.append(result)
            time.sleep(REQUEST_DELAY)
    else:
        print("\n⚠ No token provided — skipping Meta API brands.")
        print("  To fetch real ad images, re-run with your token.\n")
        # Still include empty entries so the gallery shows all brands
        for brand in BRANDS_META:
            all_results.append({
                "name": brand["name"],
                "slug": brand["slug"],
                "color": brand["color"],
                "ads": [],
            })

    print("\n── Manually Curated Brands ──")
    for brand in BRANDS_MANUAL:
        result = process_manual_brand(brand)
        all_results.append(result)

    # Write campaigns.json
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)

    # Summary
    total_ads = sum(len(b["ads"]) for b in all_results)
    brands_with_ads = sum(1 for b in all_results if b["ads"])
    print(f"\n╔══════════════════════════════════════════╗")
    print(f"║  Done! {total_ads:>3} images across {brands_with_ads:>2} brands          ║")
    print(f"╚══════════════════════════════════════════╝")
    print(f"\n  Output: {JSON_PATH.resolve()}")
    print(f"  Reload http://localhost:5500 to see the gallery.\n")


if __name__ == "__main__":
    main()
