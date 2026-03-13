from __future__ import annotations

import asyncio
import re
from datetime import datetime
from typing import Iterable, List
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from models import Event

HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    )
}


# -----------------------------
# Helpers
# -----------------------------
def clean_lines(text: str) -> List[str]:
    return [line.strip() for line in text.splitlines() if line and line.strip()]


def absolute_url(base: str, href: str) -> str:
    return urljoin(base, href)


def uniq_keep_order(values: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            out.append(value)
    return out


def looks_like_day_date(line: str) -> bool:
    return bool(
        re.search(
            r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+[A-Z][a-z]+\s+\d{1,2}",
            line,
        )
    )


def looks_like_short_month_date(line: str) -> bool:
    return bool(
        re.search(
            r"^[A-Z][a-zA-Z]{2,8}\s+\d{1,2}\s+[•\-]\s+\d{1,2}:\d{2}\s*[APMapm]{2}",
            line,
        )
        or re.search(
            r"^[A-Z]{3}\s+\d{1,2}\s+[•\-]\s+\d{1,2}:\d{2}",
            line,
        )
    )


def parse_date_to_iso(raw: str) -> str | None:
    if not raw:
        return None

    value = raw.strip()
    current_year = datetime.utcnow().year

    patterns = [
        ("%A, %B %d, %I:%M %p", False),
        ("%A, %B %d, %Y %I:%M %p", True),
        ("%b %d %Y", True),
        ("%B %d %Y", True),
        ("%B %d, %Y", True),
        ("%b %d, %Y", True),
        ("%Y-%m-%d", True),
    ]

    for fmt, has_year in patterns:
        try:
            if fmt == "%Y-%m-%d":
                dt = datetime.strptime(value[:10], fmt)
                return dt.strftime("%Y-%m-%d")
            if has_year:
                dt = datetime.strptime(value, fmt)
            else:
                dt = datetime.strptime(value, fmt).replace(year=current_year)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            continue

    try:
        date_part = value.split("•")[0].strip()
        dt = datetime.strptime(f"{date_part} {current_year}", "%b %d %Y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    try:
        date_part = value.split("-")[0].strip()
        dt = datetime.strptime(f"{date_part} {current_year}", "%b %d %Y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    return None


def month_only_to_iso(value: str) -> str | None:
    candidates = [
        ("%B %Y", value),
        ("%b %Y", value),
        ("%B, %Y", value),
        ("%b, %Y", value),
    ]
    for fmt, raw in candidates:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%Y-%m-01")
        except Exception:
            continue
    return None


def extract_first_isoish_date(text: str) -> str | None:
    if not text:
        return None

    value = text.strip()
    current_year = datetime.utcnow().year

    patterns = [
        r"([A-Z][a-z]{2,8}\s+\d{1,2},\s+20\d{2})",
        r"([A-Z][a-z]{2,8}\s+\d{1,2}\s+20\d{2})",
        r"([A-Z][a-z]{2,8}\s+\d{1,2})",
    ]

    for pattern in patterns:
        match = re.search(pattern, value)
        if not match:
            continue

        raw = match.group(1).strip()

        for fmt in ["%B %d, %Y", "%b %d, %Y", "%B %d %Y", "%b %d %Y"]:
            try:
                return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
            except Exception:
                pass

        for fmt in ["%B %d", "%b %d"]:
            try:
                return datetime.strptime(
                    f"{raw} {current_year}",
                    f"{fmt} %Y"
                ).strftime("%Y-%m-%d")
            except Exception:
                pass

    range_match = re.search(
        r"([A-Z][a-z]{2,8})\s+(\d{1,2})\s*[-–]\s*\d{1,2}(?:,\s*20\d{2})?",
        value,
    )
    if range_match:
        month_name = range_match.group(1)
        first_day = range_match.group(2)
        for fmt in ["%B %d %Y", "%b %d %Y"]:
            try:
                return datetime.strptime(
                    f"{month_name} {first_day} {current_year}",
                    fmt
                ).strftime("%Y-%m-%d")
            except Exception:
                pass

    month_match = re.search(r"\b([A-Z][a-z]{2,8}\s+20\d{2})\b", value)
    if month_match:
        return month_only_to_iso(month_match.group(1))

    return None


def infer_event_type(text: str, keyword_map: dict[str, str]) -> str | None:
    lowered = text.lower()
    for needle, normalized in keyword_map.items():
        if needle.lower() in lowered:
            return normalized
    return None


def make_release(
    source: str,
    game: str,
    title: str,
    date_iso: str | None,
    url: str,
    notes: str | None = None,
    image_url: str | None = None,
) -> Event:
    return Event(
        source=source,
        game=game,
        title=title,
        event_type="Release",
        start_date=date_iso,
        url=url,
        image_url=image_url,
        image_alt=title,
        notes=notes,
        location_text=f"{game} official release source",
    )


async def fetch_html(url: str) -> str:
    async with httpx.AsyncClient(
        headers=HEADERS,
        timeout=30,
        follow_redirects=True,
    ) as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.text


async def fetch_page_text(url: str, wait_ms: int = 5000) -> str:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, wait_until="networkidle")
        if wait_ms:
            await page.wait_for_timeout(wait_ms)
        text = await page.locator("body").inner_text()
        await browser.close()
    return text


async def scrape_locator_style_events(
    url: str,
    game: str,
    source: str,
    type_keywords: dict[str, str],
) -> List[Event]:
    text = await fetch_page_text(url, wait_ms=7000)
    lines = clean_lines(text)

    events: List[Event] = []
    i = 0

    while i < len(lines):
        line = lines[i]

        if looks_like_day_date(line) or looks_like_short_month_date(line):
            raw_date = line
            parsed_date = parse_date_to_iso(raw_date)

            title_candidates = []
            if i >= 1:
                title_candidates.append(lines[i - 1])
            if i >= 2:
                title_candidates.append(lines[i - 2])
            if i + 1 < len(lines):
                title_candidates.append(lines[i + 1])

            title = None
            for candidate in title_candidates:
                if candidate and candidate != raw_date and len(candidate) > 2:
                    if not looks_like_day_date(candidate) and not looks_like_short_month_date(candidate):
                        title = candidate
                        break

            title = title or f"{game} Event"

            detail_lines: List[str] = []
            for j in range(i + 1, min(i + 6, len(lines))):
                candidate = lines[j]
                if looks_like_day_date(candidate) or looks_like_short_month_date(candidate):
                    break
                detail_lines.append(candidate)

            detail_blob = " | ".join(detail_lines)
            all_text = " | ".join([title, raw_date, detail_blob])

            subtype = infer_event_type(all_text, type_keywords)
            event_type = f"Play - {subtype}" if subtype else "Play"

            venue = detail_lines[0] if len(detail_lines) >= 1 else None
            location_text = detail_lines[1] if len(detail_lines) >= 2 else None

            events.append(
                Event(
                    source=source,
                    game=game,
                    title=title,
                    event_type=event_type,
                    start_date=parsed_date,
                    venue=venue,
                    location_text=location_text,
                    notes=" | ".join(x for x in [raw_date, detail_blob] if x),
                    url=url,
                )
            )

            i += max(2, len(detail_lines))
            continue

        i += 1

    return events


# -----------------------------
# Release sources
# -----------------------------
async def scrape_mtg_releases() -> List[Event]:
    url = "https://magic.wizards.com/en/news/announcements/everything-announced-for-magic-the-gathering-in-2026"
    html = await fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    og = soup.find("meta", attrs={"property": "og:image"})
    image_url = og["content"] if og and og.get("content") else None

    return [
        make_release("Wizards of the Coast", "Magic: The Gathering", "Lorwyn Eclipsed", "2026-01-23", url, "official exact date", image_url),
        make_release("Wizards of the Coast", "Magic: The Gathering", "Magic: The Gathering | Teenage Mutant Ninja Turtles", "2026-03-01", url, "month-only: March 2026", image_url),
        make_release("Wizards of the Coast", "Magic: The Gathering", "Secrets of Strixhaven", "2026-04-24", url, "official exact date", image_url),
        make_release("Wizards of the Coast", "Magic: The Gathering", "Magic: The Gathering | Marvel Super Heroes", "2026-06-26", url, "official exact date", image_url),
        make_release("Wizards of the Coast", "Magic: The Gathering", "Magic: The Gathering | The Hobbit", "2026-08-01", url, "month-only: August 2026", image_url),
        make_release("Wizards of the Coast", "Magic: The Gathering", "Reality Fracture", "2026-10-01", url, "month-only: October 2026", image_url),
        make_release("Wizards of the Coast", "Magic: The Gathering", "Magic: The Gathering | Star Trek", "2026-11-01", url, "month-only: November 2026", image_url),
    ]


async def scrape_dnd_releases() -> List[Event]:
    url = "https://www.dndbeyond.com/posts/2136-d-d-2026-calendar-release"
    html = await fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    og = soup.find("meta", attrs={"property": "og:image"})
    image_url = og["content"] if og and og.get("content") else None

    return [
        make_release("D&D Beyond", "Dungeons & Dragons", "Ravenloft: The Horrors Within", "2026-04-13", url, "pre-order", image_url),
        make_release("D&D Beyond", "Dungeons & Dragons", "Ravenloft: The Horrors Within", "2026-06-02", url, "Master Tier release", image_url),
        make_release("D&D Beyond", "Dungeons & Dragons", "Ravenloft: The Horrors Within", "2026-06-09", url, "Hero Tier release", image_url),
        make_release("D&D Beyond", "Dungeons & Dragons", "Ravenloft: The Horrors Within", "2026-06-16", url, "wide release", image_url),
        make_release("D&D Beyond", "Dungeons & Dragons", "D&D Reference Cards", "2026-08-01", url, "month-only: August 2026", image_url),
        make_release("D&D Beyond", "Dungeons & Dragons", "Arcana Unleashed", "2026-09-01", url, "month-only: September 2026", image_url),
        make_release("D&D Beyond", "Dungeons & Dragons", "Arcana Unleashed: Deadfall", "2026-09-01", url, "month-only: September 2026", image_url),
    ]


async def scrape_lorcana_releases() -> List[Event]:
    url = "https://www.disneylorcana.com/en-GB/news"
    html = await fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    og = soup.find("meta", attrs={"property": "og:image"})
    image_url = og["content"] if og and og.get("content") else None

    return [
        make_release("Disney Lorcana News", "Disney Lorcana", "Disney Lorcana Collector’s Guides (Sets 1–4 and 5–8)", "2026-02-01", url, "official exact date", image_url),
        make_release("Disney Lorcana News", "Disney Lorcana", "Winterspell Prerelease", "2026-02-13", url, "official exact date", image_url),
        make_release("Disney Lorcana News", "Disney Lorcana", "Winterspell", "2026-02-20", url, "official exact date", image_url),
        make_release("Disney Lorcana News", "Disney Lorcana", "Scrooge McDuck Gift Box", "2026-03-13", url, "official exact date", image_url),
        make_release("Disney Lorcana News", "Disney Lorcana", "Collection Starter Set – Stitch Edition", "2026-03-13", url, "official exact date", image_url),
        make_release("Disney Lorcana News", "Disney Lorcana", "Wilds Unknown Prerelease", "2026-05-08", url, "secondary-confirmed exact date", image_url),
        make_release("Disney Lorcana News", "Disney Lorcana", "Wilds Unknown", "2026-05-15", url, "secondary-confirmed exact date", image_url),
        make_release("Disney Lorcana News", "Disney Lorcana", "2-Player Starter Set", "2026-05-08", url, "official exact date", image_url),
        make_release("Disney Lorcana News", "Disney Lorcana", "Attack of the Vine", "2026-08-01", url, "month-only: August 2026", image_url),
        make_release("Disney Lorcana News", "Disney Lorcana", "Unnamed Fourth Set", "2026-11-01", url, "month-only: Nov/Dec 2026", image_url),
    ]


async def scrape_riftbound_releases() -> List[Event]:
    url = "https://riftbound.leagueoflegends.com/en-us/news/announcements/2026-roadmap/"
    html = await fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    og = soup.find("meta", attrs={"property": "og:image"})
    image_url = og["content"] if og and og.get("content") else None

    return [
        make_release("Riftbound 2026 Roadmap", "Riftbound", "Spiritforged Pre-Rift", "2026-02-06", url, "official exact start date", image_url),
        make_release("Riftbound 2026 Roadmap", "Riftbound", "Spiritforged (English)", "2026-02-13", url, "official exact date", image_url),
        make_release("Riftbound 2026 Roadmap", "Riftbound", "Unleashed (China)", "2026-04-10", url, "secondary-confirmed exact date; roadmap confirms Unleashed in 2026", image_url),
        make_release("Riftbound 2026 Roadmap", "Riftbound", "Unleashed (Global English)", "2026-05-08", url, "secondary-confirmed exact date; roadmap confirms Unleashed in 2026", image_url),
        make_release("Riftbound 2026 Roadmap", "Riftbound", "Vendetta", "2026-07-31", url, "secondary-confirmed exact date; official sources confirm later-2026 release window", image_url),
        make_release("Riftbound 2026 Roadmap", "Riftbound", "Radiance", "2026-10-23", url, "secondary-confirmed exact date; official sources confirm year-end release", image_url),
    ]


async def scrape_star_wars_releases() -> List[Event]:
    url = "https://starwarsunlimited.com/articles/a-message-from-the-team"
    html = await fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    og = soup.find("meta", attrs={"property": "og:image"})
    image_url = og["content"] if og and og.get("content") else None

    return [
        make_release("Star Wars Unlimited", "Star Wars: Unlimited", "A Lawless Time", "2026-03-13", url, "official exact date", image_url),
        make_release("Star Wars Unlimited", "Star Wars: Unlimited", "Twin Suns Decks", "2026-05-01", url, "month-only: May 2026", image_url),
        make_release("Star Wars Unlimited", "Star Wars: Unlimited", "Ashes of the Empire", "2026-06-01", url, "season-only: Summer 2026", image_url),
        make_release("Star Wars Unlimited", "Star Wars: Unlimited", "Homeworlds", "2026-10-01", url, "quarter-only: Q4 2026", image_url),
        make_release("Star Wars Unlimited", "Star Wars: Unlimited", "Icons", "2026-12-01", url, "officially named as 2026 set; exact month/day not verified in source", image_url),
    ]


async def scrape_one_piece_releases() -> List[Event]:
    url = "https://en.onepiece-cardgame.com/products/"
    return [
        make_release("ONE PIECE Official", "One Piece", "BOOSTER PACK -THE AZURE SEA’S SEVEN- [OP14-EB04]", "2026-01-16", url, "official exact date"),
        make_release("ONE PIECE Official", "One Piece", "STARTER DECK -Egghead- [ST-29]", "2026-01-16", url, "official exact date"),
        make_release("ONE PIECE Official", "One Piece", "Tin Pack Set Vol.2 [TS-02]", "2026-01-30", url, "official exact date"),
        make_release("ONE PIECE Official", "One Piece", "EXTRA BOOSTER -ONE PIECE HEROINES EDITION- [EB-03]", "2026-02-20", url, "official exact date"),
        make_release("ONE PIECE Official", "One Piece", "BOOSTER PACK -ADVENTURE ON KAMI'S ISLAND- [OP15-EB04]", "2026-04-03", url, "official exact date"),
        make_release("ONE PIECE Official", "One Piece", "Double Pack Set Vol.10 [DP-10]", "2026-04-03", url, "official exact date"),
    ]


async def scrape_gundam_releases() -> List[Event]:
    url = "https://www.gundam-gcg.com/en/news/1stanniversary.html"
    html = await fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    og = soup.find("meta", attrs={"property": "og:image"})
    image_url = og["content"] if og and og.get("content") else None

    return [
        make_release("GUNDAM Official", "Gundam Card Game", "Freedom Ascension [GD05]", "2026-07-24", url, "official exact date", image_url),
    ]


async def scrape_pokemon_releases() -> List[Event]:
    url = "https://www.pokemon.com/us/pokemon-news/check-out-every-pokemon-tcg-product-release-in-march-2026"
    html = await fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    og = soup.find("meta", attrs={"property": "og:image"})
    image_url = og["content"] if og and og.get("content") else None

    return [
        make_release("Pokémon", "Pokémon", "Mega Evolution—Perfect Order", "2026-03-27", url, "official exact date", image_url),
        make_release("Pokémon", "Pokémon", "Mega Evolution—Chaos Rising", "2026-05-22", url, "official exact date", image_url),
    ]


# -----------------------------
# Play sources
# -----------------------------
async def scrape_wpn_events() -> List[Event]:
    url = "https://wpn.wizards.com/en/events"
    html = await fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)

    events: List[Event] = []
    for line in clean_lines(text):
        subtype = infer_event_type(
            line,
            {
                "Commander": "Commander",
                "Draft": "Draft",
                "Modern": "Modern",
                "Standard": "Standard",
                "cEDH": "cEDH",
            },
        )
        if subtype:
            events.append(
                Event(
                    source="WPN",
                    game="Magic: The Gathering",
                    title=line.strip(),
                    event_type=f"Play - {subtype}",
                    raw_category="Magic WPN program",
                    url=url,
                )
            )
    return events


async def scrape_magic_locator() -> List[Event]:
    return await scrape_locator_style_events(
        url="https://locator.wizards.com/",
        game="Magic: The Gathering",
        source="Wizards Locator",
        type_keywords={
            "Commander": "Commander",
            "Draft": "Draft",
            "Modern": "Modern",
            "Standard": "Standard",
            "cEDH": "cEDH",
        },
    )


async def scrape_dnd_locator() -> List[Event]:
    return await scrape_locator_style_events(
        url="https://locator.wizards.com/",
        game="Dungeons & Dragons",
        source="Wizards Locator",
        type_keywords={
            "Adventurers League": "Adventurers League",
            "Ladies D&D": "Ladies Night",
            "Ladies D&D Night": "Ladies Night",
            "Ladies Night": "Ladies Night",
            "Book Release": "Book Release",
            "D&D": "D&D Event",
        },
    )


async def scrape_pokemon_locator() -> List[Event]:
    return await scrape_locator_style_events(
        url="https://events.pokemon.com/EventLocator/?locale=en-us",
        game="Pokémon",
        source="Play! Pokémon",
        type_keywords={
            "League Challenge": "League Challenge",
            "League Cup": "League Cup",
            "Prerelease": "Prerelease",
            "Regional Championships": "Regionals",
            "League": "League",
        },
    )


async def scrape_lorcana_locator() -> List[Event]:
    url = "https://tcg.ravensburgerplay.com/events"
    text = await fetch_page_text(url, wait_ms=7000)
    lines = clean_lines(text)

    events: List[Event] = []
    i = 0

    while i < len(lines):
        line = lines[i]

        if looks_like_day_date(line) or looks_like_short_month_date(line):
            raw_date = line
            parsed_date = parse_date_to_iso(raw_date)

            title = lines[i + 1] if i + 1 < len(lines) else "Disney Lorcana Event"
            detail_1 = lines[i + 2] if i + 2 < len(lines) else ""
            detail_2 = lines[i + 3] if i + 3 < len(lines) else ""

            subtype = infer_event_type(
                " | ".join([title, detail_1, detail_2]),
                {
                    "Set Championship": "Set Championship",
                    "Weekly Play": "Weekly Play",
                    "Draft": "Draft",
                    "Sealed": "Sealed",
                    "Constructed": "Constructed",
                    "Challenge": "Challenge",
                    "Casual": "Casual Play",
                },
            )

            events.append(
                Event(
                    source="Ravensburger Play Hub",
                    game="Disney Lorcana",
                    title=title,
                    event_type=f"Play - {subtype}" if subtype else "Play",
                    start_date=parsed_date,
                    venue=detail_1 or None,
                    location_text=detail_2 or None,
                    notes=raw_date,
                    url=url,
                )
            )
            i += 4
            continue

        i += 1

    return events


async def scrape_riftbound_events() -> List[Event]:
    url = "https://locator.riftbound.uvsgames.com/events"
    text = await fetch_page_text(url, wait_ms=7000)
    lines = clean_lines(text)

    events: List[Event] = []
    i = 0

    while i < len(lines):
        line = lines[i]

        if looks_like_day_date(line) or looks_like_short_month_date(line):
            raw_date = line
            parsed_date = parse_date_to_iso(raw_date)

            title = lines[i + 1] if i + 1 < len(lines) else "Riftbound Event"
            detail_1 = lines[i + 2] if i + 2 < len(lines) else ""
            detail_2 = lines[i + 3] if i + 3 < len(lines) else ""

            subtype = infer_event_type(
                " | ".join([title, detail_1, detail_2]),
                {
                    "Starter Deck": "Starter Deck Event",
                    "Learn to Play": "Learn to Play",
                    "Sealed": "Sealed",
                    "Draft": "Draft",
                    "Constructed": "Constructed",
                    "Casual": "Casual Play",
                    "Tournament": "Tournament",
                    "Weekly": "Weekly Play",
                },
            )

            events.append(
                Event(
                    source="Riftbound Locator",
                    game="Riftbound",
                    title=title,
                    event_type=f"Play - {subtype}" if subtype else "Play",
                    start_date=parsed_date,
                    venue=detail_1 or None,
                    location_text=detail_2 or None,
                    notes=raw_date,
                    url=url,
                )
            )
            i += 4
            continue

        i += 1

    return events


async def scrape_star_wars_locator() -> List[Event]:
    return await scrape_locator_style_events(
        url=(
            "https://starwarsunlimited.com/search"
            "?distance=100&myLocation=false"
            "&geo=35.7890402%7C-78.77976439999999%7CCary%2C+NC%2C+USA"
            "&type=events"
        ),
        game="Star Wars: Unlimited",
        source="Star Wars Unlimited",
        type_keywords={
            "Planetary Qualifier": "Planetary Qualifier",
            "Premier": "Premier",
            "Draft": "Draft",
            "Showdown": "Showdown",
            "Store Showdown": "Store Showdown",
            "Sealed": "Sealed",
        },
    )


async def scrape_one_piece_events() -> List[Event]:
    root_url = "https://en.onepiece-cardgame.com/events/"
    html = await fetch_html(root_url)
    soup = BeautifulSoup(html, "html.parser")

    links: List[str] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue
        full = absolute_url(root_url, href)
        if "/events/" in full and full not in links:
            links.append(full)

    links = uniq_keep_order(links)

    events: List[Event] = []
    for url in links:
        try:
            page_html = await fetch_html(url)
            page_soup = BeautifulSoup(page_html, "html.parser")
            text = page_soup.get_text("\n", strip=True)
            lines = clean_lines(text)

            title = None
            h1 = page_soup.find("h1")
            if h1:
                title = h1.get_text(" ", strip=True)
            if not title and lines:
                title = lines[0]

            subtype = infer_event_type(
                title or text,
                {
                    "Treasure Cup": "Treasure Cup",
                    "Championship": "Championship",
                    "Regional": "Regional",
                    "Store Championship": "Store Championship",
                    "Convention": "Convention",
                },
            )

            image_url = None
            og_image = page_soup.find("meta", attrs={"property": "og:image"})
            if og_image and og_image.get("content"):
                image_url = absolute_url(url, og_image["content"])

            date_line = None
            for line in lines:
                if re.search(r"\b20\d{2}\b", line) or "Jan" in line or "Feb" in line or "Mar" in line:
                    date_line = line
                    break

            events.append(
                Event(
                    source="ONE PIECE Official",
                    game="One Piece",
                    title=title or "One Piece Event",
                    event_type=f"Play - {subtype}" if subtype else "Play",
                    start_date=extract_first_isoish_date(date_line or text),
                    url=url,
                    image_url=image_url,
                    image_alt=title,
                    notes=date_line,
                )
            )
        except Exception as exc:
            print(f"One Piece scrape failed for {url}: {exc}")

    return events


async def scrape_gundam_events() -> List[Event]:
    root_url = "https://www.gundam-gcg.com/en/events/"
    html = await fetch_html(root_url)
    soup = BeautifulSoup(html, "html.parser")

    detail_links: List[str] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue

        full_url = absolute_url(root_url, href)
        if "/events/" not in full_url:
            continue
        if not full_url.endswith(".html"):
            continue
        if full_url == root_url:
            continue

        detail_links.append(full_url)

    detail_links = uniq_keep_order(detail_links)

    events: List[Event] = []
    async with httpx.AsyncClient(
        headers=HEADERS,
        timeout=30,
        follow_redirects=True,
    ) as client:
        for detail_url in detail_links:
            try:
                response = await client.get(detail_url)
                response.raise_for_status()
                page_html = response.text
                page_soup = BeautifulSoup(page_html, "html.parser")
                text = page_soup.get_text("\n", strip=True)
                lines = clean_lines(text)

                title = None
                h1 = page_soup.find("h1")
                if h1:
                    title = h1.get_text(" ", strip=True)
                if not title:
                    title_tag = page_soup.find("title")
                    if title_tag:
                        title = title_tag.get_text(" ", strip=True)
                if not title and lines:
                    title = lines[0]

                event_period = None
                for idx, line in enumerate(lines):
                    normalized = line.strip()
                    if normalized in {"Event Period", "Period", "Date"}:
                        if idx + 1 < len(lines):
                            event_period = lines[idx + 1].strip()
                            break

                if not event_period:
                    for line in lines:
                        if re.search(r"[A-Z][a-z]{2,8}\s+\d{1,2}\s*[-–]\s*\d{1,2}(?:,\s*20\d{2})?", line):
                            event_period = line.strip()
                            break

                image_url = None
                og_image = page_soup.find("meta", attrs={"property": "og:image"})
                if og_image and og_image.get("content"):
                    image_url = absolute_url(detail_url, og_image["content"])
                if not image_url:
                    img = page_soup.find("img")
                    if img and img.get("src"):
                        image_url = absolute_url(detail_url, img["src"])

                subtype = infer_event_type(
                    title or text,
                    {
                        "Store Tournament": "Store Tournament",
                        "Store Championship": "Store Championship",
                        "Store Championships": "Store Championship",
                        "TEAM BATTLE": "Team Battle",
                        "WORLD CHAMPIONSHIPS": "World Championships",
                        "Regional": "Regional",
                    },
                )

                events.append(
                    Event(
                        source="GUNDAM Official",
                        game="Gundam Card Game",
                        title=title or detail_url.rsplit("/", 1)[-1],
                        event_type=f"Play - {subtype}" if subtype else "Play",
                        start_date=extract_first_isoish_date(event_period or text),
                        url=detail_url,
                        image_url=image_url,
                        image_alt=title,
                        notes=" | ".join([x for x in [event_period, detail_url] if x]),
                        location_text="Official Gundam event page",
                    )
                )
            except Exception as exc:
                print(f"Gundam scrape failed for {detail_url}: {exc}")

    return events


async def scrape_all() -> List[Event]:
    batches = await asyncio.gather(
        # Releases
        scrape_mtg_releases(),
        scrape_dnd_releases(),
        scrape_lorcana_releases(),
        scrape_riftbound_releases(),
        scrape_star_wars_releases(),
        scrape_one_piece_releases(),
        scrape_gundam_releases(),
        scrape_pokemon_releases(),

        # Play
        scrape_wpn_events(),
        scrape_magic_locator(),
        scrape_dnd_locator(),
        scrape_pokemon_locator(),
        scrape_lorcana_locator(),
        scrape_riftbound_events(),
        scrape_star_wars_locator(),
        scrape_one_piece_events(),
        scrape_gundam_events(),
        return_exceptions=True,
    )

    events: List[Event] = []
    for batch in batches:
        if isinstance(batch, Exception):
            print(f"Scraper failed: {batch}")
            continue
        events.extend(batch)

    deduped = {}
    for event in events:
        deduped[event.dedupe_key()] = event

    return sorted(
        deduped.values(),
        key=lambda e: (
            e.game or "",
            e.start_date or "9999-99-99",
            e.title or "",
        ),
    )
