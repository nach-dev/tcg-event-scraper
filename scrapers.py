from __future__ import annotations

import asyncio
from typing import List

import httpx
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from models import Event

HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
}


async def scrape_wpn_events() -> List[Event]:
    url = "https://wpn.wizards.com/en/events"
    async with httpx.AsyncClient(headers=HEADERS, timeout=30) as client:
        html = (await client.get(url)).text
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)

    events: List[Event] = []
    for line in text.splitlines():
        if any(x in line for x in ["Commander", "Draft", "Modern", "Standard", "cEDH"]):
            events.append(
                Event(
                    source="WPN",
                    game="Magic: The Gathering",
                    title=line.strip(),
                    raw_category="Magic WPN program",
                    url=url,
                )
            )
    return events


async def scrape_one_piece_events() -> List[Event]:
    url = "https://en.onepiece-cardgame.com/events/"
    async with httpx.AsyncClient(headers=HEADERS, timeout=30) as client:
        html = (await client.get(url)).text
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)

    events: List[Event] = []
    for line in text.splitlines():
        if any(x in line for x in ["Championship", "Store Championship", "Treasure Cup", "Convention"]):
            events.append(
                Event(
                    source="ONE PIECE",
                    game="One Piece",
                    title=line.strip(),
                    url=url,
                )
            )
    return events


async def scrape_gundam_events() -> List[Event]:
    url = "https://www.gundam-gcg.com/en/events/"
    async with httpx.AsyncClient(headers=HEADERS, timeout=30) as client:
        html = (await client.get(url)).text
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)

    events: List[Event] = []
    for line in text.splitlines():
        if any(x in line for x in ["Store Tournament", "Store Championships", "2v2 TEAM BATTLE", "WORLD CHAMPIONSHIPS"]):
            events.append(
                Event(
                    source="GUNDAM",
                    game="Gundam Card Game",
                    title=line.strip(),
                    url=url,
                )
            )
    return events


async def scrape_riftbound_events() -> List[Event]:
    url = "https://riftbound.leagueoflegends.com/en-us/news/announcements/2026-roadmap/"
    async with httpx.AsyncClient(headers=HEADERS, timeout=30) as client:
        html = (await client.get(url)).text
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)

    patterns = ["Summoner Skirmish", "Regional Qualifier", "MomoCon", "Minor Tournaments"]
    events: List[Event] = []

    for line in text.splitlines():
        if any(p in line for p in patterns):
            events.append(
                Event(
                    source="Riftbound",
                    game="Riftbound",
                    title=line.strip(),
                    url=url,
                )
            )
    return events


async def scrape_with_browser(
    url: str,
    game: str,
    source: str,
    keyword_map: dict[str, str],
) -> List[Event]:
    events: List[Event] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, wait_until="networkidle")
        text = await page.locator("body").inner_text()
        await browser.close()

    for line in text.splitlines():
        for needle, normalized in keyword_map.items():
            if needle.lower() in line.lower():
                events.append(
                    Event(
                        source=source,
                        game=game,
                        title=line.strip(),
                        raw_category=normalized,
                        url=url,
                    )
                )
                break

    return events


async def scrape_magic_locator() -> List[Event]:
    return await scrape_with_browser(
        url="https://locator.wizards.com/",
        game="Magic: The Gathering",
        source="Wizards Locator",
        keyword_map={
            "Commander": "Commander",
            "Draft": "Draft",
            "Modern": "Modern",
            "Standard": "Standard",
            "cEDH": "cEDH",
        },
    )


async def scrape_dnd_locator() -> List[Event]:
    return await scrape_with_browser(
        url="https://locator.wizards.com/",
        game="Dungeons & Dragons",
        source="Wizards Locator",
        keyword_map={
            "Adventurers League": "Adventurers League",
            "Ladies D&D": "Ladies Night",
            "Ladies D&D Night": "Ladies Night",
            "Book Release": "Book Release",
        },
    )


async def scrape_pokemon_locator() -> List[Event]:
    return await scrape_with_browser(
        url="https://events.pokemon.com/EventLocator/?locale=en-us",
        game="Pokémon",
        source="Play! Pokémon",
        keyword_map={
            "League": "League",
            "League Challenge": "League Challenge",
            "League Cup": "League Cup",
            "Prerelease": "Prerelease",
            "Regional Championships": "Regionals",
        },
    )


async def scrape_lorcana_locator() -> List[Event]:
    return await scrape_with_browser(
        url="https://www.disneylorcana.com/en-US/play",
        game="Disney Lorcana",
        source="Ravensburger",
        keyword_map={
            "Challenge": "Challenge",
            "League": "League",
            "Set Championship": "Set Championship",
            "Casual": "Casual Play",
        },
    )


async def scrape_star_wars_locator() -> List[Event]:
    return await scrape_with_browser(
        url="https://starwarsunlimited.com/search?type=events",
        game="Star Wars: Unlimited",
        source="SWU Event Locator",
        keyword_map={
            "Planetary Qualifier": "Planetary Qualifier",
            "Premier": "Premier",
            "Showdown": "Showdown",
            "Store Showdown": "Store Showdown",
        },
    )


async def scrape_all() -> List[Event]:
    batches = await asyncio.gather(
        scrape_wpn_events(),
        scrape_one_piece_events(),
        scrape_gundam_events(),
        scrape_riftbound_events(),
        scrape_magic_locator(),
        scrape_dnd_locator(),
        scrape_pokemon_locator(),
        scrape_lorcana_locator(),
        scrape_star_wars_locator(),
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
        key = "|".join(
            [
                (event.game or "").strip().lower(),
                (event.title or "").strip().lower(),
                (event.start_date or "").strip().lower(),
                (event.venue or event.location_text or "").strip().lower(),
                (event.city or "").strip().lower(),
                (event.country or "").strip().lower(),
            ]
        )
        deduped[key] = event

    return sorted(
        deduped.values(),
        key=lambda e: ((e.game or ""), (e.start_date or "9999-99-99"), e.title or ""),
    )
