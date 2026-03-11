from __future__ import annotations

import asyncio
import re
from typing import Iterable, List

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
        if "Commander" in line or "Draft" in line or "Modern" in line or "Standard" in line:
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
                Event(source="ONE PIECE", game="One Piece", title=line.strip(), url=url)
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
            events.append(Event(source="GUNDAM", game="Gundam Card Game", title=line.strip(), url=url))
    return events


async def scrape_riftbound_events() -> List[Event]:
    url = "https://riftbound.leagueoflegends.com/en-us/news/announcements/2026-roadmap/"
    async with httpx.AsyncClient(headers=HEADERS, timeout=30) as client:
        html = (await client.get(url)).text
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    return sorted(deduped.values(), key=lambda e: ((e.game or ""), (e.start_date or "9999-99-99"), e.title))