from __future__ import annotations

import asyncio
import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import orjson

from scrapers import scrape_all


def event_month(value: str | None) -> str:
    if not value:
        return "unknown"

    value = value.strip()
    if not value:
        return "unknown"

    try:
        return datetime.strptime(value[:10], "%Y-%m-%d").strftime("%Y-%m")
    except Exception:
        return "unknown"


def display_date(event_date: str | None, notes: str | None) -> str:
    if event_date:
        try:
            return datetime.strptime(event_date[:10], "%Y-%m-%d").strftime("%b %d, %Y")
        except Exception:
            return event_date

    if notes and "month-only" in notes.lower():
        return notes.replace("month-only:", "").strip()

    return ""


def event_kind(event_type: str | None) -> str:
    value = (event_type or "").lower()
    if value == "release":
        return "release"
    if value.startswith("play"):
        return "play"
    return "other"


def main() -> None:
    events = asyncio.run(scrape_all())

    rows = []
    for e in events:
        description_parts = [
            e.event_type or e.format or e.raw_category,
            e.venue,
            e.location_text,
            e.notes,
        ]
        description = " | ".join([x for x in description_parts if x])

        rows.append(
            {
                "month": event_month(e.start_date),
                "kind": event_kind(e.event_type),
                "game_type": e.game,
                "event_type": e.event_type or e.format or e.raw_category,
                "event_name": e.title,
                "event_date": e.start_date,
                "event_date_display": display_date(e.start_date, e.notes),
                "event_description": description,
                "source_site": e.source,
                "source_url": e.url,
                "image_url": getattr(e, "image_url", None),
                "image_alt": getattr(e, "image_alt", None),
                "location_text": e.location_text
                or " · ".join([x for x in [e.venue, e.city, e.region, e.country] if x]),
                "notes": e.notes,
            }
        )

    rows.sort(
        key=lambda r: (
            r["kind"] or "",
            r["month"] or "9999-99",
            r["game_type"] or "",
            r["event_date"] or "9999-99-99",
            r["event_name"] or "",
        )
    )

    grouped = defaultdict(list)
    for row in rows:
        grouped[row["month"]].append(row)

    out_dir = Path("site/data")
    out_dir.mkdir(parents=True, exist_ok=True)

    (out_dir / "events-by-month.json").write_bytes(
        orjson.dumps(dict(grouped), option=orjson.OPT_INDENT_2)
    )

    with (out_dir / "events-verification.csv").open(
        "w", newline="", encoding="utf-8"
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "month",
                "kind",
                "game_type",
                "event_type",
                "event_name",
                "event_date",
                "event_date_display",
                "event_description",
                "source_site",
                "source_url",
                "image_url",
                "image_alt",
                "location_text",
                "notes",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows")


if __name__ == "__main__":
    main()
