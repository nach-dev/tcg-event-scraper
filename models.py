from __future__ import annotations

from typing import Optional
from pydantic import BaseModel


class Event(BaseModel):
    source: str
    game: str
    title: str
    event_type: Optional[str] = None
    format: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    city: Optional[str] = None
    region: Optional[str] = None
    country: Optional[str] = None
    venue: Optional[str] = None
    location_text: Optional[str] = None
    url: Optional[str] = None
    notes: Optional[str] = None
    raw_category: Optional[str] = None
    image_url: Optional[str] = None
    image_alt: Optional[str] = None

    def dedupe_key(self) -> str:
        return "|".join(
            [
                (self.game or "").strip().lower(),
                (self.title or "").strip().lower(),
                (self.start_date or "").strip().lower(),
                (self.venue or self.location_text or "").strip().lower(),
                (self.city or "").strip().lower(),
                (self.country or "").strip().lower(),
            ]
        )
