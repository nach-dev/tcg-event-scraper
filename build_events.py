from __future__ import annotations

import asyncio
from pathlib import Path
import orjson

from scrapers import scrape_all


def main() -> None:
    events = asyncio.run(scrape_all())
    payload = [e.model_dump() for e in events]
    out_dir = Path("site/data")
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "events.json").write_bytes(orjson.dumps(payload, option=orjson.OPT_INDENT_2))
    print(f"Wrote {len(payload)} events")


if __name__ == "__main__":
    main()