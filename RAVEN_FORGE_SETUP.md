# Raven Forge store-only feed

The scraper now writes `site/data/raven-forge-events.json` in addition to the
existing national event files. The website should consume only this new file.

Connected official store pages:

- Disney Lorcana: Raven Forge Games store ID `d9a4caf9-daf1-43d2-8c26-83879cccd8a9`
- Magic: The Gathering: Wizards store ID `14156`
- Pokémon: Play! Pokémon location GUID `f6cba702-09c8-c1d4-cf12-ad908a8096fd`

One Piece and Gundam are intentionally marked `needs_store_url` until
an official public Raven Forge store page is available. National events are not
allowed into the store-only feed.

The existing GitHub Actions workflow already runs `build_events.py` and commits
all files under `site/data`, so no workflow change is required.
