"""
TMDb (The Movie Database) source adapter.
Fetches new/popular movies and TV shows from TMDb API.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any, AsyncIterator, Dict, List

import structlog

from .base import APISource, SourceConfig
from ..shared.types import MediaType, RawRelease

logger = structlog.get_logger(__name__)


class TMDbSource(APISource):
    """TMDb API source for discovering new releases."""

    def __init__(self, config: SourceConfig):
        super().__init__(config)
        self.api_key = config.config.get("api_key")
        self.language = config.config.get("language", "en-US")
        self.regions = config.config.get("regions", ["US"])
        self.include_adult = config.config.get("include_adult", False)

        # Set up base API URL
        if not self.api_key:
            raise ValueError("TMDb API key required")

        self.base_url = "https://api.themoviedb.org/3"
        self.config.config["url"] = self.base_url
        self.config.config["headers"] = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }
        self.config.config["params"] = {
            "language": self.language,
            "include_adult": str(self.include_adult).lower(),
        }

    def extract_items(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        return data.get("results", [])

    def has_more_pages(self, data: Dict[str, Any], current_page: int) -> bool:
        total_pages = data.get("total_pages", 1)
        return current_page < total_pages

    async def fetch_releases(self) -> AsyncIterator[RawRelease]:
        """Fetch new releases from multiple TMDb endpoints."""
        # Fetch from different endpoints concurrently
        tasks = [
            self._fetch_now_playing_movies(),
            self._fetch_upcoming_movies(),
            self._fetch_airing_today_tv(),
            self._fetch_on_the_air_tv(),
        ]

        for task in asyncio.as_completed(tasks):
            async for release in task:
                yield release

    async def _fetch_now_playing_movies(self) -> AsyncIterator[RawRelease]:
        """Fetch movies currently in theaters."""
        url = f"{self.base_url}/movie/now_playing"
        params = {**self.config.config["params"], "page": 1}

        async for release in self._paginate_and_parse(url, params, MediaType.MOVIE, "now_playing"):
            yield release

    async def _fetch_upcoming_movies(self) -> AsyncIterator[RawRelease]:
        """Fetch upcoming movie releases."""
        url = f"{self.base_url}/movie/upcoming"
        params = {**self.config.config["params"], "page": 1}

        async for release in self._paginate_and_parse(url, params, MediaType.MOVIE, "upcoming"):
            yield release

    async def _fetch_airing_today_tv(self) -> AsyncIterator[RawRelease]:
        """Fetch TV episodes airing today."""
        url = f"{self.base_url}/tv/airing_today"
        params = {**self.config.config["params"], "page": 1}

        async for release in self._paginate_and_parse(url, params, MediaType.TV, "airing_today"):
            yield release

    async def _fetch_on_the_air_tv(self) -> AsyncIterator[RawRelease]:
        """Fetch TV shows currently on air."""
        url = f"{self.base_url}/tv/on_the_air"
        params = {**self.config.config["params"], "page": 1}

        async for release in self._paginate_and_parse(url, params, MediaType.TV, "on_the_air"):
            yield release

    async def _paginate_and_parse(
        self, url: str, params: Dict[str, Any], media_type: MediaType, source_tag: str
    ) -> AsyncIterator[RawRelease]:
        max_pages = self.get_config("pagination", {}).get("max_pages", 5)
        page = 1

        while page <= max_pages:
            await self.rate_limiter.acquire()

            params["page"] = page

            try:
                response = await self.client.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                for item in data.get("results", []):
                    yield self.parse_item(item, media_type, source_tag)

                if not self.has_more_pages(data, page):
                    break

                page += 1

            except Exception as e:
                logger.error(
                    "tmdb_fetch_failed",
                    url=url,
                    page=page,
                    media_type=media_type.value,
                    error=str(e),
                )
                break

    def parse_item(self, item: Dict[str, Any], media_type: MediaType = None, source_tag: str = "") -> RawRelease:
        """Parse TMDb item into RawRelease."""
        if media_type is None:
            media_type = MediaType.MOVIE if "title" in item else MediaType.TV

        # Extract common fields
        tmdb_id = str(item.get("id"))
        title = item.get("title") or item.get("name") or "Unknown"
        original_title = item.get("original_title") or item.get("original_name")
        overview = item.get("overview", "")
        release_date = item.get("release_date") or item.get("first_air_date")
        poster_path = item.get("poster_path")
        backdrop_path = item.get("backdrop_path")
        genre_ids = item.get("genre_ids", [])
        vote_average = item.get("vote_average")
        vote_count = item.get("vote_count")
        popularity = item.get("popularity")

        # Extract year
        year = None
        if release_date:
            try:
                year = int(release_date[:4])
            except (ValueError, IndexError):
                pass

        # Determine media type specific fields
        season = None
        episode = None
        if media_type == MediaType.TV:
            # For TV, we might get show-level data
            # Episodes would need separate API calls
            pass

        # Build source URL
        if media_type == MediaType.MOVIE:
            source_url = f"https://www.themoviedb.org/movie/{tmdb_id}"
        else:
            source_url = f"https://www.themoviedb.org/tv/{tmdb_id}"

        # Build source data for metadata matching
        source_data = {
            "tmdb_id": tmdb_id,
            "media_type": media_type.value,
            "overview": overview,
            "poster_path": poster_path,
            "backdrop_path": backdrop_path,
            "genre_ids": genre_ids,
            "vote_average": vote_average,
            "vote_count": vote_count,
            "popularity": popularity,
            "original_language": item.get("original_language"),
            "source_tag": source_tag,
        }

        return RawRelease(
            source_id=self.config.id,
            source_url=source_url,
            source_id_external=tmdb_id,
            title=title,
            original_title=original_title,
            year=year,
            season=season,
            episode=episode,
            media_type=media_type,
            release_date=release_date,
            source_data=source_data,
        )

    async def check_health(self) -> bool:
        try:
            await self.rate_limiter.acquire()
            response = await self.client.get(
                f"{self.base_url}/configuration",
                headers=self.config.config["headers"],
            )
            return response.status_code == 200
        except Exception:
            return False


class TMDbAnimeSource(TMDbSource):
    """TMDb source filtered for anime content."""

    def __init__(self, config: SourceConfig):
        super().__init__(config)
        # Anime genre IDs on TMDb: 16 (Animation), 99 (Documentary - sometimes anime)
        self.anime_genre_ids = config.config.get("anime_genre_ids", [16])

    async def _fetch_anime_movies(self) -> AsyncIterator[RawRelease]:
        """Fetch anime movies using discover endpoint."""
        url = f"{self.base_url}/discover/movie"
        params = {
            **self.config.config["params"],
            "with_genres": ",".join(map(str, self.anime_genre_ids)),
            "sort_by": "release_date.desc",
            "page": 1,
        }

        async for release in self._paginate_and_parse(url, params, MediaType.ANIME, "anime_movies"):
            yield release

    async def _fetch_anime_tv(self) -> AsyncIterator[RawRelease]:
        """Fetch anime TV shows using discover endpoint."""
        url = f"{self.base_url}/discover/tv"
        params = {
            **self.config.config["params"],
            "with_genres": ",".join(map(str, self.anime_genre_ids)),
            "sort_by": "first_air_date.desc",
            "page": 1,
        }

        async for release in self._paginate_and_parse(url, params, MediaType.ANIME, "anime_tv"):
            yield release

    async def fetch_releases(self) -> AsyncIterator[RawRelease]:
        tasks = [
            self._fetch_anime_movies(),
            self._fetch_anime_tv(),
        ]

        for task in asyncio.as_completed(tasks):
            async for release in task:
                yield release