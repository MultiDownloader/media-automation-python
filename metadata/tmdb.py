"""
TMDb metadata matching and retrieval.
"""

from __future__ import annotations

import asyncio
import structlog
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx

from ..shared.config import get_settings
from ..shared.types import MediaType, ExternalIdentifier, MetadataSnapshot

logger = structlog.get_logger(__name__)


class TMDbClient:
    """TMDb API client for metadata retrieval."""

    def __init__(self, api_key: str = None, language: str = "en-US"):
        self.settings = get_settings()
        self.api_key = api_key or self.settings.tmdb_api_key
        self.language = language
        self.base_url = "https://api.themoviedb.org/3"
        self.image_base_url = "https://image.tmdb.org/t/p"
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "TMDbClient":
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Accept": "application/json",
            },
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._client:
            await self._client.aclose()

    def _params(self, **extra) -> Dict[str, Any]:
        return {"language": self.language, **extra}

    async def search_movie(self, query: str, year: int = None) -> List[Dict[str, Any]]:
        """Search for movies."""
        params = self._params(query=query, include_adult="false")
        if year:
            params["year"] = year

        response = await self._client.get(f"{self.base_url}/search/movie", params=params)
        response.raise_for_status()
        return response.json().get("results", [])

    async def search_tv(self, query: str, year: int = None) -> List[Dict[str, Any]]:
        """Search for TV shows."""
        params = self._params(query=query, include_adult="false")
        if year:
            params["first_air_date_year"] = year

        response = await self._client.get(f"{self.base_url}/search/tv", params=params)
        response.raise_for_status()
        return response.json().get("results", [])

    async def get_movie_details(self, movie_id: int) -> Dict[str, Any]:
        """Get detailed movie information."""
        params = self._params(
            append_to_response="credits,external_ids,keywords,release_dates,videos,images"
        )
        response = await self._client.get(f"{self.base_url}/movie/{movie_id}", params=params)
        response.raise_for_status()
        return response.json()

    async def get_tv_details(self, tv_id: int) -> Dict[str, Any]:
        """Get detailed TV show information."""
        params = self._params(
            append_to_response="credits,external_ids,keywords,content_ratings,videos,images"
        )
        response = await self._client.get(f"{self.base_url}/tv/{tv_id}", params=params)
        response.raise_for_status()
        return response.json()

    async def get_season_details(self, tv_id: int, season_number: int) -> Dict[str, Any]:
        """Get season details."""
        params = self._params(append_to_response="credits,videos,images")
        response = await self._client.get(
            f"{self.base_url}/tv/{tv_id}/season/{season_number}", params=params
        )
        response.raise_for_status()
        return response.json()

    async def get_episode_details(self, tv_id: int, season_number: int, episode_number: int) -> Dict[str, Any]:
        """Get episode details."""
        params = self._params(append_to_response="credits,videos,images")
        response = await self._client.get(
            f"{self.base_url}/tv/{tv_id}/season/{season_number}/episode/{episode_number}",
            params=params,
        )
        response.raise_for_status()
        return response.json()

    async def find_by_external_id(self, external_id: str, source: str = "imdb_id") -> Dict[str, Any]:
        """Find by external ID (IMDb, TVDb, etc.)."""
        params = self._params(external_source=source)
        response = await self._client.get(f"{self.base_url}/find/{external_id}", params=params)
        response.raise_for_status()
        return response.json()

    def get_image_url(self, path: str, size: str = "original") -> str:
        """Get full image URL."""
        if not path:
            return ""
        return f"{self.image_base_url}/{size}{path}"


class TMDbMatcher:
    """Match releases against TMDb and retrieve metadata."""

    def __init__(self, client: TMDbClient = None):
        self.client = client or TMDbClient()
        self.confidence_threshold = 0.8

    async def match_release(
        self,
        title: str,
        media_type: MediaType,
        year: int = None,
        season: int = None,
        episode: int = None,
    ) -> List[Dict[str, Any]]:
        """Match a release against TMDb."""
        async with self.client as client:
            if media_type == MediaType.MOVIE:
                results = await client.search_movie(title, year)
                return [self._score_movie_match(r, title, year) for r in results]
            elif media_type in (MediaType.TV, MediaType.ANIME):
                results = await client.search_tv(title, year)
                return [self._score_tv_match(r, title, year) for r in results]
            return []

    def _score_movie_match(self, result: Dict[str, Any], query_title: str, query_year: int = None) -> Dict[str, Any]:
        """Score a movie match."""
        score = 0.0
        reasons = []

        # Title similarity
        result_title = result.get("title", "").lower()
        query_lower = query_title.lower()

        if result_title == query_lower:
            score += 0.5
            reasons.append("exact_title")
        elif result_title in query_lower or query_lower in result_title:
            score += 0.3
            reasons.append("partial_title")
        else:
            # Fuzzy matching would go here
            score += 0.1
            reasons.append("fuzzy_title")

        # Year match
        release_date = result.get("release_date")
        if release_date and query_year:
            try:
                result_year = int(release_date[:4])
                if result_year == query_year:
                    score += 0.3
                    reasons.append("exact_year")
                elif abs(result_year - query_year) <= 1:
                    score += 0.1
                    reasons.append("close_year")
            except (ValueError, IndexError):
                pass

        # Popularity boost
        popularity = result.get("popularity", 0)
        if popularity > 100:
            score += 0.1
            reasons.append("high_popularity")

        result["match_score"] = min(score, 1.0)
        result["match_reasons"] = reasons
        return result

    def _score_tv_match(self, result: Dict[str, Any], query_title: str, query_year: int = None) -> Dict[str, Any]:
        """Score a TV show match."""
        score = 0.0
        reasons = []

        result_title = result.get("name", "").lower()
        query_lower = query_title.lower()

        if result_title == query_lower:
            score += 0.5
            reasons.append("exact_title")
        elif result_title in query_lower or query_lower in result_title:
            score += 0.3
            reasons.append("partial_title")
        else:
            score += 0.1
            reasons.append("fuzzy_title")

        first_air_date = result.get("first_air_date")
        if first_air_date and query_year:
            try:
                result_year = int(first_air_date[:4])
                if result_year == query_year:
                    score += 0.3
                    reasons.append("exact_year")
            except (ValueError, IndexError):
                pass

        popularity = result.get("popularity", 0)
        if popularity > 50:
            score += 0.1
            reasons.append("high_popularity")

        result["match_score"] = min(score, 1.0)
        result["match_reasons"] = reasons
        return result

    async def get_best_match(
        self,
        title: str,
        media_type: MediaType,
        year: int = None,
        season: int = None,
        episode: int = None,
    ) -> Optional[Dict[str, Any]]:
        """Get the best match above confidence threshold."""
        matches = await self.match_release(title, media_type, year, season, episode)

        if not matches:
            return None

        # Sort by score
        matches.sort(key=lambda x: x["match_score"], reverse=True)
        best = matches[0]

        if best["match_score"] >= self.confidence_threshold:
            return best

        return None

    async def retrieve_metadata(
        self,
        provider_id: str,
        provider_type: str,
    ) -> Dict[str, Any]:
        """Retrieve full metadata for a matched item."""
        async with self.client as client:
            tmdb_id = int(provider_id)

            if provider_type == "movie":
                return await client.get_movie_details(tmdb_id)
            elif provider_type == "tv":
                return await client.get_tv_details(tmdb_id)
            elif provider_type == "season":
                # provider_id format: "tv_id:season_number"
                tv_id, season_num = map(int, provider_id.split(":"))
                return await client.get_season_details(tv_id, season_num)
            elif provider_type == "episode":
                # provider_id format: "tv_id:season_number:episode_number"
                tv_id, season_num, ep_num = map(int, provider_id.split(":"))
                return await client.get_episode_details(tv_id, season_num, ep_num)

        return {}


async def create_metadata_snapshot(
    release_id: str,
    provider: str,
    provider_id: str,
    provider_type: str,
    data: Dict[str, Any],
    etag: str = None,
) -> MetadataSnapshot:
    """Create a metadata snapshot record."""
    import uuid

    return MetadataSnapshot(
        id=str(uuid.uuid4()),
        release_id=release_id,
        provider=provider,
        provider_id=provider_id,
        provider_type=provider_type,
        data=data,
        retrieved_at=datetime.utcnow().isoformat() + "Z",
        etag=etag,
    )


async def create_external_identifier(
    release_id: str,
    provider: str,
    provider_id: str,
    provider_type: str,
    confidence: float,
    match_method: str,
) -> ExternalIdentifier:
    """Create an external identifier record."""
    import uuid

    return ExternalIdentifier(
        id=str(uuid.uuid4()),
        release_id=release_id,
        provider=provider,
        provider_id=provider_id,
        provider_type=provider_type,
        confidence=confidence,
        matched_at=datetime.utcnow().isoformat() + "Z",
        match_method=match_method,
    )