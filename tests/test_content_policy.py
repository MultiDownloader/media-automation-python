import pytest
import pytest_asyncio
from datetime import datetime


@pytest.fixture
def sample_release_data():
    return {
        "id": "test-release-1",
        "source_id": "src-1",
        "source_url": "https://example.com/release/1",
        "source_id_external": "ext-1",
        "title": "Test Movie 2024",
        "original_title": "Test Movie",
        "year": 2024,
        "media_type": "movie",
        "normalized_title": "test movie 2024",
        "search_key": "test movie 2024",
        "status": "new",
        "discovered_at": datetime.utcnow().isoformat() + "Z",
        "created_at": datetime.utcnow().isoformat() + "Z",
        "updated_at": datetime.utcnow().isoformat() + "Z",
    }


def test_normalized_release_structure(sample_release_data):
    """Test that release data has all required fields."""
    required = ["id", "source_id", "title", "media_type", "normalized_title", "search_key", "status"]
    for field in required:
        assert field in sample_release_data
    assert sample_release_data["media_type"] in ["movie", "tv", "anime"]


def test_search_key_format(sample_release_data):
    """Test search key is lowercase alphanumeric."""
    key = sample_release_data["search_key"]
    assert key.islower()
    assert all(c.isalnum() or c == " " for c in key)


@pytest.mark.asyncio
async def test_content_policy_import():
    """Verify content policy module imports without error."""
    from shared.content_policy import (
        ContentFilter,
        PolicyDecision,
        normalize_host,
        classify_release,
        evaluate_policy,
    )
    assert normalize_host("https://example.com") == "example.com"
    assert normalize_host("http://www.EXAMPLE.COM/path") == "example.com"
    assert normalize_host("") is None
    assert normalize_host("://") is None
    assert normalize_host("http://valid.com") == "valid.com"


@pytest.mark.asyncio
async def test_filter_classification():
    """Test filter keyword matching."""
    from shared.content_policy import ContentFilter, classify_release

    filters = [
        ContentFilter(
            filter_key="adult",
            label="18+ (Adult)",
            category="rating",
            enabled=0,
            default_enabled=0,
            match_keywords=["18+", "xxx", "porn"],
            match_media_types=[],
        ),
        ContentFilter(
            filter_key="anime",
            label="Anime",
            category="genre",
            enabled=1,
            default_enabled=1,
            match_keywords=[],
            match_media_types=["anime"],
        ),
    ]

    # Should match adult by keyword
    matched = classify_release("My Movie 18+ Special", "movie", filters)
    assert "adult" in matched

    # Should match anime by media type
    matched = classify_release("Some Anime", "anime", filters)
    assert "anime" in matched

    # Should match nothing
    matched = classify_release("Regular Movie", "movie", filters)
    assert matched == []


@pytest.mark.asyncio
async def test_policy_evaluation():
    """Test full policy evaluation with allowlist and filters."""
    from shared.content_policy import (
        ContentFilter,
        AllowlistRow,
        evaluate_policy,
        normalize_host,
    )

    filters = [
        ContentFilter("adult", "18+", "rating", 0, 0, ["18+"], []),
        ContentFilter("anime", "Anime", "genre", 1, 1, [], ["anime"]),
    ]

    # Empty allowlist = permissive
    decision = evaluate_policy(
        release_host="example.com",
        title="Normal Movie",
        media_type="movie",
        filters=filters,
        allowlist_hosts=[],
        allowlist_configured=False,
    )
    assert decision.allowed is True

    # Non-empty allowlist blocks unknown hosts
    allowlist = [AllowlistRow("allowed.com", "https://allowed.com", 1)]
    decision = evaluate_policy(
        release_host="blocked.com",
        title="Normal Movie",
        media_type="movie",
        filters=filters,
        allowlist_hosts=["allowed.com"],
        allowlist_configured=True,
    )
    assert decision.allowed is False
    assert decision.reason == "site_not_allowed"

    # Disabled filter blocks even on allowed host
    decision = evaluate_policy(
        release_host="allowed.com",
        title="Movie 18+",
        media_type="movie",
        filters=filters,
        allowlist_hosts=["allowed.com"],
        allowlist_configured=True,
    )
    assert decision.allowed is False
    assert decision.reason == "filter_blocked"
    assert "adult" in decision.blocked_by