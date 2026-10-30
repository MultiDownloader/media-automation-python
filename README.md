# media-automation-python

Python workers for the Media Automation Platform.

This repository contains the standalone Python side of the platform: source
discovery, metadata extraction, AI-assisted tagging, media download, validation,
upload, scheduling and analytics event collection.

> Public mirror of the `python/` directory from the private
> `media-automation-platform` repository.

## Modules

| Module | Purpose |
|--------|---------|
| `discovery/` | Source polling + release discovery with content-policy gating |
| `sources/` | Source registry (allowlist enforcement per source) |
| `metadata/` | Metadata extraction/normalization |
| `ai_generation/` | AI-assisted title/description/tag generation |
| `downloader/` | Download workers |
| `media_validation/` | File validation (codec/container/duration checks) |
| `uploaders/` | Destination upload adapters |
| `processors/` | Post-processing steps |
| `analytics/` | Analytics event collector (traceparent-aware, no PII) |
| `scheduler/` | Job scheduling loop |
| `shared/` | Shared helpers: config, logging, DB access, content policy |
| `processor.py` | Main job processor entry point |
| `tests/` | Pytest suite |

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

## Run tests

```bash
python -m pytest tests/ -v
```

## Content policy

`shared/content_policy.py` mirrors the TypeScript policy engine. A release is
processed only if:

1. its host is on the site allowlist (when the allowlist is non-empty), and
2. none of its matched content filters are disabled by the user.

User filter decisions are authoritative — 18+/adult ships disabled (blocked)
by default and can only be changed by the user.

## Compliance notes

- Sources are restricted to an explicit allowlist with approval gates.
- `robots.txt`, rate limits and terms of the target site are respected.
- No DRM circumvention, no paywall bypass, no CAPTCHA/anti-bot bypass.
- No secrets are ever committed; credentials come from the environment.
