"""
AI text generation for metadata enrichment.
Supports configurable providers (OpenAI, Anthropic, Ollama, custom).
"""

from __future__ import annotations

import abc
import json
import structlog
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx

from ..shared.config import get_settings
from ..shared.types import GeneratedContent, MediaType

logger = structlog.get_logger(__name__)


@dataclass
class GenerationPrompt:
    """Prompt configuration for text generation."""
    system_prompt: str
    user_prompt_template: str
    max_tokens: int = 2000
    temperature: float = 0.7
    response_format: str = "json"  # json or text


@dataclass
class GeneratedText:
    """Generated text content."""
    display_title: str
    seo_title: str
    overview: str
    seo_description: str
    keywords: List[str]
    tags: List[str]
    translations: Dict[str, Dict[str, str]]
    image_prompt: Optional[str] = None


class BaseTextGenerator(abc.ABC):
    """Base class for text generation providers."""

    def __init__(self, model: str, **kwargs):
        self.model = model
        self.config = kwargs
        self.settings = get_settings()

    @abc.abstractmethod
    async def generate(self, prompt: GenerationPrompt, **variables) -> GeneratedText:
        """Generate text content."""
        pass

    def build_prompt(self, template: str, **variables) -> str:
        """Build prompt from template with variables."""
        return template.format(**variables)

    def parse_json_response(self, text: str) -> Dict[str, Any]:
        """Parse JSON from model response."""
        # Try to extract JSON from response
        text = text.strip()

        # Find JSON object
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            json_str = text[start:end+1]
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                pass

        # Try parsing as-is
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return {"raw": text}


class OpenAITextGenerator(BaseTextGenerator):
    """OpenAI GPT text generator."""

    def __init__(self, model: str = "gpt-4o-mini", api_key: str = None, base_url: str = None, **kwargs):
        super().__init__(model, **kwargs)
        self.api_key = api_key or self.settings.ai_text_api_key
        self.base_url = base_url or self.settings.ai_text_base_url
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "OpenAITextGenerator":
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(60.0, connect=10.0),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            base_url=self.base_url,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._client:
            await self._client.aclose()

    async def generate(self, prompt: GenerationPrompt, **variables) -> GeneratedText:
        user_prompt = self.build_prompt(prompt.user_prompt_template, **variables)

        messages = [
            {"role": "system", "content": prompt.system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        payload = {
            "model": self.model,
            "messages": messages,
            "max_tokens": prompt.max_tokens,
            "temperature": prompt.temperature,
        }

        if prompt.response_format == "json":
            payload["response_format"] = {"type": "json_object"}

        response = await self._client.post("/chat/completions", json=payload)
        response.raise_for_status()

        data = response.json()
        content = data["choices"][0]["message"]["content"]

        if prompt.response_format == "json":
            parsed = self.parse_json_response(content)
        else:
            parsed = {"raw": content}

        return self._parse_generated_text(parsed)

    def _parse_generated_text(self, data: Dict[str, Any]) -> GeneratedText:
        """Parse generated text into structured format."""
        return GeneratedText(
            display_title=data.get("display_title", ""),
            seo_title=data.get("seo_title", ""),
            overview=data.get("overview", ""),
            seo_description=data.get("seo_description", ""),
            keywords=data.get("keywords", []),
            tags=data.get("tags", []),
            translations=data.get("translations", {}),
            image_prompt=data.get("image_prompt"),
        )


class AnthropicTextGenerator(BaseTextGenerator):
    """Anthropic Claude text generator."""

    def __init__(self, model: str = "claude-3-haiku-20240307", api_key: str = None, **kwargs):
        super().__init__(model, **kwargs)
        self.api_key = api_key or self.settings.ai_text_api_key
        self.base_url = "https://api.anthropic.com/v1"
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "AnthropicTextGenerator":
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(60.0, connect=10.0),
            headers={
                "x-api-key": self.api_key,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            },
            base_url=self.base_url,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._client:
            await self._client.aclose()

    async def generate(self, prompt: GenerationPrompt, **variables) -> GeneratedText:
        user_prompt = self.build_prompt(prompt.user_prompt_template, **variables)

        payload = {
            "model": self.model,
            "max_tokens": prompt.max_tokens,
            "temperature": prompt.temperature,
            "system": prompt.system_prompt,
            "messages": [{"role": "user", "content": user_prompt}],
        }

        response = await self._client.post("/messages", json=payload)
        response.raise_for_status()

        data = response.json()
        content = data["content"][0]["text"]

        parsed = self.parse_json_response(content)
        return self._parse_generated_text(parsed)

    def _parse_generated_text(self, data: Dict[str, Any]) -> GeneratedText:
        return GeneratedText(
            display_title=data.get("display_title", ""),
            seo_title=data.get("seo_title", ""),
            overview=data.get("overview", ""),
            seo_description=data.get("seo_description", ""),
            keywords=data.get("keywords", []),
            tags=data.get("tags", []),
            translations=data.get("translations", {}),
            image_prompt=data.get("image_prompt"),
        )


class OllamaTextGenerator(BaseTextGenerator):
    """Ollama local LLM text generator."""

    def __init__(self, model: str = "llama3", base_url: str = "http://localhost:11434", **kwargs):
        super().__init__(model, **kwargs)
        self.base_url = base_url
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "OllamaTextGenerator":
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(120.0, connect=10.0),
            base_url=self.base_url,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._client:
            await self._client.aclose()

    async def generate(self, prompt: GenerationPrompt, **variables) -> GeneratedText:
        user_prompt = self.build_prompt(prompt.user_prompt_template, **variables)

        payload = {
            "model": self.model,
            "prompt": f"{prompt.system_prompt}\n\n{user_prompt}",
            "stream": False,
            "options": {
                "temperature": prompt.temperature,
                "num_predict": prompt.max_tokens,
            },
        }

        if prompt.response_format == "json":
            payload["format"] = "json"

        response = await self._client.post("/api/generate", json=payload)
        response.raise_for_status()

        data = response.json()
        content = data.get("response", "")

        parsed = self.parse_json_response(content)
        return self._parse_generated_text(parsed)

    def _parse_generated_text(self, data: Dict[str, Any]) -> GeneratedText:
        return GeneratedText(
            display_title=data.get("display_title", ""),
            seo_title=data.get("seo_title", ""),
            overview=data.get("overview", ""),
            seo_description=data.get("seo_description", ""),
            keywords=data.get("keywords", []),
            tags=data.get("tags", []),
            translations=data.get("translations", {}),
            image_prompt=data.get("image_prompt"),
        )


class CustomTextGenerator(BaseTextGenerator):
    """Custom HTTP API text generator."""

    def __init__(
        self,
        model: str,
        api_url: str,
        api_key: str = None,
        headers: Dict[str, str] = None,
        **kwargs
    ):
        super().__init__(model, **kwargs)
        self.api_url = api_url
        self.api_key = api_key
        self.custom_headers = headers or {}
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "CustomTextGenerator":
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        headers.update(self.custom_headers)

        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(60.0, connect=10.0),
            headers=headers,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._client:
            await self._client.aclose()

    async def generate(self, prompt: GenerationPrompt, **variables) -> GeneratedText:
        user_prompt = self.build_prompt(prompt.user_prompt_template, **variables)

        payload = {
            "model": self.model,
            "prompt": user_prompt,
            "system_prompt": prompt.system_prompt,
            "max_tokens": prompt.max_tokens,
            "temperature": prompt.temperature,
        }

        response = await self._client.post(self.api_url, json=payload)
        response.raise_for_status()

        data = response.json()
        content = data.get("content") or data.get("text") or data.get("response", "")

        parsed = self.parse_json_response(content)
        return self._parse_generated_text(parsed)

    def _parse_generated_text(self, data: Dict[str, Any]) -> GeneratedText:
        return GeneratedText(
            display_title=data.get("display_title", ""),
            seo_title=data.get("seo_title", ""),
            overview=data.get("overview", ""),
            seo_description=data.get("seo_description", ""),
            keywords=data.get("keywords", []),
            tags=data.get("tags", []),
            translations=data.get("translations", {}),
            image_prompt=data.get("image_prompt"),
        )


def create_text_generator(provider: str = None, **kwargs) -> BaseTextGenerator:
    """Factory function to create text generator."""
    settings = get_settings()
    provider = provider or settings.ai_text_provider

    if provider == "openai":
        return OpenAITextGenerator(
            model=kwargs.get("model", settings.ai_text_model),
            api_key=kwargs.get("api_key", settings.ai_text_api_key),
            base_url=kwargs.get("base_url", settings.ai_text_base_url),
        )
    elif provider == "anthropic":
        return AnthropicTextGenerator(
            model=kwargs.get("model", settings.ai_text_model),
            api_key=kwargs.get("api_key", settings.ai_text_api_key),
        )
    elif provider == "ollama":
        return OllamaTextGenerator(
            model=kwargs.get("model", settings.ai_text_model),
            base_url=kwargs.get("base_url", "http://localhost:11434"),
        )
    elif provider == "custom":
        return CustomTextGenerator(
            model=kwargs.get("model", settings.ai_text_model),
            api_url=kwargs.get("api_url", settings.ai_text_base_url),
            api_key=kwargs.get("api_key", settings.ai_text_api_key),
            headers=kwargs.get("headers"),
        )
    else:
        raise ValueError(f"Unknown text generation provider: {provider}")


# Default prompts
DEFAULT_SYSTEM_PROMPT = """You are an expert media content writer. Generate engaging, SEO-optimized content for media releases.
Always respond with valid JSON matching the requested schema.
Do not include any explanations, only the JSON object."""

DEFAULT_USER_PROMPT_TEMPLATE = """Generate content for the following media release:

Title: {title}
Original Title: {original_title}
Year: {year}
Media Type: {media_type}
Season: {season}
Episode: {episode}
TMDb Overview: {overview}
Genres: {genres}
Cast: {cast}
Release Date: {release_date}

Generate the following JSON:
{
  "display_title": "Human-readable display title",
  "seo_title": "SEO-optimized title (max 60 chars)",
  "overview": "Engaging 2-3 paragraph overview (200-300 words)",
  "seo_description": "Meta description for SEO (max 160 chars)",
  "keywords": ["keyword1", "keyword2", "keyword3"],
  "tags": ["tag1", "tag2", "tag3"],
  "translations": {
    "es": {"display_title": "", "overview": "", "seo_description": ""},
    "fr": {"display_title": "", "overview": "", "seo_description": ""}
  },
  "image_prompt": "Detailed prompt for AI image generation"
}

Guidelines:
- Display title should be clear and recognizable
- SEO title should include key search terms
- Overview should be original, not copied from TMDb
- Keywords should be relevant for discovery
- Image prompt should be detailed for high-quality artwork
- Translations should be natural, not literal
"""