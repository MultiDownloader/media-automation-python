"""
AI image generation for artwork.
Supports configurable providers (OpenAI DALL-E, Stability AI, Replicate, custom).
"""

from __future__ import annotations

import abc
import base64
import structlog
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import httpx

from ..shared.config import get_settings
from ..shared.types import GeneratedArtwork

logger = structlog.get_logger(__name__)


@dataclass
class ImageGenerationRequest:
    """Request for image generation."""
    prompt: str
    negative_prompt: str = ""
    width: int = 1024
    height: int = 1024
    model: str = ""
    quality: str = "standard"
    style: str = "vivid"
    n: int = 1


@dataclass
class ImageGenerationResult:
    """Result of image generation."""
    image_url: str
    revised_prompt: Optional[str] = None
    width: int = 0
    height: int = 0
    format: str = "png"
    file_size: int = 0
    generation_params: Dict[str, Any] = None


class BaseImageGenerator(abc.ABC):
    """Base class for image generation providers."""

    def __init__(self, model: str, **kwargs):
        self.model = model
        self.config = kwargs
        self.settings = get_settings()

    @abc.abstractmethod
    async def generate(self, request: ImageGenerationRequest) -> ImageGenerationResult:
        """Generate image."""
        pass

    async def download_image(self, url: str, dest_path: Path) -> int:
        """Download image from URL to local path."""
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            dest_path.write_bytes(response.content)
            return len(response.content)


class OpenAIImageGenerator(BaseImageGenerator):
    """OpenAI DALL-E image generator."""

    def __init__(
        self,
        model: str = "dall-e-3",
        api_key: str = None,
        base_url: str = None,
        **kwargs
    ):
        super().__init__(model, **kwargs)
        self.api_key = api_key or get_settings().ai_image_api_key
        self.base_url = base_url or get_settings().ai_image_base_url
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "OpenAIImageGenerator":
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(120.0, connect=10.0),
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

    async def generate(self, request: ImageGenerationRequest) -> ImageGenerationResult:
        payload = {
            "model": request.model or self.model,
            "prompt": request.prompt,
            "n": request.n,
            "size": f"{request.width}x{request.height}",
            "quality": request.quality,
            "style": request.style,
            "response_format": "url",
        }

        response = await self._client.post("/images/generations", json=payload)
        response.raise_for_status()

        data = response.json()
        image_data = data["data"][0]

        return ImageGenerationResult(
            image_url=image_data["url"],
            revised_prompt=image_data.get("revised_prompt"),
            width=request.width,
            height=request.height,
            format="png",
            file_size=0,  # Will be updated after download
            generation_params={
                "model": self.model,
                "prompt": request.prompt,
                "size": f"{request.width}x{request.height}",
                "quality": request.quality,
            },
        )


class StabilityImageGenerator(BaseImageGenerator):
    """Stability AI (Stable Diffusion) image generator."""

    def __init__(
        self,
        model: str = "stable-diffusion-xl-1024-v1-0",
        api_key: str = None,
        base_url: str = "https://api.stability.ai",
        **kwargs
    ):
        super().__init__(model, **kwargs)
        self.api_key = api_key or get_settings().ai_image_api_key
        self.base_url = base_url
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "StabilityImageGenerator":
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(120.0, connect=10.0),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Accept": "application/json",
            },
            base_url=self.base_url,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._client:
            await self._client.aclose()

    async def generate(self, request: ImageGenerationRequest) -> ImageGenerationResult:
        payload = {
            "prompt": request.prompt,
            "negative_prompt": request.negative_prompt,
            "width": request.width,
            "height": request.height,
            "samples": request.n,
            "steps": 30,
            "cfg_scale": 7,
            "style_preset": "cinematic",
        }

        response = await self._client.post(
            f"/v1/generation/{request.model or self.model}/text-to-image",
            json=payload,
        )
        response.raise_for_status()

        data = response.json()
        artifact = data["artifacts"][0]

        # Stability returns base64 encoded image
        image_bytes = base64.b64decode(artifact["base64"])

        # Save to temp file and return file:// URL or upload
        # For now, return a placeholder
        return ImageGenerationResult(
            image_url=f"data:image/png;base64,{artifact['base64'][:100]}...",
            revised_prompt=None,
            width=artifact.get("width", request.width),
            height=artifact.get("height", request.height),
            format="png",
            file_size=len(image_bytes),
            generation_params={
                "model": self.model,
                "prompt": request.prompt,
                "seed": artifact.get("seed"),
            },
        )


class ReplicateImageGenerator(BaseImageGenerator):
    """Replicate image generator."""

    def __init__(
        self,
        model: str = "stability-ai/sdxl:39ed52f2a78e934b3ba6e2a89f5b1c712de7dfea535525255b1aa35c5565e08b",
        api_key: str = None,
        base_url: str = "https://api.replicate.com/v1",
        **kwargs
    ):
        super().__init__(model, **kwargs)
        self.api_key = api_key or get_settings().ai_image_api_key
        self.base_url = base_url
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "ReplicateImageGenerator":
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(300.0, connect=10.0),
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

    async def generate(self, request: ImageGenerationRequest) -> ImageGenerationResult:
        # Start prediction
        payload = {
            "version": request.model or self.model,
            "input": {
                "prompt": request.prompt,
                "negative_prompt": request.negative_prompt,
                "width": request.width,
                "height": request.height,
                "num_outputs": request.n,
            },
        }

        response = await self._client.post("/predictions", json=payload)
        response.raise_for_status()

        prediction = response.json()
        prediction_id = prediction["id"]

        # Poll for completion
        import asyncio
        for _ in range(60):  # Max 5 minutes
            await asyncio.sleep(5)
            response = await self._client.get(f"/predictions/{prediction_id}")
            response.raise_for_status()
            prediction = response.json()

            if prediction["status"] == "succeeded":
                output = prediction["output"]
                image_url = output[0] if isinstance(output, list) else output

                return ImageGenerationResult(
                    image_url=image_url,
                    revised_prompt=None,
                    width=request.width,
                    height=request.height,
                    format="png",
                    file_size=0,
                    generation_params={
                        "model": self.model,
                        "prompt": request.prompt,
                        "prediction_id": prediction_id,
                    },
                )
            elif prediction["status"] == "failed":
                raise RuntimeError(f"Generation failed: {prediction.get('error')}")

        raise TimeoutError("Image generation timed out")


class CustomImageGenerator(BaseImageGenerator):
    """Custom HTTP API image generator."""

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

    async def __aenter__(self) -> "CustomImageGenerator":
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        headers.update(self.custom_headers)

        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(120.0, connect=10.0),
            headers=headers,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._client:
            await self._client.aclose()

    async def generate(self, request: ImageGenerationRequest) -> ImageGenerationResult:
        payload = {
            "model": request.model or self.model,
            "prompt": request.prompt,
            "negative_prompt": request.negative_prompt,
            "width": request.width,
            "height": request.height,
            "n": request.n,
        }

        response = await self._client.post(self.api_url, json=payload)
        response.raise_for_status()

        data = response.json()
        image_url = data.get("image_url") or data.get("url") or data.get("output", [None])[0]

        return ImageGenerationResult(
            image_url=image_url,
            revised_prompt=data.get("revised_prompt"),
            width=request.width,
            height=request.height,
            format=data.get("format", "png"),
            file_size=data.get("file_size", 0),
            generation_params={"model": self.model, "prompt": request.prompt},
        )


def create_image_generator(provider: str = None, **kwargs) -> BaseImageGenerator:
    """Factory function to create image generator."""
    settings = get_settings()
    provider = provider or settings.ai_image_provider

    if provider == "disabled":
        raise ValueError("Image generation is disabled")

    if provider == "openai":
        return OpenAIImageGenerator(
            model=kwargs.get("model", settings.ai_image_model),
            api_key=kwargs.get("api_key", settings.ai_image_api_key),
            base_url=kwargs.get("base_url", settings.ai_image_base_url),
        )
    elif provider == "stability":
        return StabilityImageGenerator(
            model=kwargs.get("model", "stable-diffusion-xl-1024-v1-0"),
            api_key=kwargs.get("api_key", settings.ai_image_api_key),
        )
    elif provider == "replicate":
        return ReplicateImageGenerator(
            model=kwargs.get("model", "stability-ai/sdxl"),
            api_key=kwargs.get("api_key", settings.ai_image_api_key),
        )
    elif provider == "custom":
        return CustomImageGenerator(
            model=kwargs.get("model", settings.ai_image_model),
            api_url=kwargs.get("api_url", settings.ai_image_base_url),
            api_key=kwargs.get("api_key", settings.ai_image_api_key),
            headers=kwargs.get("headers"),
        )
    else:
        raise ValueError(f"Unknown image generation provider: {provider}")