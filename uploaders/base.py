"""
Base uploader classes and factory.
"""

from __future__ import annotations

import abc
import structlog
from dataclasses import dataclass
from typing import Any, Dict, Optional

logger = structlog.get_logger(__name__)


@dataclass
class UploadResult:
    """Result of an upload operation."""
    remote_id: Optional[str] = None
    remote_url: Optional[str] = None
    metadata: Dict[str, Any] = None


class BaseUploader(abc.ABC):
    """Base class for destination uploaders."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    @abc.abstractmethod
    async def upload(self, file_path: str, destination_config: Dict[str, Any]) -> UploadResult:
        """Upload file to destination."""
        pass

    async def __aenter__(self) -> "BaseUploader":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        pass


class LocalUploader(BaseUploader):
    """Upload to local filesystem."""

    async def upload(self, file_path: str, destination_config: Dict[str, Any]) -> UploadResult:
        import shutil
        from pathlib import Path

        dest_root = destination_config.get("path", "./media/uploads")
        dest_path = Path(dest_root)

        # Create destination directory
        dest_path.mkdir(parents=True, exist_ok=True)

        # Copy file
        src = Path(file_path)
        dst = dest_path / src.name

        # Handle duplicates
        counter = 1
        while dst.exists():
            stem = src.stem
            suffix = src.suffix
            dst = dest_path / f"{stem}_{counter}{suffix}"
            counter += 1

        shutil.copy2(src, dst)

        logger.info("local_upload_complete", source=file_path, destination=str(dst))

        return UploadResult(
            remote_id=str(dst),
            remote_url=f"file://{dst.absolute()}",
            metadata={"path": str(dst), "size": dst.stat().st_size},
        )


class YouTubeUploader(BaseUploader):
    """Upload to YouTube via YouTube Data API v3."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.client_id = config.get("client_id")
        self.client_secret = config.get("client_secret")
        self.refresh_token = config.get("refresh_token")
        self.access_token = config.get("access_token")
        self._token_expiry = 0

    async def _get_access_token(self) -> str:
        """Get valid access token, refreshing if needed."""
        import time
        import httpx

        if self.access_token and time.time() < self._token_expiry - 60:
            return self.access_token

        if not self.refresh_token:
            raise ValueError("No refresh token available")

        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "refresh_token": self.refresh_token,
                    "grant_type": "refresh_token",
                },
            )
            response.raise_for_status()
            data = response.json()

            self.access_token = data["access_token"]
            self._token_expiry = time.time() + data.get("expires_in", 3600)

            return self.access_token

    async def upload(self, file_path: str, destination_config: Dict[str, Any]) -> UploadResult:
        import httpx
        import mimetypes

        access_token = await self._get_access_token()

        # Get video metadata from destination config
        title = destination_config.get("title", "Untitled")
        description = destination_config.get("description", "")
        tags = destination_config.get("tags", [])
        privacy = destination_config.get("privacy", "private")

        # Start resumable upload
        async with httpx.AsyncClient(timeout=3600.0) as client:
            # 1. Initiate upload session
            init_response = await client.post(
                "https://www.googleapis.com/upload/youtube/v3/videos?part=snippet,status&uploadType=resumable",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                    "X-Upload-Content-Type": mimetypes.guess_type(file_path)[0] or "video/*",
                    "X-Upload-Content-Length": str(Path(file_path).stat().st_size),
                },
                json={
                    "snippet": {
                        "title": title,
                        "description": description,
                        "tags": tags,
                        "categoryId": destination_config.get("category_id", "22"),
                    },
                    "status": {
                        "privacyStatus": privacy,
                        "selfDeclaredMadeForKids": destination_config.get("made_for_kids", False),
                    },
                },
            )
            init_response.raise_for_status()

            upload_url = init_response.headers.get("Location")
            if not upload_url:
                raise RuntimeError("No upload URL returned")

            # 2. Upload video file
            file_size = Path(file_path).stat().st_size
            chunk_size = 256 * 1024  # 256KB chunks

            with open(file_path, "rb") as f:
                uploaded = 0
                while uploaded < file_size:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break

                    end = min(uploaded + len(chunk), file_size) - 1
                    headers = {
                        "Content-Range": f"bytes {uploaded}-{end}/{file_size}",
                        "Content-Length": str(len(chunk)),
                    }

                    response = await client.put(upload_url, content=chunk, headers=headers, timeout=300.0)

                    if response.status_code in (200, 201):
                        # Upload complete
                        data = response.json()
                        video_id = data.get("id")
                        return UploadResult(
                            remote_id=video_id,
                            remote_url=f"https://www.youtube.com/watch?v={video_id}",
                            metadata=data,
                        )
                    elif response.status_code == 308:
                        # Resume incomplete
                        range_header = response.headers.get("Range", "")
                        if range_header:
                            uploaded = int(range_header.split("-")[-1]) + 1
                    else:
                        response.raise_for_status()

            # If we get here, something went wrong
            raise RuntimeError("Upload did not complete")


class S3Uploader(BaseUploader):
    """Upload to S3-compatible storage (AWS S3, Cloudflare R2, MinIO, etc.)."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.endpoint_url = config.get("endpoint_url")
        self.access_key_id = config.get("access_key_id")
        self.secret_access_key = config.get("secret_access_key")
        self.bucket = config.get("bucket")
        self.region = config.get("region", "us-east-1")
        self._client = None

    async def __aenter__(self) -> "S3Uploader":
        import boto3
        from botocore.config import Config

        self._client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name=self.region,
            config=Config(s3={"addressing_style": "virtual"}),
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self._client = None

    async def upload(self, file_path: str, destination_config: Dict[str, Any]) -> UploadResult:
        import asyncio
        from pathlib import Path

        if not self._client:
            await self.__aenter__()

        key = destination_config.get("key", Path(file_path).name)
        extra_args = destination_config.get("extra_args", {})

        # Upload in thread pool
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: self._client.upload_file(file_path, self.bucket, key, ExtraArgs=extra_args),
        )

        # Generate URL
        url = f"https://{self.bucket}.{self.endpoint_url.replace('https://', '')}/{key}"

        logger.info("s3_upload_complete", bucket=self.bucket, key=key)

        return UploadResult(
            remote_id=key,
            remote_url=url,
            metadata={"bucket": self.bucket, "key": key, "size": Path(file_path).stat().st_size},
        )


class VimeoUploader(BaseUploader):
    """Upload to Vimeo."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.access_token = config.get("access_token")
        self._client = None

    async def __aenter__(self) -> "VimeoUploader":
        import httpx

        self._client = httpx.AsyncClient(
            timeout=3600.0,
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
                "Accept": "application/vnd.vimeo.*+json;version=3.4",
            },
            base_url="https://api.vimeo.com",
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._client:
            await self._client.aclose()

    async def upload(self, file_path: str, destination_config: Dict[str, Any]) -> UploadResult:
        import mimetypes
        from pathlib import Path

        if not self._client:
            await self.__aenter__()

        # 1. Create upload ticket
        create_response = await self._client.post(
            "/me/videos",
            json={
                "upload": {
                    "approach": "tus",
                    "size": Path(file_path).stat().st_size,
                },
                "privacy": {
                    "view": destination_config.get("privacy", "unlisted"),
                },
            },
        )
        create_response.raise_for_status()

        video_data = create_response.json()
        video_uri = video_data["uri"]
        upload_link = video_data["upload"]["upload_link"]

        # 2. Upload using TUS protocol (simplified - in production use tus-py)
        # For now, we'll use the direct upload approach
        file_size = Path(file_path).stat().st_size

        with open(file_path, "rb") as f:
            upload_response = await self._client.patch(
                upload_link,
                content=f.read(),
                headers={
                    "Upload-Offset": "0",
                    "Upload-Length": str(file_size),
                    "Content-Type": "application/offset+octet-stream",
                },
                timeout=3600.0,
            )

        # 3. Complete upload
        await self._client.delete(f"{upload_link}?action=complete")

        video_id = video_uri.split("/")[-1]

        return UploadResult(
            remote_id=video_id,
            remote_url=f"https://vimeo.com/{video_id}",
            metadata=video_data,
        )


def create_uploader(destination_type: str, config: Dict[str, Any]) -> Optional[BaseUploader]:
    """Factory function to create uploader."""
    uploaders = {
        "local": LocalUploader,
        "youtube": YouTubeUploader,
        "s3": S3Uploader,
        "r2": S3Uploader,  # R2 is S3-compatible
        "vimeo": VimeoUploader,
    }

    uploader_class = uploaders.get(destination_type)
    if uploader_class:
        return uploader_class(config)

    return None