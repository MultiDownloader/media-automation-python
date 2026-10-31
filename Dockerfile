# Worker image for the queue/worker stack (see docker-compose.queue.yml).
#
# Build context is the REPO ROOT's python/ directory. No secrets are baked
# into the image: configuration arrives via environment at runtime.
FROM python:3.11-slim

# System deps kept minimal; ffmpeg is included because the media pipeline
# (yt-dlp downloads, validation) needs it at runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install core requirements first (better layer caching), then the optional
# queue/scraping extras that the worker actually uses.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Run as a non-root user.
RUN useradd --create-home --uid 10001 worker
USER worker

# Default: celery worker consuming the default + dead-letter queues.
# Override the command (e.g. to run discovery/processor loops) as needed.
ENV PYTHONUNBUFFERED=1
CMD ["celery", "-A", "queue_workers.worker", "worker", "-Q", "default,dead_letter", "--loglevel=info"]
