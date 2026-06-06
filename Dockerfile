# Image for running the all python file

FROM python:3.11-slim

# CHROME_BIN: nodriver's PATH-based auto-detect (find_chrome_executable) is
# fragile inside DockerOperator-spawned containers — the inherited PATH
# sometimes drops /usr/bin and the binary lookup fails. Setting CHROME_BIN
# lets the crawler pass an explicit `browser_executable_path` to uc.start()
# and skip auto-detect entirely.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEBIAN_FRONTEND=noninteractive \
    IS_DOCKER=1 \
    CHROME_BIN=/usr/bin/google-chrome

RUN apt-get update && apt-get install -y --no-install-recommends \
        wget gnupg ca-certificates \
        fonts-liberation \
        libasound2 libatk-bridge2.0-0 libatk1.0-0 libcups2 libdbus-1-3 \
        libdrm2 libgbm1 libnspr4 libnss3 libx11-xcb1 libxcomposite1 \
        libxdamage1 libxkbcommon0 libxrandr2 xdg-utils xvfb xauth \
    && wget -q -O /tmp/chrome.deb \
        https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get install -y --no-install-recommends /tmp/chrome.deb \
    && rm /tmp/chrome.deb \
    && apt-get purge -y gnupg \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && test -x "$CHROME_BIN" \
    && "$CHROME_BIN" --version

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip \
    && pip install -r /app/requirements.txt

COPY src /app/src

# PYTHONPATH=/app de `python -m src.*` resolve duoc ma khong can `cd /app`.
ENV PYTHONPATH=/app