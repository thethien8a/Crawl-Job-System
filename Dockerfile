# Image for running the all python file

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEBIAN_FRONTEND=noninteractive \
    IS_DOCKER=1 \
    CHROME_BIN=/usr/bin/google-chrome

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        wget gnupg ca-certificates \
        fonts-liberation \
        libasound2 libatk-bridge2.0-0 libatk1.0-0 libcups2 libdbus-1-3 \
        libdrm2 libgbm1 libnspr4 libnss3 libx11-xcb1 libxcomposite1 \
        libxdamage1 libxkbcommon0 libxrandr2 xdg-utils xvfb xauth \
        libxss1 libgtk-3-0 libpangocairo-1.0-0 libpango-1.0-0 \
        libcairo2 libvulkan1; \
    arch="$(dpkg --print-architecture)"; \
    if [ "$arch" = "amd64" ]; then \
        wget -q -O /tmp/chrome.deb \
            https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb; \
        apt-get install -y --no-install-recommends /tmp/chrome.deb; \
        rm -f /tmp/chrome.deb; \
    else \
        apt-get install -y --no-install-recommends chromium; \
        ln -sf /usr/bin/chromium "$CHROME_BIN"; \
    fi; \
    apt-get purge -y gnupg; \
    apt-get autoremove -y; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*; \
    test -x "$CHROME_BIN"; \
    "$CHROME_BIN" --version

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip \
    && pip install -r /app/requirements.txt

COPY src /app/src

ENV PYTHONPATH=/app