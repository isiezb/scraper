FROM python:3.12-slim

WORKDIR /app

# Cache-bust: v12 — changing this invalidates ALL layers below
ARG CACHEBUST=v38

# System deps for lxml + Playwright Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxml2-dev libxslt1-dev \
    libxfixes3 libx11-6 libx11-xcb1 libxcb1 libxext6 \
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 \
    libcups2 libdrm2 libxkbcommon0 libxcomposite1 \
    libxdamage1 libxrandr2 libgbm1 libpango-1.0-0 \
    libcairo2 libasound2 libxshmfence1 \
    libdbus-1-3 libatspi2.0-0 libglib2.0-0 \
    fonts-liberation libfontconfig1 && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright Chromium browser binary
RUN playwright install chromium

# Force Python to run unbuffered so Railway sees logs
ENV PYTHONUNBUFFERED=1

COPY . .

RUN echo "Build $CACHEBUST"

CMD ["python", "-u", "orchestrator.py", "--once"]
