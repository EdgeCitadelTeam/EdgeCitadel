FROM python@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de

RUN apt-get update \
    && apt-get install --no-install-recommends -y iproute2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY scripts/research/requirements.lock.txt /tmp/requirements.lock.txt
RUN python -m pip install --no-cache-dir --require-hashes \
        -r /tmp/requirements.lock.txt \
    && rm /tmp/requirements.lock.txt

COPY adapters /app/adapters
COPY aggregator /app/aggregator
COPY schemas /app/schemas
COPY scripts /app/scripts
