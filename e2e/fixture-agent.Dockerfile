FROM python@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de

WORKDIR /app

COPY agent-platform /app/agent-platform
RUN python -m pip install --no-cache-dir -e /app/agent-platform

COPY schemas /app/schemas
COPY e2e/fixture_agent /app/e2e/fixture_agent
