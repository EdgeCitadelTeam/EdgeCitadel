FROM python:3.12-slim
WORKDIR /app/aggregator
COPY agent-platform/ /app/agent-platform/
COPY aggregator/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt -e /app/agent-platform
WORKDIR /app
COPY aggregator/ ./aggregator/
COPY schemas/ ./schemas/
CMD ["uvicorn", "aggregator.main:app", "--host", "0.0.0.0", "--port", "8000"]
