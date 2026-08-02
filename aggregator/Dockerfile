FROM python:3.12-slim
WORKDIR /app
COPY aggregator/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY aggregator/ ./aggregator/
COPY schemas/ ./schemas/
COPY scripts/__init__.py ./scripts/__init__.py
COPY scripts/research/__init__.py ./scripts/research/__init__.py
COPY scripts/research/lab_config.py ./scripts/research/lab_config.py
CMD ["uvicorn", "aggregator.main:app", "--host", "0.0.0.0", "--port", "8000"]
