FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /app
RUN useradd --create-home --uid 10001 appuser
COPY requirements.txt pyproject.toml ./
COPY src ./src
COPY feature_repo ./feature_repo
COPY streamlit_app.py ./streamlit_app.py
RUN mkdir -p /app/artifacts /app/data/events /mlflow && pip install --upgrade pip && pip install . && chown -R appuser:appuser /app /mlflow
USER appuser
EXPOSE 8000
CMD ["uvicorn", "recsys.api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
