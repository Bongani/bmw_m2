FROM python:3.12-slim
WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY pyproject.toml .
RUN uv sync --no-dev
COPY data_generator.py .
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV PYTHONUNBUFFERED=1
CMD ["uv", "run", "python", "data_generator.py"]
