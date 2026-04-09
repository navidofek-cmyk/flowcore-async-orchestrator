FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY flowcore ./flowcore

RUN python -m pip install --upgrade pip && \
    python -m pip install .

CMD ["flowcore", "--help"]
