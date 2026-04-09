FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY flowcore ./flowcore
COPY demo_fastapi ./demo_fastapi

RUN pip install --no-cache-dir fastapi uvicorn && \
    pip install --no-cache-dir .

EXPOSE 9101

CMD ["uvicorn", "demo_fastapi.app:app", "--host", "0.0.0.0", "--port", "9101"]
