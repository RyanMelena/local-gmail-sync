FROM python:3.12-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        isync \
        curl \
        ca-certificates \
        sqlite3 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
        qdrant-client==1.9.1 \
        requests==2.31.0 \
        beautifulsoup4==4.12.3 \
        lxml==5.2.1 \
        tiktoken==0.7.0

WORKDIR /app
COPY src/token_refresh.py src/ingest.py src/entrypoint.sh ./
RUN chmod +x entrypoint.sh

CMD ["/app/entrypoint.sh"]