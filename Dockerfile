FROM python:3.12-alpine

RUN apk add --no-cache \
        --repository=https://dl-cdn.alpinelinux.org/alpine/edge/main \
        --repository=https://dl-cdn.alpinelinux.org/alpine/edge/community \
        isync \
        cyrus-sasl \
        cyrus-sasl-xoauth2 \
        curl \
        sqlite

RUN pip install --no-cache-dir \
        google-auth==2.29.0 \
        qdrant-client==1.9.1 \
        requests==2.31.0 \
        beautifulsoup4==4.12.3 \
        lxml==5.2.1

WORKDIR /app
COPY src/token_refresh.py src/ingest.py src/entrypoint.sh ./
RUN chmod +x entrypoint.sh

CMD ["/app/entrypoint.sh"]