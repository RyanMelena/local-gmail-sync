FROM python:3.12-alpine

RUN apk add --no-cache \
        isync \
        notmuch \
        curl

RUN pip install --no-cache-dir \
        qdrant-client==1.9.1 \
        requests==2.31.0 \
        beautifulsoup4==4.12.3 \
        lxml==5.2.1

WORKDIR /app
COPY src/ingest.py src/entrypoint.sh src/notmuchserver.py ./
RUN chmod +x entrypoint.sh

CMD ["/app/entrypoint.sh"]
