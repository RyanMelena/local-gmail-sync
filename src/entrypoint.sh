#!/bin/sh
set -eu

# ── Validate required env vars ────────────────────────────────────────────────
: "${GMAIL_ADDRESS:?GMAIL_ADDRESS is required}"
: "${GMAIL_APP_PASSWORD:?GMAIL_APP_PASSWORD is required}"
: "${QDRANT_COLLECTION:?QDRANT_COLLECTION is required}"
: "${QDRANT_URL:?QDRANT_URL is required}"
: "${OLLAMA_URL:?OLLAMA_URL is required}"

SYNC_INTERVAL="${SYNC_INTERVAL_SECONDS:-900}"
SYNC_PATTERN="${SYNC_PATTERN:-*}"
MBSYNCRC="/tmp/mbsyncrc-${QDRANT_COLLECTION}"
MAILDIR="/mail"
NOTMUCH_CFG="${MAILDIR}/.notmuch-config"

# ── Generate mbsync config at runtime from env vars ──────────────────────────
cat > "${MBSYNCRC}" << EOF
IMAPAccount gmail
Host imap.gmail.com
User ${GMAIL_ADDRESS}
Pass "${GMAIL_APP_PASSWORD}"
TLSType IMAPS
CertificateFile /etc/ssl/certs/ca-certificates.crt

IMAPStore gmail-remote
Account gmail

MaildirStore gmail-local
Path ${MAILDIR}/
Inbox ${MAILDIR}/INBOX
SubFolders Verbatim

Channel gmail-channel
Far :gmail-remote:
Near :gmail-local:
Patterns ${SYNC_PATTERN}
Sync Pull
Create Near
Expunge None
SyncState *
EOF

echo "[entrypoint] Config written to ${MBSYNCRC}"

# ── Generate notmuch config ───────────────────────────────────────────────────
cat > "${NOTMUCH_CFG}" << EOF
[database]
path=${MAILDIR}

[user]
name=${GMAIL_ADDRESS}
primary_email=${GMAIL_ADDRESS}

[new]
tags=unread;inbox;
ignore=.mbsync;.ingest_state.sqlite;

[search]
exclude_tags=deleted;spam;

[maildir]
synchronize_flags=true
EOF

echo "[entrypoint] notmuch config written to ${NOTMUCH_CFG}"

# ── Initialize notmuch database if needed ─────────────────────────────────────
if [ ! -d "${MAILDIR}/.notmuch" ]; then
    echo "[entrypoint] Initializing notmuch database..."
    notmuch --config "${NOTMUCH_CFG}" new --no-hooks
fi

# ── Pull the embedding model before the first sync ───────────────────────────
echo "[entrypoint] Ensuring embedding model '${EMBED_MODEL:-nomic-embed-text}' is available..."
curl -sf --retry 12 --retry-delay 10 \
     -X POST "${OLLAMA_URL}/api/pull" \
     -d "{\"name\":\"${EMBED_MODEL:-nomic-embed-text}\",\"stream\":false}" \
     -H "Content-Type: application/json" > /dev/null
echo "[entrypoint] Embedding model ready."

# ── Start notmuch HTTP server in background ───────────────────────────────────
echo "[entrypoint] Starting notmuch search server..."
python3 /app/notmuch_server.py &

# ── Main loop ─────────────────────────────────────────────────────────────────
while true; do
    echo "[$(date -u +%FT%TZ)] ── Syncing ${GMAIL_ADDRESS} ──"

    mbsync -c "${MBSYNCRC}" gmail-channel 2>&1 || true

    echo "[$(date -u +%FT%TZ)] ── Updating notmuch index ──"
    notmuch --config "${NOTMUCH_CFG}" new --no-hooks 2>&1 || true

    python3 /app/ingest.py

    echo "[$(date -u +%FT%TZ)] ── Cycle complete. Sleeping ${SYNC_INTERVAL}s ──"
    sleep "${SYNC_INTERVAL}"
done
