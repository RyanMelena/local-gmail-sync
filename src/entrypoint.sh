#!/usr/bin/env bash
set -euo pipefail

# ── Validate required env vars ────────────────────────────────────────────────
: "${GMAIL_ADDRESS:?GMAIL_ADDRESS is required}"
: "${GMAIL_CLIENT_ID:?GMAIL_CLIENT_ID is required}"
: "${GMAIL_CLIENT_SECRET:?GMAIL_CLIENT_SECRET is required}"
: "${GMAIL_REFRESH_TOKEN:?GMAIL_REFRESH_TOKEN is required}"
: "${QDRANT_COLLECTION:?QDRANT_COLLECTION is required}"
: "${QDRANT_URL:?QDRANT_URL is required}"
: "${OLLAMA_URL:?OLLAMA_URL is required}"

SYNC_INTERVAL="${SYNC_INTERVAL_SECONDS:-900}"
MBSYNCRC="/tmp/mbsyncrc-${QDRANT_COLLECTION}"
MAILDIR="/mail"

# ── Generate mbsync config at runtime from env vars ──────────────────────────
cat > "${MBSYNCRC}" << EOF
IMAPAccount gmail
Host imap.gmail.com
User ${GMAIL_ADDRESS}
AuthMechs XOAUTH2
PassCmd "python3 /app/token_refresh.py"
SSLType IMAPS
SSLVersions TLSv1.3 TLSv1.2
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
Patterns INBOX "[Google Mail]/All Mail"
Expunge None
Sync Pull
MaxMessages ${INITIAL_BATCH_SIZE:-0}
EOF

echo "[entrypoint] Config written to ${MBSYNCRC}"

# Capture the output into a variable
VERSION_INFO=$(curl --version)

# Print the captured output
echo "Curl version: ${VERSION_INFO}"

# ── Pull the embedding model before the first sync ───────────────────────────
echo "[entrypoint] Ensuring embedding model '${EMBED_MODEL}' is available..."
curl -sf --retry 12 --retry-delay 10 \
     -X POST "${OLLAMA_URL}/api/pull" \
     -d "{\"name\":\"${EMBED_MODEL}\",\"stream\":false}" \
     -H "Content-Type: application/json" > /dev/null
echo "[entrypoint] Embedding model ready."

# ── Main loop ─────────────────────────────────────────────────────────────────
while true; do
    echo "[$(date -u +%FT%TZ)] ── Syncing ${GMAIL_ADDRESS} ──"

    # Sync email: pull-only, never expunge remote
    mbsync -c "${MBSYNCRC}" gmail-channel 2>&1 || true

    # Ingest new mail into Qdrant
    python3 /app/ingest.py

    echo "[$(date -u +%FT%TZ)] ── Cycle complete. Sleeping ${SYNC_INTERVAL}s ──"
    sleep "${SYNC_INTERVAL}"
done