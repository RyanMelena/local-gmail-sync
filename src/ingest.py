#!/usr/bin/env python3
"""
Walks the Maildir at /mail, embeds new messages, and upserts to Qdrant.
State is tracked in /mail/.ingest_state.sqlite so restarts are safe/idempotent.
"""
import os, email, mailbox, hashlib, json, sqlite3, re, requests, time
from pathlib import Path
from email.header import decode_header, make_header
from bs4 import BeautifulSoup
from qdrant_client import QdrantClient, models

# ── Config from env ───────────────────────────────────────────────────────────
MAILDIR        = Path("/mail")
QDRANT_URL     = os.environ["QDRANT_URL"]
COLLECTION     = os.environ["QDRANT_COLLECTION"]
OLLAMA_URL     = os.environ["OLLAMA_URL"]
EMBED_MODEL    = os.environ.get("EMBED_MODEL", "nomic-embed-text")
CHUNK_SIZE     = int(os.environ.get("CHUNK_SIZE_TOKENS", "400"))
CHUNK_OVERLAP  = int(os.environ.get("CHUNK_OVERLAP_TOKENS", "50"))
ACCOUNT_LABEL  = os.environ.get("ACCOUNT_LABEL", COLLECTION)
GMAIL_ADDRESS  = os.environ["GMAIL_ADDRESS"]
DB_PATH        = MAILDIR / ".ingest_state.sqlite"

# nomic-embed-text produces 768-dim vectors
VECTOR_SIZE    = 768

MAX_EMBED_CHARS = 2000  # conservative limit (~2 chars per token average)

# ── Helpers ───────────────────────────────────────────────────────────────────

def decode_header_value(raw):
    if not raw:
        return ""
    try:
        return str(make_header(decode_header(raw)))
    except Exception:
        return str(raw)


def extract_body(msg: email.message.Message) -> str:
    """Extract plain text from a (possibly multipart) email."""
    parts = []
    for part in msg.walk():
        ct = part.get_content_type()
        disp = str(part.get("Content-Disposition") or "")
        if "attachment" in disp:
            continue
        if ct == "text/plain":
            try:
                parts.append(part.get_payload(decode=True).decode(
                    part.get_content_charset() or "utf-8", errors="replace"))
            except Exception:
                pass
        elif ct == "text/html" and not parts:
            try:
                html = part.get_payload(decode=True).decode(
                    part.get_content_charset() or "utf-8", errors="replace")
                parts.append(BeautifulSoup(html, "html.parser").get_text(separator="\n"))
            except Exception:
                pass
    return "\n".join(parts).strip()


def simple_chunk(text: str, chunk_size: int, overlap: int) -> list[str]:
    """Word-boundary chunking (avoids tiktoken dependency at scale)."""
    words = text.split()
    chunks, i = [], 0
    while i < len(words):
        chunk = " ".join(words[i : i + chunk_size])
        if chunk:
            chunks.append(chunk)
        i += chunk_size - overlap
    return chunks or [text]


def embed(texts: list[str]) -> list[list[float]]:
    results = []
    for t in texts:
        t = t[:MAX_EMBED_CHARS]
        if not t or not t.strip():
            raise ValueError("Empty input text passed to embed()")
        try:
            resp = requests.post(f"{OLLAMA_URL}/api/embed",
                                 json={"model": EMBED_MODEL, "input": t},
                                 timeout=60)
            resp.raise_for_status()
        except requests.HTTPError as e:
            print(f"[ingest] Ollama 400 input sample (first 200 chars): {repr(t[:200])}")
            print(f"[ingest] Ollama response body: {e.response.text}")
            raise
        data = resp.json()
        if "embeddings" not in data:
            raise ValueError(f"Unexpected Ollama response: {data}")
        results.append(data["embeddings"][0])
    return results


def stable_id(message_id: str, chunk_index: int) -> str:
    """Deterministic Qdrant point ID from message-id + chunk index."""
    raw = f"{message_id}::{chunk_index}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]

# ── State DB ──────────────────────────────────────────────────────────────────

def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ingested (
            file_path TEXT PRIMARY KEY,
            message_id TEXT,
            ingested_at INTEGER
        )
    """)
    conn.commit()
    return conn


def already_ingested(conn, message_id: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM ingested WHERE message_id=?", (message_id,)
    ).fetchone() is not None


def mark_ingested(conn, message_id: str):
    conn.execute(
        "INSERT OR REPLACE INTO ingested VALUES (?,?)",
        (message_id, int(time.time()))
    )
    conn.commit()

# ── Qdrant setup ──────────────────────────────────────────────────────────────

def ensure_collection(client: QdrantClient):
    existing = {c.name for c in client.get_collections().collections}
    if COLLECTION not in existing:
        print(f"[ingest] Creating Qdrant collection '{COLLECTION}'")
        client.create_collection(
            collection_name=COLLECTION,
            vectors_config=models.VectorParams(
                size=VECTOR_SIZE,
                distance=models.Distance.COSINE,
            ),
        )

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    conn   = open_db()
    client = QdrantClient(url=QDRANT_URL)
    ensure_collection(client)

    # Collect all Maildir message files across all subdirectories
    new_files = []
    for subdir in MAILDIR.rglob("cur"):
        for f in subdir.iterdir():
            if f.is_file() and not f.name.startswith("."):
                if not already_ingested(conn, str(f)):
                    new_files.append(f)
    for subdir in MAILDIR.rglob("new"):
        for f in subdir.iterdir():
            if f.is_file() and not f.name.startswith("."):
                if not already_ingested(conn, str(f)):
                    new_files.append(f)

    if not new_files:
        print("[ingest] No new messages to ingest.")
        return

    print(f"[ingest] Ingesting {len(new_files)} new messages into '{COLLECTION}'...")

    processed = 0
    for path in new_files:
        try:
            with open(path, "rb") as fh:
                msg = email.message_from_binary_file(fh)
        except Exception as e:
            print(f"[ingest] WARNING: Could not parse {path}: {e}")
            continue

        message_id = (msg.get("Message-ID") or str(path)).strip()

        if already_ingested(conn, message_id):
            continue

        subject     = decode_header_value(msg.get("Subject") or "(no subject)")
        from_addr   = decode_header_value(msg.get("From") or "")
        to_addr     = decode_header_value(msg.get("To") or "")
        date_str    = msg.get("Date") or ""
        thread_id   = decode_header_value(msg.get("Thread-Index") or msg.get("In-Reply-To") or message_id)
        body        = extract_body(msg)
        folder      = str(path.parent.parent.relative_to(MAILDIR))

        if not body:
            mark_ingested(conn, message_id)
            continue

        # Build a rich text blob for embedding: header context + body chunk
        header_ctx = (
            f"From: {from_addr}\n"
            f"To: {to_addr}\n"
            f"Date: {date_str}\n"
            f"Subject: {subject}\n\n"
        )

        chunks = simple_chunk(body, CHUNK_SIZE, CHUNK_OVERLAP)
        points = []

        try:
            vectors = embed([(header_ctx + c)[:MAX_EMBED_CHARS] for c in chunks])
        except Exception as e:
            print(f"[ingest] Embed failed for {message_id}: {e}")
            continue

        for idx, (chunk, vector) in enumerate(zip(chunks, vectors)):
            points.append(models.PointStruct(
                id=stable_id(message_id, idx),
                vector=vector,
                payload={
                    "message_id":    message_id,
                    "chunk_index":   idx,
                    "chunk_total":   len(chunks),
                    "subject":       subject,
                    "from":          from_addr,
                    "to":            to_addr,
                    "date":          date_str,
                    "thread_id":     thread_id,
                    "folder":        folder,
                    "account":       GMAIL_ADDRESS,
                    "account_label": ACCOUNT_LABEL,
                    "body_chunk":    chunk,
                },
            ))

        if points:
            client.upsert(collection_name=COLLECTION, points=points)

        mark_ingested(conn, str(path), message_id)
        processed += 1
        if processed % 500 == 0:
            print(f"[ingest] ... {processed}/{len(new_files)}")

    print(f"[ingest] Done. {processed} messages ingested.")


if __name__ == "__main__":
    main()