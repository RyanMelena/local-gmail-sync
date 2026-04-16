#!/usr/bin/env python3
"""
Walks the Maildir at /mail, embeds new messages, and upserts to Qdrant.
State is tracked in /mail/.ingest_state.sqlite so restarts are safe/idempotent.
"""
import os, email, hashlib, sqlite3, requests, time
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
CHUNK_SIZE     = int(os.environ.get("CHUNK_SIZE_TOKENS", "200"))
CHUNK_OVERLAP  = int(os.environ.get("CHUNK_OVERLAP_TOKENS", "20"))
ACCOUNT_LABEL  = os.environ.get("ACCOUNT_LABEL", COLLECTION)
GMAIL_ADDRESS  = os.environ["GMAIL_ADDRESS"]
DB_PATH        = MAILDIR / ".ingest_state.sqlite"
VECTOR_SIZE    = 768
MAX_EMBED_CHARS = 2000
UPSERT_BATCH   = int(os.environ.get("UPSERT_BATCH_SIZE", "100"))   # messages per Qdrant upsert
EMBED_BATCH    = int(os.environ.get("EMBED_BATCH_SIZE", "32"))      # chunks per Ollama call
DB_COMMIT_EVERY = int(os.environ.get("DB_COMMIT_EVERY", "200"))     # messages per SQLite commit

# ── Helpers ───────────────────────────────────────────────────────────────────

def decode_hdr(raw):
    if not raw:
        return ""
    try:
        return str(make_header(decode_header(raw)))
    except Exception:
        return str(raw)


def extract_body(msg: email.message.Message) -> str:
    parts = []
    for part in msg.walk():
        ct   = part.get_content_type()
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


def simple_chunk(text: str) -> list[str]:
    words = text.split()
    out, i = [], 0
    while i < len(words):
        c = " ".join(words[i:i + CHUNK_SIZE])
        if c:
            out.append(c)
        i += CHUNK_SIZE - CHUNK_OVERLAP
    return out or [text]


def embed_batch(texts: list[str]) -> list[list[float]]:
    """Send a batch of texts to Ollama in a single request."""
    truncated = [t[:MAX_EMBED_CHARS] for t in texts]
    try:
        resp = requests.post(
            f"{OLLAMA_URL}/api/embed",
            json={"model": EMBED_MODEL, "input": truncated},
            timeout=120,
        )
        resp.raise_for_status()
    except requests.HTTPError as e:
        print(f"[ingest] Ollama error: {e.response.text}")
        raise
    data = resp.json()
    if "embeddings" not in data:
        raise ValueError(f"Unexpected Ollama response: {data}")
    return data["embeddings"]


def stable_id(message_id: str, chunk_index: int) -> str:
    return hashlib.sha256(f"{message_id}::{chunk_index}".encode()).hexdigest()[:32]

# ── State DB ──────────────────────────────────────────────────────────────────

def open_db() -> tuple[sqlite3.Connection, set[str], set[str]]:
    """
    Opens the state DB and returns:
      - the connection
      - set of all known message_ids (for dedup)
      - set of all known file paths (for fast pre-filter)
    Loading both into memory at startup eliminates per-file DB queries.
    """
    conn = sqlite3.connect(DB_PATH, isolation_level=None)  # manual transaction control
    conn.execute("PRAGMA journal_mode=WAL")                # faster concurrent writes
    conn.execute("PRAGMA synchronous=NORMAL")              # safe but faster than FULL
    conn.execute("""CREATE TABLE IF NOT EXISTS ingested (
        message_id TEXT PRIMARY KEY,
        ingested_at INTEGER
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS seen_paths (
        file_path TEXT PRIMARY KEY
    )""")
    conn.commit()

    known_ids   = {row[0] for row in conn.execute("SELECT message_id FROM ingested")}
    known_paths = {row[0] for row in conn.execute("SELECT file_path FROM seen_paths")}
    return conn, known_ids, known_paths


def flush_db(conn, new_ids: list[str], new_paths: list[str]):
    """Batch commit message_ids and file paths to the DB."""
    conn.execute("BEGIN")
    conn.executemany(
        "INSERT OR IGNORE INTO ingested VALUES (?,?)",
        [(mid, int(time.time())) for mid in new_ids]
    )
    conn.executemany(
        "INSERT OR IGNORE INTO seen_paths VALUES (?)",
        [(p,) for p in new_paths]
    )
    conn.execute("COMMIT")

# ── Qdrant ────────────────────────────────────────────────────────────────────

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
    conn, known_ids, known_paths = open_db()
    client = QdrantClient(url=QDRANT_URL)
    ensure_collection(client)

    # ── Fast file discovery using in-memory path set ──────────────────────────
    new_files = []
    for subdir in list(MAILDIR.rglob("cur")) + list(MAILDIR.rglob("new")):
        for f in subdir.iterdir():
            if f.is_file() and not f.name.startswith("."):
                if str(f) not in known_paths:
                    new_files.append(f)

    if not new_files:
        print("[ingest] No new messages to ingest.")
        return

    print(f"[ingest] Evaluating {len(new_files)} new files...")

    # ── Accumulators for batched writes ───────────────────────────────────────
    pending_points  : list[models.PointStruct] = []
    pending_ids     : list[str] = []
    pending_paths   : list[str] = []
    processed = 0
    skipped   = 0
    errors    = 0

    def flush_qdrant():
        if pending_points:
            try:
                client.upsert(collection_name=COLLECTION, points=pending_points)
            except Exception as e:
                print(f"[ingest] WARNING: Qdrant upsert failed: {e}")
            pending_points.clear()

    def flush_all():
        flush_qdrant()
        if pending_ids or pending_paths:
            flush_db(conn, pending_ids, pending_paths)
            pending_ids.clear()
            pending_paths.clear()

    for path in new_files:
        path_str = str(path)
        try:
            with open(path, "rb") as fh:
                msg = email.message_from_binary_file(fh)
        except Exception as e:
            print(f"[ingest] WARNING: Could not parse {path}: {e}")
            pending_paths.append(path_str)
            continue

        message_id = (msg.get("Message-ID") or path_str).strip()

        # Always record the path so we don't re-open this file next cycle
        pending_paths.append(path_str)
        known_paths.add(path_str)

        if message_id in known_ids:
            skipped += 1
            # Flush paths periodically even when skipping
            if len(pending_paths) >= DB_COMMIT_EVERY:
                flush_db(conn, [], pending_paths)
                pending_paths.clear()
            continue

        subject   = decode_hdr(msg.get("Subject") or "(no subject)")
        from_addr = decode_hdr(msg.get("From") or "")
        to_addr   = decode_hdr(msg.get("To") or "")
        date_str  = msg.get("Date") or ""
        thread_id = decode_hdr(msg.get("In-Reply-To") or message_id)
        body      = extract_body(msg)
        folder    = str(path.parent.parent.relative_to(MAILDIR))

        if not body:
            known_ids.add(message_id)
            pending_ids.append(message_id)
            continue

        header_ctx = (
            f"From: {from_addr}\nTo: {to_addr}\n"
            f"Date: {date_str}\nSubject: {subject}\n\n"
        )
        chunks = simple_chunk(body)
        inputs = [(header_ctx + c)[:MAX_EMBED_CHARS] for c in chunks]

        # ── Batch embed in EMBED_BATCH-sized groups ────────────────────────────
        all_vectors = []
        try:
            for i in range(0, len(inputs), EMBED_BATCH):
                all_vectors.extend(embed_batch(inputs[i:i + EMBED_BATCH]))
        except Exception as e:
            print(f"[ingest] Embed failed for {message_id}: {e}")
            errors += 1
            continue

        for idx, (chunk, vector) in enumerate(zip(chunks, all_vectors)):
            pending_points.append(models.PointStruct(
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

        known_ids.add(message_id)
        pending_ids.append(message_id)
        processed += 1

        # ── Flush when batch thresholds are reached ────────────────────────────
        if len(pending_points) >= UPSERT_BATCH * 10:  # ~10 chunks avg per message
            flush_qdrant()
        if len(pending_ids) >= DB_COMMIT_EVERY:
            flush_db(conn, pending_ids, pending_paths)
            pending_ids.clear()
            pending_paths.clear()

        if processed % 500 == 0:
            print(f"[ingest] ... {processed} ingested, {skipped} skipped, {errors} errors")

    # ── Final flush ───────────────────────────────────────────────────────────
    flush_all()
    print(f"[ingest] Done. {processed} ingested, {skipped} skipped, {errors} errors.")


if __name__ == "__main__":
    main()