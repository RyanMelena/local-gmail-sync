#!/usr/bin/env python3
"""
Minimal HTTP API wrapping notmuch for exact/boolean keyword search.
Listens on 0.0.0.0:8080 on the default bridge network.
Accessible from OpenWebUI tool via http://email-sync-{collection}:8080
"""
import json, os, subprocess
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

MAILDIR      = "/mail"
NOTMUCH_CFG  = f"{MAILDIR}/.notmuch-config"


def notmuch(*args) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["notmuch", "--config", NOTMUCH_CFG, *args],
        capture_output=True,
        text=True,
    )


def search(query: str, limit: int = 20) -> list[dict]:
    # Get matching message IDs
    id_result = notmuch(
        "search",
        "--output=messages",
        f"--limit={limit}",
        "--sort=newest-first",
        query,
    )
    message_ids = [
        l.strip() for l in id_result.stdout.splitlines() if l.strip()
    ]

    results = []
    for mid in message_ids:
        # Fetch formatted summary for each message
        show = notmuch("show", "--format=json", "--entire-thread=false", mid)
        try:
            data = json.loads(show.stdout)
            # notmuch show returns nested lists: [[[ {message}, ... ]]]
            msg = data[0][0][0]
            headers = msg.get("headers", {})
            results.append({
                "message_id": mid,
                "subject":    headers.get("Subject", ""),
                "from":       headers.get("From", ""),
                "to":         headers.get("To", ""),
                "date":       headers.get("Date", ""),
                "tags":       msg.get("tags", []),
                "body":       (msg.get("body") or [{}])[0].get("content", "")[:500],
            })
        except Exception as e:
            results.append({"message_id": mid, "error": str(e)})

    return results


class Handler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        pass  # suppress per-request access logs

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        if parsed.path != "/search":
            self.send_response(404)
            self.end_headers()
            return

        query = params.get("query", [""])[0].strip()
        limit = int(params.get("limit", ["20"])[0])

        if not query:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'{"error": "query parameter required"}')
            return

        try:
            results = search(query, limit)
            body = json.dumps(results).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode())


if __name__ == "__main__":
    port = int(os.environ.get("NOTMUCH_SERVER_PORT", "8080"))
    print(f"[notmuch_server] Listening on 0.0.0.0:{port}")
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()
