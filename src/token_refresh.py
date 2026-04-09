#!/usr/bin/env python3
"""
Called by mbsync via PassCmd to produce a fresh OAuth2 access token.
Reads credentials from environment variables.
"""
import os, sys, requests

resp = requests.post(
    "https://oauth2.googleapis.com/token",
    data={
        "client_id":     os.environ["GMAIL_CLIENT_ID"],
        "client_secret": os.environ["GMAIL_CLIENT_SECRET"],
        "refresh_token": os.environ["GMAIL_REFRESH_TOKEN"],
        "grant_type":    "refresh_token",
    },
    timeout=15,
)
resp.raise_for_status()
token = resp.json().get("access_token")
if not token:
    print(f"Token refresh failed: {resp.text}", file=sys.stderr)
    sys.exit(1)
print(token)   # mbsync reads stdout