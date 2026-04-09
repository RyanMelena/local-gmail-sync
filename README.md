## Getting OAuth2 Refresh Tokens (One-Time Per Account)

Because mbsync uses IMAP with XOAUTH2 and the `gmail.readonly` scope, you need a refresh token per account. This is a one-time interactive step:

```bash
# Install the helper on your HOST (not in Docker)
pip install google-auth-oauthlib

# Use this script to authorize and print the refresh token
python3 - << 'EOF'
from google_auth_oauthlib.flow import InstalledAppFlow
import json

# Point to the client_secret JSON downloaded from Google Cloud Console
flow = InstalledAppFlow.from_client_secrets_file(
    "client_secret.json",
    scopes=["https://www.googleapis.com/auth/gmail.readonly"]
)
creds = flow.run_local_server(port=0)
print("Refresh token:", creds.refresh_token)
EOF
```

Paste the printed token into .env as the REFRESH_TOKEN value for that account.

> Note: Create one Google Cloud OAuth2 project per user, or reuse one project with separate credentials per account. The `gmail.readonly` scope is enforced server-side — even if a bug existed in any container, Google's API would reject any write or send attempt.