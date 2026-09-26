#!/usr/bin/env python3
"""Print a PostgreSQL SCRAM-SHA-256 verifier for a password read from stdin.

Pass the output to bootstrap-roles.sql (-v app_password=...) so the plaintext
never reaches the server. Reads stdin so the password is not in argv.
ASCII passwords only (Postgres applies SASLprep to non-ASCII input).
"""
import base64
import hashlib
import hmac
import os
import sys

ITERATIONS = 4096


def verifier(password: bytes) -> str:
    salt = os.urandom(16)
    salted = hashlib.pbkdf2_hmac("sha256", password, salt, ITERATIONS)
    client_key = hmac.new(salted, b"Client Key", "sha256").digest()
    stored_key = hashlib.sha256(client_key).digest()
    server_key = hmac.new(salted, b"Server Key", "sha256").digest()
    b64 = lambda b: base64.b64encode(b).decode()
    return f"SCRAM-SHA-256${ITERATIONS}:{b64(salt)}${b64(stored_key)}:{b64(server_key)}"


def main() -> int:
    password = sys.stdin.readline().rstrip("\n")
    if not password or not password.isascii():
        print("password must be non-empty ASCII", file=sys.stderr)
        return 1
    print(verifier(password.encode()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
