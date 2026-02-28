from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path


def _auth_db_path() -> Path:
    configured = os.getenv("PORTAL_AUTH_DB_PATH", "customer_portal/portal_auth.db")
    path = Path(configured)
    return path if path.is_absolute() else Path.cwd() / path


def _secret_key() -> str:
    return os.getenv("PORTAL_AUTH_SECRET", "change-this-in-production")


@dataclass(frozen=True)
class AuthUser:
    user_id: int
    username: str
    full_name: str | None
    customer_id: int
    is_active: bool


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_auth_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def init_auth_db() -> None:
    _auth_db_path().parent.mkdir(parents=True, exist_ok=True)
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS portal_users (
                user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                password_salt TEXT NOT NULL,
                full_name TEXT,
                customer_id INTEGER NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.commit()


def _hash_password(password: str, salt: str) -> str:
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        120_000,
    )
    return base64.urlsafe_b64encode(digest).decode("utf-8")


def create_user(
    username: str,
    password: str,
    customer_id: int,
    full_name: str | None = None,
    is_active: bool = True,
) -> AuthUser:
    init_auth_db()
    salt = secrets.token_urlsafe(24)
    password_hash = _hash_password(password=password, salt=salt)
    with _connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO portal_users (username, password_hash, password_salt, full_name, customer_id, is_active)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (username.strip(), password_hash, salt, full_name, int(customer_id), 1 if is_active else 0),
        )
        conn.commit()
        user_id = int(cursor.lastrowid)
    return AuthUser(
        user_id=user_id,
        username=username.strip(),
        full_name=full_name,
        customer_id=int(customer_id),
        is_active=is_active,
    )


def list_users() -> list[AuthUser]:
    init_auth_db()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT user_id, username, full_name, customer_id, is_active
            FROM portal_users
            ORDER BY user_id
            """
        ).fetchall()
    return [
        AuthUser(
            user_id=int(row["user_id"]),
            username=str(row["username"]),
            full_name=row["full_name"],
            customer_id=int(row["customer_id"]),
            is_active=bool(row["is_active"]),
        )
        for row in rows
    ]


def get_user_by_username(username: str) -> AuthUser | None:
    init_auth_db()
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT user_id, username, full_name, customer_id, is_active
            FROM portal_users
            WHERE username = ?
            """,
            (username.strip(),),
        ).fetchone()
    if row is None:
        return None
    return AuthUser(
        user_id=int(row["user_id"]),
        username=str(row["username"]),
        full_name=row["full_name"],
        customer_id=int(row["customer_id"]),
        is_active=bool(row["is_active"]),
    )


def get_user_by_id(user_id: int) -> AuthUser | None:
    init_auth_db()
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT user_id, username, full_name, customer_id, is_active
            FROM portal_users
            WHERE user_id = ?
            """,
            (int(user_id),),
        ).fetchone()
    if row is None:
        return None
    return AuthUser(
        user_id=int(row["user_id"]),
        username=str(row["username"]),
        full_name=row["full_name"],
        customer_id=int(row["customer_id"]),
        is_active=bool(row["is_active"]),
    )


def verify_user_credentials(username: str, password: str) -> AuthUser | None:
    init_auth_db()
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT user_id, username, full_name, customer_id, is_active, password_hash, password_salt
            FROM portal_users
            WHERE username = ?
            """,
            (username.strip(),),
        ).fetchone()
    if row is None:
        return None
    expected = str(row["password_hash"])
    actual = _hash_password(password=password, salt=str(row["password_salt"]))
    if not hmac.compare_digest(expected, actual):
        return None
    return AuthUser(
        user_id=int(row["user_id"]),
        username=str(row["username"]),
        full_name=row["full_name"],
        customer_id=int(row["customer_id"]),
        is_active=bool(row["is_active"]),
    )


def _encode_part(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _decode_part(data: str) -> bytes:
    padded = data + "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(padded.encode("utf-8"))


def issue_access_token(user: AuthUser, expiry_minutes: int = 60 * 8) -> str:
    payload = {
        "uid": user.user_id,
        "usr": user.username,
        "cid": user.customer_id,
        "exp": int(time.time()) + (expiry_minutes * 60),
    }
    payload_b64 = _encode_part(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signature = hmac.new(
        _secret_key().encode("utf-8"),
        payload_b64.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).digest()
    signature_b64 = _encode_part(signature)
    return f"{payload_b64}.{signature_b64}"


def decode_access_token(token: str) -> dict:
    try:
        payload_b64, signature_b64 = token.split(".", 1)
    except ValueError as exc:
        raise ValueError("Malformed token") from exc

    expected_signature = hmac.new(
        _secret_key().encode("utf-8"),
        payload_b64.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).digest()
    actual_signature = _decode_part(signature_b64)
    if not hmac.compare_digest(expected_signature, actual_signature):
        raise ValueError("Invalid token signature")

    payload_bytes = _decode_part(payload_b64)
    payload = json.loads(payload_bytes.decode("utf-8"))
    if int(payload.get("exp", 0)) < int(time.time()):
        raise ValueError("Token expired")
    return payload

