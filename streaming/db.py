"""Shared PostgreSQL connection helper for the streaming components.

Credentials come only from environment variables (or a local, git-ignored
``.env`` file); nothing sensitive is hard-coded.
"""

import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def db_params() -> dict:
    password = os.getenv("DB_PASSWORD")
    if not password:
        raise RuntimeError("DB_PASSWORD is not set. Copy .env.example to .env and fill it in.")
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "dbname": os.getenv("DB_NAME", "etl_dw"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": password,
    }


def get_db_connection():
    return psycopg2.connect(connect_timeout=10, **db_params())
