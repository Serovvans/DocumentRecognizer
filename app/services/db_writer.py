import json
import threading

import psycopg2
import psycopg2.pool
from sshtunnel import SSHTunnelForwarder

from app import config as cfg


def _qi(name: str) -> str:
    """Quote a PostgreSQL identifier."""
    return '"' + name.replace('"', '""') + '"'


class DBWriter:
    """Thread-safe DB writer that connects via SSH tunnel using psycopg2."""

    def __init__(self, schema: str, table: str, fields: list[str]):
        self.schema = schema
        self.table = table
        self.fields = fields  # user-defined field names (keys in extracted JSON)

        self._tunnel: SSHTunnelForwarder | None = None
        self._pool: psycopg2.pool.ThreadedConnectionPool | None = None
        self._lock = threading.Lock()
        self.inserted = 0
        self.errors = 0

    # ── lifecycle ────────────────────────────────────────────────────────────

    def start(self, max_workers: int = 4) -> None:
        tunnel_kwargs: dict = dict(
            ssh_address_or_host=(cfg.SSH_HOST, cfg.SSH_PORT),
            ssh_username=cfg.SSH_USERNAME,
            remote_bind_address=(cfg.DB_HOST, cfg.DB_PORT),
            local_bind_address=("127.0.0.1",),
            set_keepalive=30,
        )
        if cfg.SSH_PASSWORD:
            tunnel_kwargs["ssh_password"] = cfg.SSH_PASSWORD
        if cfg.SSH_KEY_FILE:
            tunnel_kwargs["ssh_pkey"] = cfg.SSH_KEY_FILE

        self._tunnel = SSHTunnelForwarder(**tunnel_kwargs)
        self._tunnel.start()

        self._pool = psycopg2.pool.ThreadedConnectionPool(
            1,
            max(2, max_workers + 1),
            host="127.0.0.1",
            port=self._tunnel.local_bind_port,
            database=cfg.DB_NAME,
            user=cfg.DB_USER,
            password=cfg.DB_PASSWORD,
        )
        self._ensure_columns()

    def stop(self) -> None:
        if self._pool:
            try:
                self._pool.closeall()
            except Exception:
                pass
        if self._tunnel:
            try:
                self._tunnel.stop()
            except Exception:
                pass

    # ── schema management ────────────────────────────────────────────────────

    def _ensure_columns(self) -> None:
        t = f"{_qi(self.schema)}.{_qi(self.table)}"
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"ALTER TABLE {t} ADD COLUMN IF NOT EXISTS {_qi('source_file')} TEXT"
                )
                for field in self.fields:
                    cur.execute(
                        f"ALTER TABLE {t} ADD COLUMN IF NOT EXISTS {_qi(field)} TEXT"
                    )
            conn.commit()
        finally:
            self._pool.putconn(conn)

    # ── write ────────────────────────────────────────────────────────────────

    def write(self, source_file: str, data: dict) -> None:
        all_fields = ["source_file"] + self.fields
        values: list = [source_file]
        for field in self.fields:
            val = data.get(field)
            if isinstance(val, list):
                val = json.dumps(val, ensure_ascii=False)
            elif val is not None:
                val = str(val)
            values.append(val)

        t = f"{_qi(self.schema)}.{_qi(self.table)}"
        cols = ", ".join(_qi(f) for f in all_fields)
        placeholders = ", ".join("%s" for _ in all_fields)

        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO {t} ({cols}) VALUES ({placeholders})", values)
            conn.commit()
            with self._lock:
                self.inserted += 1
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            with self._lock:
                self.errors += 1
            raise
        finally:
            self._pool.putconn(conn)
