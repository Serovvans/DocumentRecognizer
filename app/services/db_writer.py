import threading
from datetime import datetime

import psycopg2
import psycopg2.pool
from sshtunnel import SSHTunnelForwarder

from app import config as cfg


def _qi(name: str) -> str:
    """Quote a PostgreSQL identifier."""
    return '"' + name.replace('"', '""') + '"'


_SQL_TYPE_MAP = {
    "text":    "TEXT",
    "double":  "DOUBLE PRECISION",
    "integer": "INTEGER",
    "date":    "DATE",
}


def _sql_type(db_type: str) -> str:
    return _SQL_TYPE_MAP.get(db_type, "TEXT")


def _cast_value(val, db_type: str):
    """Convert an extracted string value to the appropriate Python type for psycopg2."""
    if val is None:
        return None
    if db_type == "double":
        s = str(val).strip().replace(",", ".")
        try:
            return float(s)
        except (ValueError, TypeError):
            return None
    if db_type == "integer":
        s = str(val).strip().replace(",", ".").split(".")[0]
        s = "".join(c for c in s if c.isdigit() or c == "-")
        try:
            return int(s)
        except (ValueError, TypeError):
            return None
    if db_type == "date":
        s = str(val).strip()
        for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
        return None
    return str(val)


class DBWriter:
    """Thread-safe DB writer that connects via SSH tunnel using psycopg2.

    Each field dict must have a "name" key and optional keys:
      - "multi_value_mode": "rows" (default) or "columns"
      - "db_type": "text" (default), "double", "integer", "date"

    multi_value_mode:
      - "rows": if the extracted value is a list, one DB row is inserted per list
        element; scalar fields are repeated across all rows.
      - "columns": list values are spread into columns field_1, field_2, … field_N;
        always produces exactly one DB row per document.
    """

    def __init__(
        self,
        schema: str,
        table: str,
        fields: list[dict],
        save_source: bool = True,
        db_name: str = "",
        db_user: str = "",
        db_password: str = "",
    ):
        self.schema = schema
        self.table = table
        self.fields = fields
        self.save_source = save_source
        self._db_name = db_name or cfg.DB_NAME
        self._db_user = db_user or cfg.DB_USER
        self._db_password = db_password if db_password else cfg.DB_PASSWORD

        self._tunnel: SSHTunnelForwarder | None = None
        self._pool: psycopg2.pool.ThreadedConnectionPool | None = None
        self._lock = threading.Lock()
        self._col_lock = threading.Lock()
        self._known_columns: set[str] = set()
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
            database=self._db_name,
            user=self._db_user,
            password=self._db_password,
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
        """Add fixed columns (source_file + all "rows" mode fields) at startup.

        Also drops NOT NULL constraints from managed columns so that documents
        with missing fields can still be inserted.
        """
        t = f"{_qi(self.schema)}.{_qi(self.table)}"
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                if self.save_source:
                    cur.execute(
                        f"ALTER TABLE {t} ADD COLUMN IF NOT EXISTS {_qi('source_file')} TEXT"
                    )
                    cur.execute(
                        f"ALTER TABLE {t} ALTER COLUMN {_qi('source_file')} DROP NOT NULL"
                    )
                    self._known_columns.add("source_file")
                for field in self.fields:
                    if field.get("multi_value_mode", "rows") != "columns":
                        name = field["name"]
                        col_type = _sql_type(field.get("db_type", "text"))
                        cur.execute(
                            f"ALTER TABLE {t} ADD COLUMN IF NOT EXISTS {_qi(name)} {col_type}"
                        )
                        cur.execute(
                            f"ALTER TABLE {t} ALTER COLUMN {_qi(name)} DROP NOT NULL"
                        )
                        self._known_columns.add(name)
                # Handwriting quality flag
                cur.execute(
                    f"ALTER TABLE {t} ADD COLUMN IF NOT EXISTS {_qi('low_ocr_quality')} BOOLEAN"
                )
                cur.execute(
                    f"ALTER TABLE {t} ALTER COLUMN {_qi('low_ocr_quality')} DROP NOT NULL"
                )
                self._known_columns.add("low_ocr_quality")
            conn.commit()
        finally:
            self._pool.putconn(conn)

    def _add_column(self, conn, col_name: str, db_type: str = "text") -> None:
        """Ensure a column exists; thread-safe and idempotent."""
        with self._col_lock:
            if col_name in self._known_columns:
                return
            t = f"{_qi(self.schema)}.{_qi(self.table)}"
            col_type = _sql_type(db_type)
            with conn.cursor() as cur:
                cur.execute(
                    f"ALTER TABLE {t} ADD COLUMN IF NOT EXISTS {_qi(col_name)} {col_type}"
                )
            conn.commit()
            self._known_columns.add(col_name)

    # ── write ────────────────────────────────────────────────────────────────

    def write(self, source_file: str, data: dict) -> None:
        rows_fields: list[tuple[str, object, str]] = []
        cols_fields: list[tuple[str, object, str]] = []
        for field in self.fields:
            name = field["name"]
            val = data.get(name)
            db_type = field.get("db_type", "text")
            if field.get("multi_value_mode", "rows") == "columns":
                cols_fields.append((name, val, db_type))
            else:
                rows_fields.append((name, val, db_type))

        # How many DB rows to insert (max list length among "rows" mode fields)
        n_rows = 1
        for _name, val, _db_type in rows_fields:
            if isinstance(val, list):
                n_rows = max(n_rows, len(val))

        conn = self._pool.getconn()
        try:
            # Ensure "columns" mode columns exist before inserting
            for name, val, db_type in cols_fields:
                if isinstance(val, list):
                    for i in range(1, len(val) + 1):
                        self._add_column(conn, f"{name}_{i}", db_type)
                else:
                    self._add_column(conn, name, db_type)

            t = f"{_qi(self.schema)}.{_qi(self.table)}"
            n_inserted = 0
            for row_idx in range(n_rows):
                col_names: list[str] = []
                values: list = []

                if self.save_source:
                    col_names.append("source_file")
                    values.append(source_file)

                if "has_handwriting_issues" in data:
                    col_names.append("low_ocr_quality")
                    flag = data["low_ocr_quality"]
                    values.append(bool(flag) if flag is not None else None)

                for name, val, db_type in rows_fields:
                    col_names.append(name)
                    if isinstance(val, list):
                        v = val[row_idx] if row_idx < len(val) else None
                        values.append(_cast_value(v, db_type))
                    else:
                        values.append(_cast_value(val, db_type))

                for name, val, db_type in cols_fields:
                    if isinstance(val, list):
                        for i, v in enumerate(val, 1):
                            col_names.append(f"{name}_{i}")
                            values.append(_cast_value(v, db_type))
                    else:
                        col_names.append(name)
                        values.append(_cast_value(val, db_type))

                cols_sql = ", ".join(_qi(c) for c in col_names)
                placeholders = ", ".join("%s" for _ in col_names)
                with conn.cursor() as cur:
                    cur.execute(
                        f"INSERT INTO {t} ({cols_sql}) VALUES ({placeholders})", values
                    )
                n_inserted += 1

            conn.commit()
            with self._lock:
                self.inserted += n_inserted
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
