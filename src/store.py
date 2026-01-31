import sqlite3
import json
import time
from typing import Optional, Any, Dict

SCHEMA = """
CREATE TABLE IF NOT EXISTS chunk_cache (
  chunk_key TEXT PRIMARY KEY,
  created_utc INTEGER NOT NULL,
  shazam_json TEXT
);
"""

class Store:
    """
    Simple SQLite cache:
    - chunk_key -> raw Shazam response JSON (or NULL if no match)
    """
    def __init__(self, path: str):
        self.path = path
        self._init()

    def _init(self) -> None:
        with sqlite3.connect(self.path) as con:
            con.executescript(SCHEMA)

    def get_chunk(self, chunk_key: str) -> Optional[Dict[str, Any]]:
        with sqlite3.connect(self.path) as con:
            cur = con.execute("SELECT shazam_json FROM chunk_cache WHERE chunk_key=?", (chunk_key,))
            row = cur.fetchone()
            if not row or not row[0]:
                return None
            return json.loads(row[0])

    def put_chunk(self, chunk_key: str, shazam_obj: Optional[Dict[str, Any]]) -> None:
        with sqlite3.connect(self.path) as con:
            con.execute(
                "INSERT OR REPLACE INTO chunk_cache (chunk_key, created_utc, shazam_json) VALUES (?, ?, ?)",
                (chunk_key, int(time.time()), json.dumps(shazam_obj) if shazam_obj else None)
            )
            con.commit()
