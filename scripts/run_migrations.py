from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from workers.lib.env import load_repo_env
from workers.lib.db import connect, is_postgres, load_sql_file, run_migrations


load_repo_env(PROJECT_ROOT)


def main() -> None:
    sql_path = Path(
        "migrations/001_audio_features.pg.sql" if is_postgres()
        else "migrations/001_audio_features.sql"
    )
    sql_text = load_sql_file(sql_path)
    with connect() as conn:
        run_migrations(conn, sql_text)
    print(f"Applied migrations from {sql_path}")


if __name__ == "__main__":
    main()