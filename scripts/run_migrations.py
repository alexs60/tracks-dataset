from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from workers.lib.env import load_repo_env
from workers.lib.db import connect, is_postgres, load_sql_file, run_migrations


load_repo_env(PROJECT_ROOT)


def discover_migrations() -> list[Path]:
    migrations_dir = Path("migrations")
    if is_postgres():
        return sorted(migrations_dir.glob("*.pg.sql"))
    return sorted(p for p in migrations_dir.glob("*.sql") if not p.name.endswith(".pg.sql"))


def main() -> None:
    paths = discover_migrations()
    if not paths:
        print("No migrations found")
        return
    with connect() as conn:
        for path in paths:
            run_migrations(conn, load_sql_file(path))
            print(f"Applied migrations from {path}")


if __name__ == "__main__":
    main()