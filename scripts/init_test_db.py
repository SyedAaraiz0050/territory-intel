# scripts/init_test_db.py
from __future__ import annotations

import os
from pathlib import Path

from src.store import Store

TEST_DB = "territory_test.db"


def main() -> None:
    # Create a clean, separate DB for testing (does NOT touch your main territory.db)
    db_path = Path(TEST_DB)
    if db_path.exists():
        db_path.unlink()

    # If your config reads DB_PATH, this makes it consistent for anything that uses get_settings()
    os.environ["DB_PATH"] = str(db_path)

    store = Store(db_path=str(db_path))
    store.init_schema()

    print(f"✅ Created fresh test DB: {db_path.resolve()}")


if __name__ == "__main__":
    main()