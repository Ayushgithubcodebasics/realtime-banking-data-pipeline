from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_project_env() -> Path:
    """Load environment variables from the project root.

    Priority order:
      1. .env.local
      2. .env

    Returns the project root path so callers can easily reuse it.
    """
    for name in (".env.local", ".env"):
        env_path = PROJECT_ROOT / name
        if env_path.exists():
            load_dotenv(env_path, override=False)
    return PROJECT_ROOT


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value not in (None, "") else default


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value not in (None, "") else default
