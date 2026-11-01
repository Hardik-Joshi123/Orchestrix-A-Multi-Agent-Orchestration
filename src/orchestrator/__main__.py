"""Direct module execution entry point (`python -m orchestrator`)."""

from __future__ import annotations

import sys

from .main import cli

if __name__ == "__main__":
    sys.exit(cli())
