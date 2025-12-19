import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for candidate in ("src", "SRC"):
    src_path = ROOT / candidate
    if src_path.exists():
        sys.path.insert(0, str(src_path))
        break
