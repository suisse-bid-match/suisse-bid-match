from __future__ import annotations

import sys
from pathlib import Path



def repo_root_from_here() -> Path:
    # app/core_bridge.py -> backend -> web -> src -> repo_root
    return Path(__file__).resolve().parents[4]



def ensure_core_import_path() -> Path:
    core_root = repo_root_from_here() / "src" / "core"
    core_root_str = str(core_root)
    if core_root_str not in sys.path:
        sys.path.insert(0, core_root_str)
    return core_root
