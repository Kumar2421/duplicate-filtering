from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


class ManifestWriter:
    def read_json(self, path: str | Path) -> Optional[Dict[str, Any]]:
        p = Path(path)
        if not p.exists():
            return None
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return None

    def write_json(self, path: str | Path, data: Dict[str, Any]) -> str:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        return str(p)
