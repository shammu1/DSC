import json
from typing import Any, Dict

def parse_json(s: str) -> Dict[str, Any]:
    try:
        return json.loads(s or "{}")
    except Exception:
        return {}