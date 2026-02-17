# tests/resources/get.py
import json
from typing import Dict, Any

class GetOnlyResource:
    def __init__(self, name: str = "pkg", version: str = None, _exist: bool = True, **_):
        self.name = name
        self.version = version
        self._exist = _exist

    @classmethod
    def from_json(cls, json_str: str, operation: str = None) -> "GetOnlyResource":
        data = json.loads(json_str or "{}")
        return cls(
            name=data.get("name", "pkg"),
            version=data.get("version"),
            _exist=data.get("_exist", True)
        )

    def get(self) -> Dict[str, Any]:
        # Minimal, deterministic state (what your adapter expects to embed into actualState)
        state = {
            "name": self.name,
            "_exist": bool(self._exist)
        }
        if self.version:
            state["version"] = self.version
        return state

