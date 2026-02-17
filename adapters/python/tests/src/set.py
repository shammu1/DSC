# tests/resources/set.py
import json
from typing import Dict, Any, List

class SetOnlyResource:
    def __init__(self, name: str = "pkg", _exist: bool = True, want_exist: bool = True, **_):
        self.name = name
        self._exist = _exist          # current
        self.want_exist = want_exist  # desired

    @classmethod
    def from_json(cls, json_str: str, operation: str = None) -> "SetOnlyResource":
        data = json.loads(json_str or "{}")
        return cls(
            name=data.get("name", "pkg"),
            _exist=data.get("_exist", False),       # simulate "before"
            want_exist=data.get("want_exist", True) # test-only desired knob
        )

    def set(self) -> Dict[str, Any]:
        # Simulate idempotent flip to desired
        after = self.want_exist
        diffs: List[str] = []
        if after != self._exist:
            diffs.append("_exist")

        state: Dict[str, Any] = {
            "name": self.name,
            "_exist": after
        }
        # Contract: return an object with state + differingProperties
        return state, diffs

