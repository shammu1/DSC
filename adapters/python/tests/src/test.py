
# tests/resources/test_only_resource.py
import json
from typing import Dict, Any, List, Tuple

class TestOnlyResource:
    def __init__(self, name: str = "pkg", _exist: bool = True, desired_exist: bool = True, **_):
        self.name = name
        self._exist = _exist
        self.desired_exist = desired_exist

    @classmethod
    def from_json(cls, json_str: str, operation: str = None) -> "TestOnlyResource":
        data = json.loads(json_str or "{}")
        return cls(
            name=data.get("name", "pkg"),
            _exist=data.get("_exist", True),
            desired_exist=data.get("desired_exist", True)
        )

    def test(self) -> Tuple[Dict[str, Any], List[str]]:
        actual = {"name": self.name, "_exist": bool(self._exist)}
        diffs: List[str] = []
        if bool(self._exist) != bool(self.desired_exist):
            diffs.append("_exist")
        # Contract: (actual_state_dict, diffs_list)
        return actual, diffs
