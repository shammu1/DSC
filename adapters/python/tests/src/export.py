# tests/resources/export_only_resource.py
from typing import Dict, Any, Optional

class ExportOnlyResource:
    @staticmethod
    def export(instance: Optional[object] = None) -> Dict[str, Any]:
        #Future change: _exist will be named different in the resource and mapped with DSC's _exist.
        return {
            "packages": [
                {"name": "alpha", "version": "1.0.0", "_exist": True},
                {"name": "beta", "version": "2.0.0", "_exist": True}
            ]
        }
