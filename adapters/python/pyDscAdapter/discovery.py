import importlib.util
from pathlib import Path
from typing import Dict

def get_class_map_from_pyproject(pyproject_path: Path) -> Dict[str, str]:
    """
    Parse [tool.dsc.resources] section from pyproject.toml.
    Returns: {"ResourceType": "ClassName", ...}
    No external dependencies required.
    """
    if not pyproject_path.exists():
        return {}
    
    try:
        content = pyproject_path.read_text(encoding="utf-8")
    except Exception:
        return {}
    
    class_map = {}
    in_section = False
    
    for line in content.splitlines():
        stripped = line.strip()
        if stripped == "[tool.dsc.resources]":
            in_section = True
            continue
        if in_section:
            if stripped.startswith("["):
                break
            if "=" in stripped and not stripped.startswith("#"):
                key, val = stripped.split("=", 1)
                key = key.strip().strip('"\'')
                val = val.strip().strip('"\'')
                class_map[key] = val
    
    return class_map

def import_class_from_file(resource_path: Path, resource_type: str, class_name: str) -> type:
    module_name = f"dsc_{resource_type.replace('/', '_').replace('.', '_').lower()}" #if resource_type else f"dsc_{resource_path.stem.lower()}"
    spec = importlib.util.spec_from_file_location(module_name, str(resource_path))
    if not spec or not spec.loader:
        raise ImportError(f"Unable to load module '{resource_path}'")

    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    try:
        return getattr(mod, class_name)
    except AttributeError as e:
        raise ImportError(f"Class '{class_name}' not found in '{resource_path}': {e}")

