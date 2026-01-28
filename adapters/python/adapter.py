import argparse
import json
import sys
import cProfile
import pstats
import time
import logging
import io
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Callable
from functools import lru_cache


class JsonFormatter(logging.Formatter):
    """Always outputs: {"<level>": "<message>"} (level is lowercase)."""
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps({record.levelname.lower(): record.getMessage()}, ensure_ascii=False)


# ---------------------------------
# Resource adapter implementation 
# ---------------------------------

_APT_EMBEDDED_SCHEMA: Dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "APT Packages Management",
    "description": "Manages APT Packages on Linux",
    "type": "object",
    "required": ["name"],
    "additionalProperties": False,
    "properties": {
        "name": {"type": "string"},
        "version": {"type": "string"},
        "dependencies": {
            "type": "array",
            "items": {"type": "string"},
            "readOnly": True
        },
        "_exist": {"$ref": "#/$defs/dsc_exist"}
    },
    "$defs": {
        # Inline canonical schema for _exist (workaround for external $ref resolution)
        "dsc_exist": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "https://raw.githubusercontent.com/PowerShell/DSC/main/schemas/2024/04/resource/properties/exist.json",
            "title": "Instance should exist",
            "description": "Indicates whether the DSC resource instance should exist.",
            "type": "boolean",
            "default": True,
            "enum": [False, True]
        }
    }
}

def _parse_json(s: str) -> Dict[str, Any]:
    try:
        return json.loads(s or "{}")
    except Exception:
        return {}

def _is_document_payload(payload: Dict[str, Any]) -> bool:
    return isinstance(payload.get("resources"), list)


class ResourceAdapter:
    """
    Provides:
        - profile_block for lightweight timing/profiling
        - log(level, message, target, **kwargs) for structured logging
        - validate_input_json(json_str, operation) for basic input checks
        - registry to resolve resource type -> class loader
    """

    def __init__(self) -> None:
        # Map resource-type tokens/aliases to loader functions returning class objects
        # Extend here for more resource types.
        self._registry: Dict[str, Callable[[], type]] = {
            # TODO: Will have to decide the type for the apt resource.
            "Microsoft.Linux.Apt/Package": self._load_apt_class,
        }
        
        # Normalize DSC trace level to standard Python logging levels
        # Supported inputs: trace, debug, info, warning, error, critical
        dsc_level = (os.getenv("DSC_TRACE_LEVEL", "info") or "info").strip().lower()
        level_map = {
            "trace": logging.DEBUG,     # map DSC trace -> DEBUG
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warning": logging.WARNING,
            "error": logging.ERROR,
            "critical": logging.CRITICAL,
        }
        effective_level = level_map.get(dsc_level, logging.INFO)

        # You can choose when to enable profiling; common pattern:
        # enable for trace or debug
        self.ENABLE_PROFILING = dsc_level in ("trace", "debug")

        self.logger = logging.getLogger("dsc_adapter")

        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stderr)
            handler.setFormatter(JsonFormatter())
            self.logger.addHandler(handler)

        # Set the effective level
        self.logger.setLevel(effective_level)

    @lru_cache(maxsize=16)
    def _get_instance_schema_from_type(self, resource_type: str) -> Dict[str, Any]:
        """
        Resolve the resource class and call its get_schema() to obtain
        the DSC instance JSON schema. Falls back to the old embedded schema
        if get_schema() is unavailable.
        """
        cls = self._resolve_resource_class(resource_type)
        if hasattr(cls, "get_schema") and callable(getattr(cls, "get_schema")):
            schema = cls.get_schema()
            if not isinstance(schema, dict):
                raise TypeError("get_schema() must return a dict")
            return schema

        # Fallback: last-resort for older resources
        # (You can remove this fallback once all resources implement get_schema().)
        return _APT_EMBEDDED_SCHEMA

    @contextmanager
    def profile_block(self, label):
        if self.ENABLE_PROFILING:
            start_time = time.perf_counter()
            profiler = None
            try:
                profiler = cProfile.Profile()
                profiler.enable()
            except Exception:
                # Another profiler may already be active; fall back to timing only
                profiler = None
            try:
                yield
            finally:
                end_time = time.perf_counter()
                if profiler:
                    try:
                        profiler.disable()
                        s = io.StringIO()
                        ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
                        ps.print_stats(10)
                        self.logger.info(f"[PROFILE] {label} took {end_time - start_time:.4f}s")
                        self.logger.debug(f"[PROFILE DETAILS] {label}:\n{s.getvalue()}")
                    except Exception:
                        # If profiling teardown fails, still log duration
                        self.logger.info(f"[PROFILE] {label} took {end_time - start_time:.4f}s")
                else:
                    self.logger.info(f"[PROFILE] {label} took {end_time - start_time:.4f}s")
        else:
            yield

    def log(self, level: str, message: str, target: str = None, **kwargs) -> None:
        lvl = level.lower()
        method = kwargs.get("method", "?")
        core_msg = f"{target} - {method} - {message}" if target else f"{method} - {message}"

        if lvl == "trace": # and hasattr(self.logger, "trace"):
            self.logger.debug(f"[TRACE] {core_msg}")
            return

        log_fn = getattr(self.logger, lvl, self.logger.info)
        log_fn(core_msg)


    # -----------------------
    # Input JSON verification
    # -----------------------

    # TODO: This is basic validation and I have removed the Pydantic validation from the previous prototype for now. Once the the adapter can get the schema from the resource class, we can reintroduce Pydantic-based validation.
    def validate_input_json(self, json_str: str, operation: Optional[str] = None) -> bool:
        """
        Minimal structural validation that can be reused by resource classes.
        For Apt:
          - name: required (str, non-empty)
          - _exist: optional (bool)
          - version: optional (str)
          - source: optional (str)
          - dependencies: optional (list[str])
        """
        try:
            data = json.loads(json_str or "{}")
        except Exception as e:
            self.log("error", f"Invalid JSON: {e}", "Adapter", operation=operation)
            return False

        name = data.get("name")
        if not isinstance(name, str) or not name.strip():
            self.log("error", "Missing or invalid 'name' field", "Adapter", operation=operation)
            return False

        if "_exist" in data and not isinstance(data["_exist"], bool):
            self.log("error", "Field '_exist' must be a boolean when present", "Adapter", operation=operation)
            return False

        if "dependencies" in data and not isinstance(data["dependencies"], list):
            self.log("error", "Field 'dependencies' must be a list when present", "Adapter", operation=operation)
            return False

        # Operation-aware checks (add as needed)
        # Example: for 'set', ensure _exist is present to decide install/remove
        if operation == "set" and "_exist" not in data:
            self.log("error", "For 'set', '_exist' is required", "Adapter", operation=operation)
            return False

        return True

    # --------------------------
    # Resource loader and lookup
    # --------------------------
    def _load_apt_class(self) -> type:
        """
        Attempts multiple import paths to locate AptPackage class.
        Aligns with your resource-side fallbacks.
        """
        # TODO: This needs to be changed to identify ther resource class from the Python module.
        # Try relative to typical repo layout
        try:
            from resources.apt.AptPackage import AptPackage  # type: ignore
            return AptPackage
        except Exception:
            pass

        # Try package-level absolute import
        try:
            from apt.AptPackage import AptPackage  # type: ignore
            return AptPackage
        except Exception:
            pass

        _here = Path(__file__).resolve()
        _repo_root = _here.parents[2] if len(_here.parents) >= 3 else _here.parent
        _resources_root = _repo_root / "resources"
        for p in (_repo_root, _resources_root):
            p_str = str(p)
            if p_str not in sys.path:
                sys.path.insert(0, p_str)
        try:
            from resources.apt.AptPackage import AptPackage  # type: ignore
            return AptPackage
        except Exception:
            pass

        # Final failure: raise with helpful guidance
        raise ImportError(
            "Unable to import AptPackage. Ensure the module path "
            "'resources.apt.AptPackage' exists or adjust the adapter loader."
        )

    def _resolve_resource_class(self, resource_type: str) -> type:
        key = (resource_type or "").strip()
        if not key:
            raise ValueError("resource-type must be provided")
        loader = self._registry.get(key)
        if not loader:
            # Try case-insensitive lookup by normalizing registry keys
            lowered = {k.lower(): v for k, v in self._registry.items()}
            if key.lower() in lowered:
                loader = lowered[key.lower()]
            else:
                raise ValueError(f"Unsupported resource-type '{resource_type}'. Supported: {sorted(set(self._registry.keys()))}")
        return loader()

    def _instantiate_resource(self, cls: type, json_input: str, operation: Optional[str]) -> Any:
        # Resource classes may expect operation-aware validation
        if hasattr(cls, "from_json"): 
            return cls.from_json(json_input, operation=operation)
        # Fallback: direct init from dict if needed
        data = json.loads(json_input or "{}")
        return cls(**data)

    # -----------------
    # Operation routing
    # -----------------
    def _apt_resource_descriptor(self) -> Dict[str, Any]:
        """
        Build a manifest-like descriptor for the Apt resource so DSC can
        discover and use it without a separate apt.dsc.resource.json.
        """
        here = Path(__file__).resolve()
        repo_root = here.parents[2] if len(here.parents) >= 3 else here.parent
        apt_path = (repo_root / "resources" / "apt" / "AptPackage.py").resolve()
        
        
        # NEW: obtain schema from resource class
        resource_type = "Microsoft.Linux.Apt/Package"
        try:
            schema_from_class = self._get_instance_schema_from_type(resource_type)
        except Exception:
            # fallback to embedded if resource doesn't implement get_schema()
            schema_from_class = _APT_EMBEDDED_SCHEMA

        return {
                # These are the list-output fields DSC accepts
                "type": "Microsoft.Linux.Apt/Package",
                "kind": "resource",
                "version": "0.1.0",
                "capabilities": ["get", "set", "test", "export"],
                "path": str(apt_path),
                "directory": str(apt_path.parent),
                "implementedAs": "Python",
                "author": "",
                "properties": ["name", "version", "_exist", "dependencies"],
                "requireAdapter": "Microsoft.DSC.Adapters/Python",
                "description": "Manages APT packages on Linux",

                # IMPORTANT: this must look like a resource manifest and MUST include type+version
                "manifest": {
                    "$schema": "https://aka.ms/dsc/schemas/v3/resource/manifest.json",
                    "type": "Microsoft.Linux.Apt/Package",
                    "version": "0.1.0",
                    "description": "Manages APT packages on Linux"
                }
            }

    def list_resources(self) -> Dict[str, Any]:
        """Return a list of supported resources with descriptors."""
        return self._apt_resource_descriptor()
 
    
    def run_operation(self, operation: str, json_input: str, resource_type: str) -> Tuple[int, Dict[str, Any]]:
        """
        Returns (exit_code, result_dict). Prints nothing; caller decides printing.
        Single-resource mode only. Document payloads are not supported.
        """
        op = (operation or "").strip().lower()
        if op == "list":
            with self.profile_block("Adapter List"):
                descriptor = self.list_resources()
            return 0, descriptor

        if op == "validate":
            return 0, {"valid": True}

        # Reject document-shaped payloads explicitly
        as_obj = _parse_json(json_input)
        if _is_document_payload(as_obj):
            return 3, {"error": "Document payload is not supported by this adapter. Use single-resource input."}

        # Resolve resource type with fallbacks
        resolved_type = (resource_type or "").strip()
        if not resolved_type:
            env_type = os.getenv("DSC_RESOURCE_TYPE", "").strip()
            if env_type:
                resolved_type = env_type
        try:
            cls = self._resolve_resource_class(resolved_type)
        except Exception as e:
            self.log("error", str(e), "Adapter", operation=op)
            return 2, {"error": str(e)}

        # Validate for ops that take input JSON (get/set/test/validate/export)
        if op in ("get", "set", "test", "validate"):
             if not self.validate_input_json(json_input, operation=op):
                 return 3, {"error": "Invalid input JSON"}

        try:
            if op == "get":
                with self.profile_block("DSC Get Operation"):
                    instance = self._instantiate_resource(cls, json_input, operation="get")
                    data = instance.get()
            
                # derive the nested instance name from input JSON, else fall back to resource_type
                try:
                    nested_name = json.loads(json_input or "{}").get("name", "") or resource_type
                except Exception:
                    nested_name = resource_type or ""
                
                full = {
                        "metadata": {"Microsoft.DSC": {"operation": "Get"}}, 
                        "name": nested_name,  # adapter instance name; single mode often mirrors nested instance
                        "type": "Microsoft.DSC.Adapters/Python",
                        "result": [
                            {
                                "name": nested_name,
                                "type": resource_type,
                                "result": {
                                    "actualState": data
                                }
                            }
                        ]
                    }
                return (0, full)

            elif op == "set":
                with self.profile_block("DSC Set Operation"):
                    instance = self._instantiate_resource(cls, json_input, operation="set")
                    data = instance.set()
                # Ensure we have the expected keys
                state = {}
                diffs = []
                if isinstance(data, dict):
                    state = data.get("state", {})
                    diffs = data.get("differingProperties", [])
                else:
                    state = {}
                    diffs = []

                
                sys.stdout.write(json.dumps(state, ensure_ascii=False) + "\n")
                sys.stdout.write(json.dumps(diffs, ensure_ascii=False) + "\n")
                
                # Signal to caller that we've already printed the required stdout
                return (0, {"_stdout_emitted": True})

            elif op == "test":
                with self.profile_block("DSC Test Operation"):
                    instance = self._instantiate_resource(cls, json_input, operation="test")
                    actual_state, diffs = instance.test()
                    
                sys.stdout.write(json.dumps(actual_state if isinstance(actual_state, dict) else {}, ensure_ascii=False) + "\n")
                sys.stdout.write(json.dumps(diffs if isinstance(diffs, list) else [], ensure_ascii=False) + "\n")

                # Signal stdout already emitted so main() doesn't print a wrapper
                return (0, {"_stdout_emitted": True})

            elif op == "export":
                # If your resource supports filtered export with provided input, pass instance; else pass None for full export
                with self.profile_block("DSC Export Operation"):
                    # Determine if filters are provided; otherwise export all (None)
                    as_obj = _parse_json(json_input)
                    has_filters = any(k in as_obj for k in ("name", "version", "source", "dependencies"))
                    instance = self._instantiate_resource(cls, json_input, operation="export") if has_filters else None
                    data = cls.export(instance)
                    # If export returns None (prints only), still return an empty dict for adapter contract
                    return (0, data if isinstance(data, dict) else {})

            else:
                msg = f"Unsupported operation '{operation}'. Expected one of: list, get, set, test, export, validate"
                self.log("error", msg, "Adapter")
                return 2, {"error": msg}

        except SystemExit as se:
            # Resource may call sys.exit(1) on error paths (e.g., export). Normalize.
            code = int(getattr(se, "code", 1) or 1)
            self.log("error", f"Resource terminated with exit {code}", "Adapter", operation=op)
            return code, {"error": f"Resource terminated with exit {code}"}
        except Exception as err:
            self.log("error", f"Operation '{op}' failed: {err}", "Adapter", operation=op)
            return 1, {"error": str(err)}


# --------------------
# CLI / entrypoint API
# --------------------
def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dsctest",
        description="DSC v3 Python adapter CLI compatible with manifest."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    adapter = sub.add_parser("adapter", help="Adapter operations")
    adapter.add_argument("--operation", required=True, choices=["list", "get", "set", "test", "export", "validate"],
                         help="Adapter operation to execute.")
    adapter.add_argument("--input", default="{}", help="JSON string with resource configuration (single input).")
    # Accept multiple aliases to be robust to engine variations
    adapter.add_argument(
        "-ResourceType", "--ResourceType", "--resource", "--resource-type", "--resourceType",
        dest="ResourceType",
        default="",
        help="Resource type selector (e.g., Microsoft.Linux.Apt/Package)."
    )
    return parser

# Adapter instance importable by resources
resource_adapter: ResourceAdapter = ResourceAdapter()

def main(argv: Optional[list] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command != "adapter":
        print(json.dumps({"error": "Unsupported command"}))
        return 2

    # 1. Start with --input as the authoritative source
    input_str = args.input

    # 2. ONLY read stdin if:
    #    - --input was empty or "{}"
    #    - AND stdin has data available immediately (non‑blocking)
    if input_str in ("", "{}", None) and args.operation not in ("list",):
        try:
            import select
            r, _, _ = select.select([sys.stdin], [], [], 0)
            if r:
                stdin_data = sys.stdin.read().strip()
                if stdin_data:
                    input_str = stdin_data
        except Exception:
            pass

    # 3. Call operation handler
    exit_code, result = resource_adapter.run_operation(
        args.operation,
        input_str,
        args.ResourceType
    )
    
    # If set branch (or similar) already wrote to stdout, skip emitting a wrapper
    if isinstance(result, dict) and result.get("_stdout_emitted"):
        return exit_code

    # 4. Capture EXACT output passed to DSC
    out_json = json.dumps(result, ensure_ascii=False)
    try:
        with open("/tmp/dsc_python_adapter_last_stdout.json", "w", encoding="utf-8") as f:
            f.write(out_json)
    except Exception:
        pass

    print(out_json)
    return exit_code



if __name__ == "__main__":
    sys.exit(main())