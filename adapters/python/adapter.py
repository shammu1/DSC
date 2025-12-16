
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
from typing import Any, Dict, Optional, Tuple, Callable


class JsonFormatter(logging.Formatter):
    """Always outputs: {"<level>": "<message>"} (level is lowercase)."""
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps({record.levelname.lower(): record.getMessage()}, ensure_ascii=False)



# ---------------------------------
# Resource adapter implementation 
# ---------------------------------
TRACE_LEVEL_NUM = 5
class ResourceAdapter:
    """
    Provides:
        - profile_block for lightweight timing/profiling
        - log(level, message, target, **kwargs) for structured logging
        - validate_input_json(json_str, operation) for basic input checks
        - registry to resolve resource type -> class loader
    """

    _trace_registered: bool = False

    @classmethod
    def _ensure_trace_level(cls) -> None:
        if cls._trace_registered:
            return

        # Register custom TRACE level name if missing
        if not hasattr(logging, "TRACE"):
            logging.addLevelName(TRACE_LEVEL_NUM, "TRACE")

        # Add Logger.trace method if missing
        if not hasattr(logging.Logger, "trace"):
            def _trace(self, message, *args, **kwargs):
                if self.isEnabledFor(TRACE_LEVEL_NUM):
                    self._log(TRACE_LEVEL_NUM, message, args, **kwargs)
            logging.Logger.trace = _trace  # type: ignore[attr-defined]

        cls._trace_registered = True

    def __init__(self) -> None:
        # ensure TRACE level is available before creating/using logger
        self._ensure_trace_level()

        # Map resource-type tokens/aliases to loader functions returning class objects
        # Extend here for more resource types.
        self._registry: Dict[str, Callable[[], type]] = {
            # TODO: Will have to decide the type for the apt resource.
            "apt": self._load_apt_class,
            # "aptpackage": self._load_apt_class,
            # "AptPackage": self._load_apt_class,
            # "Test/AptPackage": self._load_apt_class,  # if you choose to use a namespaced token
        }
        self.TRACE_LEVEL = os.getenv("DSC_TRACE_LEVEL", "info").lower()
        self.ENABLE_PROFILING = True  # self.TRACE_LEVEL == "trace"

        self.logger = logging.getLogger("dsc_adapter")

        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(JsonFormatter())
            self.logger.addHandler(handler)

        if self.TRACE_LEVEL == "trace":
            self.logger.setLevel(TRACE_LEVEL_NUM)
        elif self.TRACE_LEVEL == "debug":
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)



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

        if lvl == "trace" and hasattr(self.logger, "trace"):
            self.logger.trace(core_msg)
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

        # Try local relative import based on current file system position
        _here = Path(__file__).resolve()
        _repo_root = _here.parents[3] if len(_here.parents) >= 4 else _here.parent
        _resources_root = _here.parents[2] if len(_here.parents) >= 3 else _here.parent
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
    def list_capabilities(self) -> Dict[str, Any]:
        """Return supported resource types and input characteristics."""
        supported = sorted(set(self._registry.keys()))
        return {
            "supportedResourceTypes": supported,
            "inputKind": "single",  # mirrors your manifest
            "operations": ["list", "get", "set", "test", "export", "validate"],
        }

    def run_operation(self, operation: str, json_input: str, resource_type: str) -> Tuple[int, Dict[str, Any]]:
        """
        Returns (exit_code, result_dict). Prints nothing; caller decides printing.
        """
        op = (operation or "").strip().lower()
        if op == "list":
            with self.profile_block("Adapter List"):
                result = self.list_capabilities()
            return 0, result

        # All ops below require a resource type
        try:
            cls = self._resolve_resource_class(resource_type)
        except Exception as e:
            self.log("error", str(e), "Adapter", operation=op)
            return 2, {"error": str(e)}

        # Validate for ops that take input JSON (get/set/test/validate/export when specific package filtering is used)
        if op in ("get", "set", "test", "validate", "export"):
            if not self.validate_input_json(json_input, operation=op):
                return 3, {"error": "Invalid input JSON"}

        try:
            if op == "get":
                with self.profile_block("DSC Get Operation"):
                    instance = self._instantiate_resource(cls, json_input, operation="get")
                    data = instance.get()
                return 0, data

            elif op == "set":
                with self.profile_block("DSC Set Operation"):
                    instance = self._instantiate_resource(cls, json_input, operation="set")
                    data = instance.set()
                # Normalize to stateAndDiff when resource provides diffs
                if isinstance(data, dict) and "differingProperties" in data:
                    state = {k: v for k, v in data.items() if k != "differingProperties"}
                    diffs = data.get("differingProperties", [])
                    return 0, {"state": state, "differingProperties": diffs}
                return 0, data if isinstance(data, dict) else {"result": data}

            elif op == "test":
                with self.profile_block("DSC Test Operation"):
                    instance = self._instantiate_resource(cls, json_input, operation="test")
                    actual_state, diffs = instance.test()
                    # Normalize to common adapter test shape
                    result = {
                        "actualState": actual_state,
                        "differingProperties": diffs,
                        "inDesiredState": len(diffs) == 0,
                    }
                return 0, result

            elif op == "export":
                # If your resource supports filtered export with provided input, pass instance; else pass None for full export
                with self.profile_block("DSC Export Operation"):
                    instance = self._instantiate_resource(cls, json_input, operation="export")
                    # Export prints to stdout in your resource; capture/normalize to a dict when possible
                    # Here we call and rely on resource's own printing behavior to remain compatible.
                    data = cls.export(instance)
                    # If export returns None (prints only), still return an empty dict for adapter contract
                    return 0, data if isinstance(data, dict) else {"status": "export invoked"}


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
    adapter.add_argument("--resource-type", default="", help="Resource type selector (e.g., apt, AptPackage).")
    return parser

# Adapter instance importable by resources (AptPackage.py)
resource_adapter: ResourceAdapter = ResourceAdapter()

def main(argv: Optional[list] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)


    if args.command != "adapter":
        print(json.dumps({"error": "Unsupported command"}))
        return 2

    #resource_adapter = ResourceAdapter()

    # Mirror manifest behavior: allow 'list' with resource-type 'none' or empty
    resource_type = args.resource_type if args.operation != "list" else (args.resource_type or "none")

    exit_code, result = resource_adapter.run_operation(args.operation, args.input, resource_type)
    # Always print a JSON result for consistency
    print(json.dumps(result))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())