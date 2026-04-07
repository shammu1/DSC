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
import contextvars
from datetime import datetime, timezone
import inspect
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Callable
import importlib.util

class JsonFormatter(logging.Formatter):
    """Always outputs: {"<level>": "<message>"} (level is lowercase)."""
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps({record.levelname.lower(): record.getMessage()}, ensure_ascii=False)

# ---------------------------------
# Logger implementation
# ---------------------------------

# Context visible to all logs emitted during an operation
cv_operation = contextvars.ContextVar("dsc_operation", default="")
cv_resource_type = contextvars.ContextVar("dsc_resource_type", default="")

# ========================
# DSC JSON Formatter - converts log records to DSC JSON format
# ========================
class DSCJsonFormatter(logging.Formatter):
    """Formats log records as DSC-compliant JSON."""
    
    def format(self, record):
        payload = {
            "message": record.getMessage(),
            "target": record.name,
        }
        
        # Add context if available
        if hasattr(record, "operation") and record.operation:
            payload["operation"] = record.operation
        if hasattr(record, "resourceType") and record.resourceType:
            payload["resourceType"] = record.resourceType
        # if hasattr(record, "method") and record.method:
        #     payload["method"] = record.method
        
        return json.dumps(payload, ensure_ascii=False)


# ========================
# Context Filter - injects contextvars into log records
# ========================
class DSCContextFilter(logging.Filter):
    """Injects DSC context variables into every log record."""
    
    def filter(self, record):
        # Inject context into record
        record.operation = cv_operation.get("")
        record.resourceType = cv_resource_type.get("")
        return True  # pass record through


# ========================
# Simple setup function
# ========================
def setup_dsc_logging(level="info"):
    """
    Configure DSC logging in one call.
    
    Args:
        level: DSC trace level (trace/debug/info/warning/error/critical)
    
    Returns:
        Logger instance ready to use
    """
    # Map DSC levels to Python levels
    level_map = {
        "trace": logging.DEBUG,
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }
    
    # Get root logger
    root = logging.getLogger()
    root.setLevel(level_map.get(level.lower(), logging.INFO))
    root.handlers.clear()
    
    # Add handler with JSON formatter and context filter
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(DSCJsonFormatter())
    handler.addFilter(DSCContextFilter())
    root.addHandler(handler)
    
    return logging.getLogger("dsc_adapter")


# ========================
# Simple context managers
# ========================
@contextmanager
def operation_context(operation, resource_type=""):
    """Set operation and resource type for all logs in scope."""
    tokens = [cv_operation.set(operation)]
    if resource_type:
        tokens.append(cv_resource_type.set(resource_type))
    
    try:
        yield
    finally:
        for token in reversed(tokens):
            token.var.reset(token)

# ---------------------------------
# Resource adapter implementation 
# ---------------------------------

def _parse_json(s: str) -> Dict[str, Any]:
    try:
        return json.loads(s or "{}")
    except Exception:
        return {}

def _get_class_map_from_pyproject(pyproject_path: Path) -> Dict[str, str]:
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
            if stripped.startswith("["):  # New section started
                break
            if "=" in stripped and not stripped.startswith("#"):
                key, val = stripped.split("=", 1)
                key = key.strip().strip('"\'')
                val = val.strip().strip('"\'')
                class_map[key] = val
    
    return class_map

def _import_class_from_file(module_path: Path, class_name: str, module_name: str) -> type:
    spec = importlib.util.spec_from_file_location(module_name, str(module_path))
    if not spec or not spec.loader:
        raise ImportError(f"Unable to load module '{module_path}'")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    try:
        return getattr(mod, class_name)
    except AttributeError as e:
        raise ImportError(f"Class '{class_name}' not found in '{module_path}': {e}")


# def _import_class_from_resource_path(resource_path: str, operation: str) -> type:
#     """
#     Load the resource module from a file pointed by --resource-path and
#     discover the appropriate class dynamically (no hardcoded class names).

#     Selection rules (in order):
#       1) If module defines RESOURCE_CLASS and it is a class, use it.
#       2) If module defines get_resource_class(operation) -> class, use it.
#       3) Otherwise, find classes defined in the module that implement the
#          requested operation method (get/set/test/export). If exactly one, use it.
#          If none or many, raise a helpful error.
#     """
#     op = (operation or "").strip().lower()
#     if op not in ("get", "set", "test", "export"):
#         raise ValueError(f"Unsupported operation '{operation}' for resource-path import")

#     p = Path(resource_path).resolve()
#     if not p.exists():
#         raise FileNotFoundError(f"resource-path not found: {p}")
#     if p.suffix.lower() != ".py":
#         raise ValueError(f"resource-path must point to a .py file: {p}")

#     # Load module from file
#     module_name = f"dsc_resourcepath_{p.stem}_{op}"
#     spec = importlib.util.spec_from_file_location(module_name, str(p))
#     if not spec or not spec.loader:
#         raise ImportError(f"Unable to load module '{p}'")

#     mod = importlib.util.module_from_spec(spec)
#     spec.loader.exec_module(mod)  # type: ignore[attr-defined]

#     # 1) Explicit module export: RESOURCE_CLASS
#     explicit = getattr(mod, "RESOURCE_CLASS", None)
#     if inspect.isclass(explicit):
#         return explicit

#     # 2) Optional hook: get_resource_class(operation)
#     hook = getattr(mod, "get_resource_class", None)
#     if callable(hook):
#         candidate = hook(op)
#         if inspect.isclass(candidate):
#             return candidate
#         raise ImportError(
#             f"get_resource_class('{op}') did not return a class in '{p}'"
#         )

#     # 3) Introspect: find classes defined in this module implementing the op method
#     candidates = []
#     for name, cls in inspect.getmembers(mod, inspect.isclass):
#         # Only consider classes defined in THIS module (avoid imported classes)
#         if getattr(cls, "__module__", None) != mod.__name__:
#             continue

#         meth = getattr(cls, op, None)
#         if callable(meth):
#             candidates.append(cls)

#     if len(candidates) == 1:
#         return candidates[0]

#     if len(candidates) == 0:
#         # Helpful: show what classes *are* available
#         defined = [
#             name for name, cls in inspect.getmembers(mod, inspect.isclass)
#             if getattr(cls, "__module__", None) == mod.__name__
#         ]
#         raise ImportError(
#             f"No class in '{p}' implements operation '{op}'. "
#             f"Classes defined in module: {defined}. "
#             f"Fix by adding a class with a '{op}()' method, or define RESOURCE_CLASS."
#         )

#     # Multiple matches: force explicit selection
#     raise ImportError(
#         f"Multiple classes in '{p}' implement operation '{op}': "
#         f"{[c.__name__ for c in candidates]}. "
#         f"Disambiguate by defining RESOURCE_CLASS = <YourClass> "
#         f"or implementing get_resource_class(operation)."
#     )


class ResourceAdapter:
    """
    Provides:
        - profile_block for lightweight timing/profiling
        - log(level, message, target, **kwargs) for structured logging
        - registry to resolve resource type -> class loader
    """

    def __init__(self) -> None:
        # Map resource-type tokens/aliases to loader functions returning class objects
        # Extend here for more resource types.
        
        here = Path(__file__).resolve()

        # Find repo root by walking up until we see "adapters/python"
        repo_root = None
        for p in [here.parent] + list(here.parents):
            if (p / "adapters" / "python").exists():
                repo_root = p
                break
        self._repo_root = repo_root or here.parents[2] 

        self._registry: Dict[str, Callable[[], type]] = {
            #"Microsoft.Linux.Apt/Package": self._load_apt_class,            
        }
        
        #TODO : Check whether to change hardcoding of manifest file path or not.
        # Consolidated manifest path
        manifest_path = (self._repo_root / "adapters" / "python" / "tests" / "pythontest.dsc.manifests.json").resolve()     
        if not manifest_path.exists():
            self.logger.warning(f"Consolidated manifest not found: {manifest_path}")
        else:
            self._load_consolidated_manifest(manifest_path)


        # Normalize DSC trace level to standard Python logging levels
        # Supported inputs: trace, debug, info, warning, error, critical
        dsc_level = (os.getenv("DSC_TRACE_LEVEL", "info") or "info").strip().lower()
        
        self.logger = setup_dsc_logging(dsc_level)
        self.logger.info("Adapter initialization complete")

        # Enable Profiling based on DSC trace level
        self.ENABLE_PROFILING = dsc_level in ("trace", "debug")
        

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

    # TODO: Log interception
    def log(self, level: str, message: str, target: str = None, **kwargs) -> None:
        lvl = level.lower()
        method = kwargs.get("method", "?")
        core_msg = f"{target} - {method} - {message}" if target else f"{method} - {message}"

        if lvl == "trace": # and hasattr(self.logger, "trace"):
            self.logger.debug(f"[TRACE] {core_msg}")
            return

        log_fn = getattr(self.logger, lvl, self.logger.info)
        log_fn(core_msg)

    
    # --------------------------
    # Resource loader and lookup
    # --------------------------


    # def _load_apt_class(self) -> type:
    #     """
    #     Attempts multiple import paths to locate AptPackage class.
    #     Aligns with your resource-side fallbacks.
    #     """
    #     # TODO: This needs to be changed to identify ther resource class from the Python module.
    #     # Try relative to typical repo layout
    #     try:
    #         from resources.apt.AptPackage import AptPackage  # type: ignore
    #         return AptPackage
    #     except Exception:
    #         pass

    #     # Try package-level absolute import
    #     try:
    #         from apt.AptPackage import AptPackage  # type: ignore
    #         return AptPackage
    #     except Exception:
    #         pass

    #     _here = Path(__file__).resolve()
    #     _repo_root = _here.parents[2] if len(_here.parents) >= 3 else _here.parent
    #     _resources_root = _repo_root / "resources"
    #     for p in (_repo_root, _resources_root):
    #         p_str = str(p)
    #         if p_str not in sys.path:
    #             sys.path.insert(0, p_str)
    #     try:
    #         from resources.apt.AptPackage import AptPackage  # type: ignore
    #         return AptPackage
    #     except Exception:
    #         pass

    #     # Final failure: raise with helpful guidance
    #     raise ImportError(
    #         "Unable to import AptPackage. Ensure the module path "
    #         "'resources.apt.AptPackage' exists or adjust the adapter loader."
    #     )


    def _load_consolidated_manifest(self, manifest_path: Path) -> None:
        """
        Load a manifest JSON mapping adapted resources to loaders and
        register loaders in _registry.

        Supports two formats:
        1) Object map: { "<type>": { "module": "<relative path>", "class": "<ClassName>" }, ... }
        2) Array under 'adaptedResources': [
               { "type": "<type>", "capabilities": ["get" | "set" | "test" | "export"], "path": "<relative .py path>" },
               ...
           ]
           For this format, class names are inferred by capability:
             get -> GetOnlyResource, set -> SetOnlyResource, test -> TestOnlyResource, export -> ExportOnlyResource
        """
        if not manifest_path.exists():
            return
        try:
            data = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception as e:
            self.log("error", f"Failed reading manifest: {e}", "Adapter", method="init")
            return

        # Load class names from pyproject.toml in same directory
        pyproject_path = manifest_path.parent / "pyproject.toml"
        class_map = _get_class_map_from_pyproject(pyproject_path)

        def registry_loader(rtype: str, resource_file_path: str, class_name: str) -> None:
            if not rtype or not resource_file_path or not class_name:
                return
            module_path = (self._repo_root / Path(resource_file_path)).resolve()
            module_name = f"dsc_manifest_{rtype.replace('/', '_').replace('.', '_').lower()}"

            def _make_loader(p: Path = module_path, clsname: str = class_name, mname: str = module_name) -> Callable[[], type]:
                return lambda: _import_class_from_file(p, clsname, mname)

            # Register exact key and a lower-case alias for case-insensitive lookup
            self._registry[rtype] = _make_loader()
            self._registry[rtype.lower()] = _make_loader()

        # # Format 1: direct map
        # if isinstance(data, dict) and all(isinstance(v, dict) for v in data.values()) and "adaptedResources" not in data:
        #     for rtype, entry in data.items():
        #         resource_file_path = str(entry.get("module", "")).strip()
        #         class_name = str(entry.get("class", "")).strip()
        #         registry_loader(rtype, resource_file_path, class_name)
        #     return

        # Format 2: adaptedResources array
        if isinstance(data, dict) and isinstance(data.get("adaptedResources"), list):
            for entry in data.get("adaptedResources", []):
                if not isinstance(entry, dict):
                    continue
                rtype = str(entry.get("type", "")).strip()
                resource_file_path = str(entry.get("path", "")).strip()

                # Class name from pyproject.toml
                class_name = class_map.get(rtype, "")

                # caps = entry.get("capabilities", [])
                # cap = (caps[0] if isinstance(caps, list) and caps else "").strip().lower()

                # # Infer class name from capability for test resources
                # class_by_cap = {
                #     "get": "GetOnlyResource",
                #     "set": "SetOnlyResource",
                #     "test": "TestOnlyResource",
                #     "export": "ExportOnlyResource",
                # }
                # class_name = class_by_cap.get(cap, "")
                registry_loader(rtype, resource_file_path, class_name)
            return

        # Unknown format; log and skip
        self.log("warning", "Manifest format not recognized; no adapted resources registered.", "Adapter", method="init")


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
    
    def run_operation(self, operation: str, json_input: str, resource_type: str, resource_path: str = "") -> Tuple[int, Dict[str, Any]]:
        """
        Returns (exit_code, result_dict). Prints nothing; caller decides printing.
        Single-resource mode only. Document payloads are not supported.
        """
        op = (operation or "").strip().lower()
        
        # Set per-operation context visible to all resource logs
        tok_op = cv_operation.set(op)
        tok_rt = cv_resource_type.set((resource_type or "").strip())

        with operation_context(op, resource_type):
            if op == "list":
                # with self.profile_block("DSC List Operation"),method_scope("list"):
                #     descriptor = self.list_resources()
                return 0, {"resources": []} #{"_stdout_emitted": True}
            #Check TODO whether to implement validate 
            if op == "validate":
                return 0, {"valid": True}

            # Resolve resource class
            try:
                if resource_path:
                    cls = _import_class_from_resource_path(resource_path, op)
                else:
                    resolved_type = (resource_type or "").strip() or os.getenv("DSC_RESOURCE_TYPE", "").strip()
                    cls = self._resolve_resource_class(resolved_type)
            except Exception as e:
                self.log("error", str(e), "Adapter", operation=op)
                return 2, {"error": str(e)}

            try:
                if op == "get":
                    with self.profile_block("DSC Get Operation"):
                        instance = self._instantiate_resource(cls, json_input, operation="get")
                        data = instance.get()
                    
                    # check if nested names are needed
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
                        state, diffs = instance.set()

                    #check if this two types of printing is OK
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
            
            finally:
                # Always reset
                cv_operation.reset(tok_op)
                cv_resource_type.reset(tok_rt)


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
    adapter.add_argument("--resource", dest="ResourceType", default="", help="Resource type selector (e.g., Microsoft.Linux.Apt/Package).")
    adapter.add_argument("--resource-path", dest="ResourcePath", default="", help="Optional resource module file path.")
    return parser

#TODO: Validate if the adapter should be importable.
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
        args.ResourceType,
        getattr(args, "ResourcePath", "")
    )
    
    # If set branch (or similar) already wrote to stdout, skip emitting a wrapper
    if isinstance(result, dict) and result.get("_stdout_emitted"):
        return exit_code

    # 4. Capture EXACT output passed to DSC
    out_json = json.dumps(result, ensure_ascii=False)

    print(out_json)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())