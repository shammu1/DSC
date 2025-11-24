import sys
import json
import logging
import os
import time
import cProfile
import pstats
import io
from contextlib import contextmanager
from typing import Optional, List, Any, Dict

try:
    # Pydantic v2
    from pydantic import BaseModel, ValidationError, model_validator
    _PYDANTIC_V2 = True
except ImportError:  # fallback to v1
    from pydantic import BaseModel, ValidationError, root_validator  # type: ignore
    _PYDANTIC_V2 = False

    
# ---------------- NEW: custom error constants ----------------
MISSING_NAME_CODE = "APT_INPUT_MISSING_NAME"
MISSING_NAME_MSG = (
    "APT input must include a non-empty 'name' property. "
    "Example: {\"name\": \"curl\", \"version\": \"7.88.1\"}"
)



TRACE_LEVEL_NUM = 5
if not hasattr(logging, "TRACE"):
    logging.addLevelName(TRACE_LEVEL_NUM, "TRACE")

    def _trace(self, message, *args, **kwargs):
        if self.isEnabledFor(TRACE_LEVEL_NUM):
            self._log(TRACE_LEVEL_NUM, message, args, **kwargs)

    logging.Logger.trace = _trace  # type: ignore


class AptPackageInput(BaseModel):
    name: Optional[str] = None
    version: Optional[str] = None
    _exist: Optional[bool] = True
    source: Optional[str] = None
    dependencies: Optional[List[str]] = []
    operation: Optional[str] = None

    if _PYDANTIC_V2:
        @model_validator(mode="before")
        def ensure_name_present(cls, values):  # type: ignore[override]
            if isinstance(values, dict):
                op = (values.get("operation") or "").lower()
                if op != "export":
                    name = values.get("name")
                    if not isinstance(name, str) or not name.strip():
                        raise ValueError("Json validation error: Name property is required for 'get' operation")
            return values
    else:
        @root_validator(pre=True)
        def ensure_name_present(cls, values):  # type: ignore[override]
            op = (values.get("operation") or "").lower()
            if op != "export":
                name = values.get("name")
                if not isinstance(name, str) or not name.strip():
                    raise ValueError("Json validation error: Name property is required for 'get' operation")
            return values

    # if _PYDANTIC_V2:
    #     @model_validator(mode="before")
    #     def ensure_name_present(cls, values):  # type: ignore[override]
    #         if isinstance(values, dict):
    #             name = values.get("name")
    #             if not isinstance(name, str) or not name.strip():
    #                 raise ValueError("Json validation error: Name property is required")
    #                 #resource_adapter.log("error", f"Json validation error: Name property is required", target="Apt Management", method="validate_input_json")
    #                 #sys.exit(1)
    #         return values
    # else:
    #     @root_validator(pre=True)
    #     def ensure_name_present(cls, values):  # type: ignore[override]
    #         name = values.get("name")
    #         if not isinstance(name, str) or not name.strip():
    #             raise ValueError("Json validation error: Name property is required")
    #             #resource_adapter.log("error", f"Json validation error: Name property is required", target="Apt Management", method="validate_input_json")
    #             #sys.exit(1)
    #         return values


class JsonFormatter(logging.Formatter):
    """Always outputs: {"<level>": "<message>"} (level is lowercase)."""
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps({record.levelname.lower(): record.getMessage()}, ensure_ascii=False)


class ResourceAdapter:
    def __init__(self):
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


    def validate_input_json(self, json_str: str, operation: Optional[str] = None) -> Dict[str, Any]:
        try:
            raw = json.loads(json_str)
        except Exception as ex:
            self.log("error", f"Invalid JSON payload: {ex}", target="Apt Management", method="validate_input_json")
            sys.exit(1)

        if not isinstance(raw, dict):
            self.log("error", "Top-level JSON must be an object", target="Apt Management", method="validate_input_json")
            sys.exit(1)

        if operation:
            raw["operation"] = operation  # inject command for conditional validation

        validated: Dict[str, Any] = {}
        try:
            model = AptPackageInput(**raw)
            validated = model.model_dump() if _PYDANTIC_V2 else model.dict()
        except ValidationError as vex:
            try:
                errs = vex.errors()
                msgs = [e.get('msg', str(vex)) for e in errs]
                message = "; ".join(msgs) if msgs else str(vex)
                self.log("error", message, target="Apt Management", method="validate_input_json")
                sys.exit(1)
            except Exception:
                message = str(vex)
                self.log("error", message, target="Apt Management", method="validate_input_json")
                sys.exit(1)

        deps = validated.get('dependencies') or []
        if not isinstance(deps, list):
            self.log("error", "'dependencies' must be a list", target="Apt Management", method="validate_input_json")
            sys.exit(1)
        validated['dependencies'] = [d for d in deps if isinstance(d, str) and d.strip()]
        return validated


    # # --- Input validation / parsing helpers ---
    # def validate_input_json(self, json_str: str) -> Dict[str, Any]:
    #     """Validate incoming JSON for an AptPackage using pydantic.
    #     Returns validated dict or raises ValueError (after logging).
    #     """
    #     try:
    #         raw = json.loads(json_str)
    #     except Exception as ex:
    #         self.log("error", f"Invalid JSON payload: {ex}", target="Apt Management", method="validate_input_json")
    #         sys.exit(1)
    #         #raise ValueError(f"Invalid JSON: {ex}")

    #     if not isinstance(raw, dict):
    #         self.log("error", "Top-level JSON must be an object", target="Apt Management", method="validate_input_json")
    #         sys.exit(1)
    #         #raise ValueError("Top-level JSON must be an object")

    #     validated: Dict[str, Any] = {}

    #     try:
    #         model = AptPackageInput(**raw)
    #         validated = model.model_dump() if _PYDANTIC_V2 else model.dict()
            
    #     except ValidationError as vex:
    #         # Collect messages; custom name message will appear if triggered
    #         try:
    #             errs = vex.errors()
    #             msgs = [e.get('msg', str(vex)) for e in errs]
    #             message = "; ".join(msgs) if msgs else str(vex)
    #             self.log("error", message, target="Apt Management", method="validate_input_json")
    #             sys.exit(1)
    #         except Exception:
    #             message = str(vex)
    #             self.log("error", message, target="Apt Management", method="validate_input_json")
    #             sys.exit(1)
    #             #raise ValueError(message)
    #     # except Exception as ex:
    #     #     self.log("error", f"Unexpected validation error: {ex}", target="Apt Management", method="validate_input_json")
    #     #     raise ValueError(f"Unexpected validation error: {ex}")
    #     deps = validated.get('dependencies') or []
    #     if not isinstance(deps, list):
    #         self.log("error", "'dependencies' must be a list", target="Apt Management", method="validate_input_json")
    #         sys.exit(1)
    #         #raise ValueError("'dependencies' must be a list")
    #     validated['dependencies'] = [d for d in deps if isinstance(d, str) and d.strip()]       
    #     return validated

    def get_input_schema(self) -> Dict[str, Any]:
        try:
            if _PYDANTIC_V2:
                return AptPackageInput.model_json_schema()
            else:
                # v1 schema
                return AptPackageInput.schema()  # type: ignore[attr-defined]
        except Exception:
            return {}

    def safe_parse(self, json_str: str, operation: Optional[str] = None) -> Dict[str, Any]:
        try:
            return self.validate_input_json(json_str, operation=operation)
        except ValueError:
            return {}

    # def safe_parse(self, json_str: str) -> Dict[str, Any]:
    #     try:
    #         return self.validate_input_json(json_str)
    #     except ValueError:
    #         return {}

    @contextmanager
    def profile_block(self, label):
        if self.ENABLE_PROFILING:
            profiler = cProfile.Profile()
            profiler.enable()
            start_time = time.perf_counter()
            yield
            end_time = time.perf_counter()
            profiler.disable()

            s = io.StringIO()
            ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
            ps.print_stats(10)

            self.logger.info(f"[PROFILE] {label} took {end_time - start_time:.4f}s")
            self.logger.debug(f"[PROFILE DETAILS] {label}:\n{s.getvalue()}")
        else:
            yield

    def log(self, level: str, message: str, target: str = None, **kwargs):
        lvl = level.lower()
        method = kwargs.get("method", "?")
        core_msg = f"{target} - {method} - {message}" if target else f"{method} - {message}"

        if lvl == "trace" and hasattr(self.logger, "trace"):
            self.logger.trace(core_msg)
            return

        log_fn = getattr(self.logger, lvl, self.logger.info)
        log_fn(core_msg)


resource_adapter = ResourceAdapter()