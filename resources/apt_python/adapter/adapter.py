import json
import logging
import os
import time
import cProfile
import pstats
import io
from contextlib import contextmanager


TRACE_LEVEL_NUM = 5
if not hasattr(logging, "TRACE"):
    logging.addLevelName(TRACE_LEVEL_NUM, "TRACE")
    def _trace(self, message, *args, **kwargs):
        if self.isEnabledFor(TRACE_LEVEL_NUM):
            self._log(TRACE_LEVEL_NUM, message, args, **kwargs)
    logging.Logger.trace = _trace  # type: ignore


class JsonFormatter(logging.Formatter):
    """
    Always outputs: {"<level>": "<message>"} (level is lowercase).
    No timestamps, no other fields.
    """
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps({record.levelname.lower(): record.getMessage()}, ensure_ascii=False)


class ResourceAdapter:
    def __init__(self):
        # # Optional: Read trace level from environment variable
        self.TRACE_LEVEL = os.getenv("DSC_TRACE_LEVEL", "info").lower()
        self.ENABLE_PROFILING = True #self.TRACE_LEVEL == "trace"

        self.logger = logging.getLogger("dsc_adapter")

        # Configure only once to avoid duplicate handlers
        if not self.logger.handlers:
            handler = logging.StreamHandler()  # stderr by default
            handler.setFormatter(JsonFormatter())
            self.logger.addHandler(handler)

        # Set level
        if self.TRACE_LEVEL == "trace":
            self.logger.setLevel(TRACE_LEVEL_NUM)
        elif self.TRACE_LEVEL == "debug":
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)


    # Context manager for profiling
    @contextmanager
    def profile_block(self,label):
        if self.ENABLE_PROFILING:
            profiler = cProfile.Profile()
            profiler.enable()
            start_time = time.perf_counter()
            yield
            end_time = time.perf_counter()
            profiler.disable()

            s = io.StringIO()
            ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
            ps.print_stats(10)  # Top 10 functions

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