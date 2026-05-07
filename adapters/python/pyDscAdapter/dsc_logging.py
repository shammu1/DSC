import json
import logging
import sys
from contextlib import contextmanager
import contextvars

# ---------------------------------
# Logger implementation
# ---------------------------------

# Context visible to all logs emitted during an operation
cv_operation = contextvars.ContextVar("dsc_operation", default="")
cv_resource_type = contextvars.ContextVar("dsc_resource_type", default="")

# --------------------------------------------------------------
# DSC JSON Formatter - converts log records to DSC JSON format
# ---------------------------------------------------------------
class DSCJsonFormatter(logging.Formatter):
    """Formats log records as DSC-compliant JSON."""
    
    def format(self, record):
        payload = {
            "message": record.getMessage(),
            "target": record.name,
            "level": record.levelname.lower(),
        }
        
        # Add context if available
        if hasattr(record, "operation") and record.operation:
            payload["operation"] = record.operation
        if hasattr(record, "resourceType") and record.resourceType:
            payload["resourceType"] = record.resourceType
        # if hasattr(record, "method") and record.method:
        #     payload["method"] = record.method
        
        return json.dumps(payload, ensure_ascii=False)


# -------------------------------------------------------
# Context Filter - injects contextvars into log records
# -------------------------------------------------------
class DSCContextFilter(logging.Filter):
    """Injects DSC context variables into every log record."""
    
    def filter(self, record):
        # Inject context into record
        record.operation = cv_operation.get("")
        record.resourceType = cv_resource_type.get("")
        return True  # pass record through


# --------------------------------------------------------------------------------
# LoggingSetup - configure root logger with DSCJsonFormatter and DSCContextFilter
# --------------------------------------------------------------------------------
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


# -------------------------------------------------------
# Context managers
# -------------------------------------------------------
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
