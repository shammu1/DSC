import sys
import json
import select
#from Python.Package.Apt.AptPackage import AptPackage

from pathlib import Path

# Robust import handling so script works whether invoked as module or as a file
try:
    # When executed with: python -m resources.Python.Package.main
    from AptPackage import AptPackage  # type: ignore
except Exception:
    # Fallbacks when run as a plain script (no package context)
    pkg_root = Path(__file__).resolve().parents[2]  # points to .../resources
    project_root = pkg_root.parent                  # repo root
    for p in (project_root, pkg_root):

        p_str = str(p)
        if p_str not in sys.path:
            sys.path.insert(0, p_str)
    try:
        from resources.apt.AptPackage import AptPackage  # absolute import
    except Exception:
        # Last resort: relative folder on sys.path (if cwd is Package)
        from AptPackage import AptPackage  # type: ignore
# #from logger import dfl_logger as Logger


def _read_all_stdin():
    """Blocking read of all stdin (used for non-export operations)."""
    return sys.stdin.read()

def _read_stdin_nonblocking():
    """Return stdin contents if data is available without blocking; else None."""
    try:
        if sys.stdin.closed:
            return None
        if sys.stdin.isatty():
            return None
        rlist, _, _ = select.select([sys.stdin], [], [], 0.01)
        if rlist:
            data = sys.stdin.read()
            return data if data.strip() else None
    except Exception:
        return None
    return None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("no operation specified (expected: get, set, delete, test, export)")
        sys.exit(1)
    operation = sys.argv[1].lower()

    # Export can be invoked with or without stdin. Handle that first.
    if operation == "export":

        input_data = _read_stdin_nonblocking()
        if input_data:
            try:
                pkg = AptPackage.from_json(input_data,operation=operation)
                AptPackage.export(pkg)
            except Exception as ex:
                #Logger.error(f"Failed to parse input for export: {ex}", "Apt Management", command="export", method="main")
                # Fallback to exporting all packages
                AptPackage.export()
        else:
            AptPackage.export()
        sys.exit(0)

    # For all other operations we require stdin describing the package
    input_data = _read_all_stdin()
    if not input_data.strip():
        #Logger.error("No input data provided for operation", "Apt Management", command=operation, method="main")
        print(json.dumps({"error": "No input data provided"}))
        sys.exit(1)

    pkg = AptPackage.from_json(input_data,operation=operation)

    if operation == "get":
        # get() already returns a dict; serialize
        print(json.dumps(pkg.get()))
    elif operation == "set":
        print(json.dumps(pkg.set()))
    elif operation == "delete":
        result = pkg.delete()
        # delete() currently returns None; wrap for consistency
        print(json.dumps({"result": result}))
    elif operation == "test":
        actual_state, diffs = pkg.test()
        # test (stateAndDiff) must print two JSON lines
        print(json.dumps(actual_state))
        print(json.dumps(diffs))
    else:
        print(json.dumps({"error": f"unknown operation: {operation}"}))
        sys.exit(1)
