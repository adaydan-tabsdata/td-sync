import importlib.util
import json
import sys
from pathlib import Path

from tabsdata._io.inputs.table_inputs import TableInput
from tabsdata._io.outputs.table_outputs import TableOutput
from tabsdata.api.apiserver import APIServer
from tabsdata.api.tabsdata_server import TabsdataServer

_CONNECTION_JSON = Path.home() / ".tabsdata" / "connection.json"


def _find_connection_json() -> Path:
    if _CONNECTION_JSON.exists():
        return _CONNECTION_JSON
    raise FileNotFoundError(
        f"No connection.json found at {_CONNECTION_JSON}\n"
        "Run 'td login' to authenticate first."
    )


def load_server() -> TabsdataServer:
    """
    Build a TabsdataServer from stored credentials, bypassing the password-based
    __init__. Uses TabsdataServer.__new__ + direct attribute injection, the same
    technique used internally by the tabsdata CLI.
    """
    path = _find_connection_json()
    with open(path) as f:
        creds = json.load(f)

    connection = APIServer(creds["url"])
    connection.bearer_token = creds.get("bearer_token")
    connection.refresh_token = creds.get("refresh_token")
    connection.token_type = creds.get("token_type")
    connection.expires_in = creds.get("expires_in")
    connection.expiration_time = creds.get("expiration_time")

    server = TabsdataServer.__new__(TabsdataServer)
    server.connection = connection
    return server


# ---------------------------------------------------------------------------
# Function scanning
# ---------------------------------------------------------------------------

def _load_tabsdata_functions(py_file: Path) -> dict:
    """
    Import py_file and return {fn_name: fn_obj} for all tabsdata-decorated
    functions (i.e. objects whose __module__ is 'tabsdata._tdfunction').
    """
    spec = importlib.util.spec_from_file_location("_td_scan", py_file)
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except Exception as e:
        print(f"  [warn] Could not import {py_file.name}: {e}")
        return {}

    td_module = "tabsdata._tdfunction"
    return {
        name: obj
        for name, obj in vars(mod).items()
        if callable(obj) and getattr(obj, "__module__", None) == td_module
    }


def _collect_folder_functions(folder: Path) -> dict:
    """
    Scan all .py files in folder and return a flat map:
        {fn_name: (py_file, fn_obj)}
    """
    result = {}
    for py_file in sorted(folder.glob("*.py")):
        for fn_name, fn_obj in _load_tabsdata_functions(py_file).items():
            result[fn_name] = (py_file, fn_obj)
    return result


# ---------------------------------------------------------------------------
# Dependency ordering
# ---------------------------------------------------------------------------

def _as_list(value) -> list:
    if value is None:
        return []
    return [value] if isinstance(value, str) else list(value)


def _input_tables(fn_obj) -> list:
    """Tables this function reads (only TableInput counts as a dependency)."""
    inp = fn_obj.input
    return _as_list(inp.table) if isinstance(inp, TableInput) else []


def _output_tables(fn_obj) -> list:
    """Tables this function writes (only TableOutput counts as a produced table)."""
    out = fn_obj.output
    return _as_list(out.table) if isinstance(out, TableOutput) else []


def _topological_sort(fn_map: dict) -> list:
    """
    Given {fn_name: (py_file, fn_obj)}, return an ordered list of
    (fn_name, py_file, fn_obj) such that producers are registered before
    consumers.

    Uses Kahn's BFS algorithm. Falls back to original order if a cycle is
    detected (and prints a warning).
    """
    # Map each output table to the function that produces it
    produces: dict[str, str] = {}
    for fn_name, (_, fn_obj) in fn_map.items():
        for table in _output_tables(fn_obj):
            produces[table] = fn_name

    # predecessors[fn_name] = set of fn_names that must be registered before it
    predecessors: dict[str, set] = {name: set() for name in fn_map}
    for fn_name, (_, fn_obj) in fn_map.items():
        for table in _input_tables(fn_obj):
            producer = produces.get(table)
            if producer and producer != fn_name:
                predecessors[fn_name].add(producer)

    # Kahn's BFS
    remaining = {name: set(preds) for name, preds in predecessors.items()}
    ready = [n for n, preds in remaining.items() if not preds]
    order = []

    while ready:
        node = ready.pop(0)
        order.append(node)
        for other, preds in remaining.items():
            if node in preds:
                preds.discard(node)
                if not preds and other not in order:
                    ready.append(other)

    if len(order) != len(fn_map):
        print(
            "  [warn] Circular dependency detected — "
            "registering in original order"
        )
        return [(name, *fn_map[name]) for name in fn_map]

    return [(name, *fn_map[name]) for name in order]


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def _ensure_collection(
    server: TabsdataServer, collection_name: str, existing: set
):
    if collection_name not in existing:
        server.create_collection(collection_name)
        existing.add(collection_name)
        print(f"  Created collection '{collection_name}'")


def _register_function(
    server: TabsdataServer,
    collection_name: str,
    fn_name: str,
    py_file: Path,
    registered: set,
):
    function_path = f"{py_file.resolve()}::{fn_name}"
    try:
        if fn_name in registered:
            server.update_function(
                collection_name,
                fn_name,
                function_path,
                description="updated via td-sync",
            )
            print(f"  Updated   {collection_name}/{fn_name}")
        else:
            server.register_function(
                collection_name=collection_name,
                function_path=function_path,
            )
            print(f"  Registered {collection_name}/{fn_name}")
    except Exception as e:
        print(f"  [failed]  {collection_name}/{fn_name}: {e}")


def _sync_file(
    server: TabsdataServer,
    py_file: Path,
    collection_name: str,
    existing_collections: set,
):
    """Register/update all tabsdata functions in a single file."""
    fn_map = _load_tabsdata_functions(py_file)
    if not fn_map:
        print(f"  [skip] {py_file.name} — no tabsdata functions found")
        return

    _ensure_collection(server, collection_name, existing_collections)
    registered = {f.name for f in server.list_functions(collection_name)}

    # Wrap into the same shape _topological_sort expects
    wrapped = {name: (py_file, obj) for name, obj in fn_map.items()}
    for fn_name, file, _ in _topological_sort(wrapped):
        _register_function(server, collection_name, fn_name, file, registered)


def _sync_folder(
    server: TabsdataServer,
    folder: Path,
    existing_collections: set,
):
    """
    Register/update all tabsdata functions across all .py files in folder
    into a collection named after the folder, in dependency order.
    """
    collection_name = folder.name
    print(f"\nSyncing folder '{folder.name}' → collection '{collection_name}'")

    fn_map = _collect_folder_functions(folder)
    if not fn_map:
        print("  [skip] No tabsdata functions found")
        return

    _ensure_collection(server, collection_name, existing_collections)
    registered = {f.name for f in server.list_functions(collection_name)}

    ordered = _topological_sort(fn_map)

    for fn_name, py_file, _ in ordered:
        _register_function(server, collection_name, fn_name, py_file, registered)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print("Usage: td-sync <path>")
        print()
        print("  <path> can be:")
        print("    a .py file        — register its tabsdata functions into a")
        print("                        collection named after its parent folder")
        print("    a folder          — register all tabsdata functions in .py")
        print("                        files into a collection of the same name")
        print("    a 'tabsdata-root' folder")
        print("                      — apply folder-sync to each subdirectory")
        sys.exit(1)

    target = Path(sys.argv[1]).resolve()

    if not target.exists():
        print(f"Error: path does not exist: {target}")
        sys.exit(1)

    server = load_server()
    existing_collections = {c.name for c in server.list_collections()}

    if target.is_file():
        if target.suffix != ".py":
            print(f"Error: '{target.name}' is not a .py file")
            sys.exit(1)
        collection_name = target.parent.name
        print(f"\nSyncing file '{target.name}' → collection '{collection_name}'")
        _sync_file(server, target, collection_name, existing_collections)

    elif target.is_dir():
        if target.name == "tabsdata-root":
            subfolders = sorted(d for d in target.iterdir() if d.is_dir())
            if not subfolders:
                print(f"No subdirectories found in: {target}")
                sys.exit(0)
            for folder in subfolders:
                _sync_folder(server, folder, existing_collections)
        else:
            _sync_folder(server, target, existing_collections)

    print("\nDone.")
