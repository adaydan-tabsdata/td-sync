import sys
from pathlib import Path

from td_sync.sync_v2 import (
    _collect_folder_functions,
    _load_tabsdata_functions,
    _sync_file,
    _sync_folder,
    load_server,
)
from td_sync.trigger import _ui_url, monitor_execution_or_transaction


def _trigger(server, collection_name: str, fn_name: str):
    print(f"\nTriggering {collection_name}/{fn_name} ...")
    server.trigger_function(
        collection_name=collection_name,
        function_name=fn_name,
    )
    execution_list = server.list_executions()
    execution = [
        i
        for i in execution_list
        if i.status not in ["Committed", "Failed", "Canceled", "Stalled"]
    ][-1]
    print(f"Execution: {_ui_url(execution.id)}\n")
    monitor_execution_or_transaction(execution, server=server)


def main():
    if len(sys.argv) < 2:
        print("Usage: td-trigger <path>")
        print()
        print("  <path> can be:")
        print("    a .py file   — trigger all tabsdata functions in the file")
        print("    a folder     — trigger all tabsdata functions in the folder")
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
        fn_names = list(_load_tabsdata_functions(target).keys())
    else:
        collection_name = target.name
        _sync_folder(server, target, existing_collections)
        fn_names = list(_collect_folder_functions(target).keys())

    if not fn_names:
        print("No tabsdata functions found.")
        sys.exit(1)

    for fn_name in fn_names:
        _trigger(server, collection_name, fn_name)
