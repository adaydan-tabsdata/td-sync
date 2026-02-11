import re
from time import sleep

import rich_click as click
from rich.console import Console
from rich.live import Live
from rich.table import Table
from tabsdata.api.tabsdata_server import TabsdataServer

from td_sync.cancel_flows import main as cancel_trx


def beautify_time(time: str) -> str:
    if time == "None":
        return "-"
    return time


def monitor_execution_or_transaction(transaction):
    server = TabsdataServer("127.0.0.1:2457", "admin", "tabsdata", "sys_admin")
    transaction = transaction
    EXECUTION_FAILED_FINAL_STATUSES = ["Stalled", "Unexpected"]
    EXECUTION_SUCCESSFUL_FINAL_STATUSES = ["Finished", "Committed"]
    EXECUTION_FINAL_STATUSES = (
        EXECUTION_FAILED_FINAL_STATUSES + EXECUTION_SUCCESSFUL_FINAL_STATUSES
    )

    list_of_runs = transaction.function_runs
    print(list_of_runs)

    def build_table():
        table = Table(title=f"Function Runs: {len(list_of_runs)}")
        table.add_column("Function Run ID", style="cyan", no_wrap=True)
        table.add_column("Collection")
        table.add_column("Function")
        (
            table.add_column("Transaction ID", no_wrap=True)
            if transaction
            else table.add_column("Plan ID", no_wrap=True)
        )
        table.add_column("Started on", no_wrap=True)
        table.add_column("Ended on", no_wrap=True)
        table.add_column("Status")
        for function_run in list_of_runs:
            table.add_row(
                function_run.id,
                function_run.collection.name,
                function_run.function.name,
                (
                    function_run.transaction.id
                    if transaction
                    else function_run.execution.id
                ),
                beautify_time(function_run.started_on_str),
                beautify_time(function_run.ended_on_str),
                function_run.status,
            )
        return table

    click.echo("Waiting for the transaction to finish...")

    refresh_rate = 1  # seconds
    with Live(
        build_table(), refresh_per_second=refresh_rate, console=Console()
    ) as live:
        while True:
            # Note: while it would initially make more sense to write this as
            # 'if transaction.status in FAILED_FINAL_STATUSES', this approach avoids
            # the risk of ignoring failed transactions that are not in a recognized
            # status due to a mismatch between the transaction status and the
            # FINAL_STATUSES set (which ideally should not happen).
            if transaction.status in EXECUTION_FINAL_STATUSES:
                break
            list_of_runs = transaction.function_runs
            live.update(build_table())
            transaction.refresh()
            sleep(1 / refresh_rate)
        list_of_runs = transaction.function_runs
        live.update(build_table())
        transaction.refresh()

    click.echo("Execution finished.")

    failed_runs = [
        fn_run
        for fn_run in list_of_runs
        if fn_run.status not in EXECUTION_FINAL_STATUSES
    ]
    if failed_runs:
        initial_failed_run = [
            fn_run for fn_run in list_of_runs if fn_run.status == "Failed"
        ][0]
        click.echo("Some function runs failed:")
        for fn_run in failed_runs:
            click.echo(f"- {fn_run.id}")
        complete_command = f"'td exe logs --trx {transaction.id}'"
        cancel_trx()
        worker_id = server.list_workers(f"function_run_id:eq:{initial_failed_run.id}")[
            0
        ]
        log = server.get_worker_log(worker_id)
        match = re.search(r"\[Exiting function execution\](.*?)={10,}", log, re.S)
        if match:
            result = match.group(1)  # text between xyz and 123
            print(result)
        else:
            print(log)

    else:
        click.echo("All function runs were successful.")


def main(collection_name: str = None, function_name: str = None, server=None):
    if server is None:
        server = TabsdataServer("127.0.0.1:2457", "admin", "tabsdata", "sys_admin")
    collections_list = server.list_collections()
    collection_list_names = [i.name for i in collections_list]
    options_dict = {i + 1: name for i, name in enumerate(collection_list_names)}
    options_string = "\n".join(f"[{i}] {name}" for i, name in options_dict.items())

    # Define Pattern for valid collection name
    pattern = re.compile(r"^(?=.*[A-Za-z])[A-Za-z0-9]{1,100}$")

    # Define Flag for Validating Collection
    collection_validated_flag = False
    initial_query = False

    # Validates the collection name or creates a new one
    while collection_validated_flag == False:
        if collection_name is None:
            if initial_query == False:
                collection_name = input(
                    "Please define a collection to use from the following list: "
                    "\n\n" + "\n" + options_string + "\n\n> "
                )
                initial_query = True
            else:
                collection_name = input(
                    "Collection Name not found. Please enter a valid collection name or valid index:\n> "
                )
            if pattern.match(collection_name) is None:
                if collection_name.isdigit():
                    if int(collection_name) in options_dict:
                        collection_name = options_dict[int(collection_name)]
                        collection_validated_flag = True
                    else:
                        collection_name = None
            else:
                if collection_name in collection_list_names:
                    collection_validated_flag = True
                else:
                    collection_name = None
        elif collection_name not in collection_list_names:
            collection_name = None
        # Breaks While Loop
        else:
            collection_validated_flag = True
    print(collection_name)

    functions_list = server.list_functions(collection_name)
    functions_list_names = [i.name for i in functions_list]
    functions_options_dict = {
        i + 1: name for i, name in enumerate(functions_list_names)
    }
    functions_options_string = "\n".join(
        f"[{i}] {name}" for i, name in functions_options_dict.items()
    )

    # Define Pattern for valid collection name
    pattern = re.compile(r"^(?=.*[A-Za-z])[A-Za-z0-9]{1,100}$")

    # Define Flag for Validating Collection
    function_validated_flag = False
    initial_query = False

    while function_validated_flag == False:
        if function_name is None:
            if initial_query == False:
                function_name = input(
                    "Please define a function to use from the following list: "
                    "\n\n" + "\n" + functions_options_string + "\n\n> "
                )
                initial_query = True
            else:
                function_name = input(
                    "Function name not found. Please enter a valid collection name or valid index:\n> "
                )
            if pattern.match(function_name) is None:
                if function_name.isdigit():
                    if int(function_name) in functions_options_dict:
                        function_name = functions_options_dict[int(function_name)]
                        function_validated_flag = True
                    else:
                        function_name = None
            else:
                if function_name in functions_list_names:
                    function_validated_flag = True
                else:
                    function_name = None
        elif function_name not in functions_list_names:
            function_name = None
        # Breaks While Loop
        else:
            function_validated_flag = True
    print(function_name)
    trigger = server.trigger_function(
        collection_name=collection_name,
        function_name=function_name,
    )
    transaction_list = server.list_transactions()
    execution_list = server.list_executions()
    transaction = [
        i
        for i in transaction_list
        if i.status not in ["Committed", "Failed", "Canceled", "Stalled"]
    ][-1]
    execution = [
        i
        for i in execution_list
        if i.status not in ["Committed", "Failed", "Canceled", "Stalled"]
    ][-1]
    monitor_execution_or_transaction(execution)
