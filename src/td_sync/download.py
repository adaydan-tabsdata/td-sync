import tabsdata as td
import os
from tabsdata import api
from tabsdata.api.tabsdata_server import TabsdataServer, APIServer
import inspect
import sys
import re
from pathlib import Path
import polars as pl


def download_table(
    collection_name,
    table_name,
    socket: str = "127.0.0.1:2457",
    username: str = "admin",
    password: str = "tabsdata",
    role: str = "sys_admin",
):
    CONFIG_DIR = Path(os.path.expanduser("~/.td_custom_extensions"))
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    filepath = CONFIG_DIR / "temp_dir.parquet"

    if filepath.exists():
        filepath.unlink()

    server = TabsdataServer(socket, username, password, role)
    table_columns = server.sample_table(
        collection_name=collection_name, table_name=table_name
    ).columns
    table = server.download_table(
        collection_name=collection_name,
        table_name=table_name,
        destination_file=filepath,
    )

    tableframe = pl.read_parquet(filepath).select("timestamp")
    # tableframe = td.TableFrame.from_polars(tableframe)
    return tableframe
