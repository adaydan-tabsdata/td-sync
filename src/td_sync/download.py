import os
from pathlib import Path

import polars as pl
from tabsdata.api.tabsdata_server import TabsdataServer

from td_sync.sync_v2 import load_server


def download_table(
    collection_name,
    table_name,
    socket: str = None,
    username: str = None,
    password: str = None,
    role: str = None,
):
    CONFIG_DIR = Path(os.path.expanduser("~/.td_custom_extensions"))
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    filepath = CONFIG_DIR / "temp_dir.parquet"

    if filepath.exists():
        filepath.unlink()

    if socket is not None:
        server = TabsdataServer(socket, username or "admin", password or "tabsdata", role or "sys_admin")
    else:
        server = load_server()
    table_columns = server.sample_table(
        collection_name=collection_name, table_name=table_name
    ).columns
    table = server.download_table(
        collection_name=collection_name,
        table_name=table_name,
        destination_file=filepath,
    )

    tableframe = pl.read_parquet(filepath).select(table_columns)
    # tableframe = td.TableFrame.from_polars(tableframe)
    return tableframe
