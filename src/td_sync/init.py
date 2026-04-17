import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Templates
# ---------------------------------------------------------------------------

_PUBLISHER_POSTGRES = '''\
import os

import tabsdata as td

pg_username = os.getenv("PG_USERNAME")
pg_password = os.getenv("PG_PASSWORD")


@td.publisher(
    source=td.PostgresSrc(
        conn=td.PostgresConn(
            uri="postgres://host:5432/dbname",
            credentials=td.UserPasswordCredentials(pg_username, pg_password),
        ),
        queries="SELECT * FROM schema.table_name",
    ),
    tables=["my_table"],
)
def publisher(tf: td.TableFrame) -> td.TableFrame:
    return tf
'''

_PUBLISHER_S3 = '''\
import os

import tabsdata as td

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_S3_URI = os.getenv("AWS_S3_URI")
AWS_REGION = os.getenv("AWS_REGION")

s3_credentials = td.S3AccessKeyCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY)


@td.publisher(
    source=td.S3Source(
        uri=[f"{AWS_S3_URI}/path/to/file.parquet"],
        credentials=s3_credentials,
        region=AWS_REGION,
    ),
    tables=["my_table"],
)
def publisher(tf: td.TableFrame) -> td.TableFrame:
    return tf
'''

_PUBLISHER_DATABRICKS = '''\
import os

import tabsdata as td

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")


@td.publisher(
    source=td.DatabricksSource(
        host_url=DATABRICKS_HOST,
        token=DATABRICKS_TOKEN,
        queries="SELECT * FROM catalog.schema.table_name",
        warehouse="my_warehouse",
    ),
    tables=["my_table"],
)
def publisher(tf: td.TableFrame) -> td.TableFrame:
    return tf
'''

_SUBSCRIBER_POSTGRES = '''\
import os

import tabsdata as td

pg_username = os.getenv("PG_USERNAME")
pg_password = os.getenv("PG_PASSWORD")


@td.subscriber(
    tables=["my_table"],
    destination=td.PostgresDest(
        conn=td.PostgresConn(
            uri="postgres://host:5432/dbname",
            credentials=td.UserPasswordCredentials(pg_username, pg_password),
        ),
        destination_tables="schema.my_table",
        if_table_exists="replace",
    ),
)
def subscriber(tf: td.TableFrame) -> td.TableFrame:
    return tf
'''

_SUBSCRIBER_S3 = '''\
import os

import tabsdata as td

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_S3_URI = os.getenv("AWS_S3_URI")
AWS_REGION = os.getenv("AWS_REGION")
AWS_GLUE_DATABASE = os.getenv("AWS_GLUE_DATABASE")

s3_credentials = td.S3AccessKeyCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY)


@td.subscriber(
    tables=["my_table"],
    destination=td.S3Destination(
        uri=[f"{AWS_S3_URI}/output/my_table/my_table-$EXPORT_TIMESTAMP.parquet"],
        region=AWS_REGION,
        credentials=s3_credentials,
        catalog=td.AWSGlue(
            definition={
                "name": "default",
                "type": "glue",
                "client.region": AWS_REGION,
            },
            tables=[f"{AWS_GLUE_DATABASE}.my_table"],
            auto_create_at=[f"{AWS_S3_URI}/output/my_table"],
            if_table_exists="replace",
            credentials=s3_credentials,
        ),
    ),
)
def subscriber(data: td.TableFrame) -> td.TableFrame:
    return data
'''

_SUBSCRIBER_SNOWFLAKE = '''\
import os

import tabsdata as td

CONNECTION_PARAMETERS = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PAT"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}


@td.subscriber(
    tables=["my_table"],
    destination=td.SnowflakeDestination(
        CONNECTION_PARAMETERS,
        destination_table="my_table",
        if_table_exists="replace",
    ),
)
def subscriber(tf: td.TableFrame) -> td.TableFrame:
    return tf
'''

_SUBSCRIBER_DATABRICKS = '''\
import os

import tabsdata as td

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")


@td.subscriber(
    tables=["my_table"],
    destination=td.DatabricksDestination(
        host_url=DATABRICKS_HOST,
        token=DATABRICKS_TOKEN,
        tables=["catalog.schema.my_table"],
        volume="my_volume",
        warehouse="my_warehouse",
        if_table_exists="replace",
    ),
)
def subscriber(tf: td.TableFrame) -> td.TableFrame:
    return tf
'''

_TRANSFORMER = '''\
import tabsdata as td


@td.transformer(
    input_tables=["input_table"],
    output_tables=["output_table"],
)
def transformer(tf: td.TableFrame) -> td.TableFrame:
    # Transform your data here
    return tf
'''

# ---------------------------------------------------------------------------
# Lookup tables
# ---------------------------------------------------------------------------

_PUBLISHERS: dict[str, str] = {
    "postgres": _PUBLISHER_POSTGRES,
    "s3": _PUBLISHER_S3,
    "databricks": _PUBLISHER_DATABRICKS,
}

_SUBSCRIBERS: dict[str, str] = {
    "postgres": _SUBSCRIBER_POSTGRES,
    "s3": _SUBSCRIBER_S3,
    "snowflake": _SUBSCRIBER_SNOWFLAKE,
    "databricks": _SUBSCRIBER_DATABRICKS,
}

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _usage():
    publishers = ", ".join(_PUBLISHERS)
    subscribers = ", ".join(_SUBSCRIBERS)
    print("Usage: td-init <type> [connector]")
    print()
    print("  td-init publisher <connector>   Create a publisher template")
    print(f"    connectors: {publishers}")
    print()
    print("  td-init subscriber <connector>  Create a subscriber template")
    print(f"    connectors: {subscribers}")
    print()
    print("  td-init transformer             Create a transformer template")


def main():
    args = sys.argv[1:]

    if not args:
        _usage()
        sys.exit(1)

    fn_type = args[0].lower()

    if fn_type == "transformer":
        content = _TRANSFORMER
        filename = "transformer.py"

    elif fn_type == "publisher":
        if len(args) < 2:
            print(f"Error: 'publisher' requires a connector.\n")
            _usage()
            sys.exit(1)
        connector = args[1].lower()
        if connector not in _PUBLISHERS:
            print(
                f"Error: unknown publisher connector '{connector}'.\n"
                f"Available: {', '.join(_PUBLISHERS)}"
            )
            sys.exit(1)
        content = _PUBLISHERS[connector]
        filename = f"publisher_{connector}.py"

    elif fn_type == "subscriber":
        if len(args) < 2:
            print(f"Error: 'subscriber' requires a connector.\n")
            _usage()
            sys.exit(1)
        connector = args[1].lower()
        if connector not in _SUBSCRIBERS:
            print(
                f"Error: unknown subscriber connector '{connector}'.\n"
                f"Available: {', '.join(_SUBSCRIBERS)}"
            )
            sys.exit(1)
        content = _SUBSCRIBERS[connector]
        filename = f"subscriber_{connector}.py"

    else:
        print(f"Error: unknown type '{fn_type}'.\n")
        _usage()
        sys.exit(1)

    dest = Path.cwd() / filename
    if dest.exists():
        print(f"Error: '{filename}' already exists in the current directory.")
        sys.exit(1)

    dest.write_text(content)
    print(f"Created {dest}")
