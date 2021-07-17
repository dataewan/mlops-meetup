import datetime
import prefect
from mlops_meetup import config
import duckdb
from typing import Dict, Optional, Tuple


def _read_sql(path: str) -> str:
    with open(path, "r") as f:
        return f.read()


def hydrate_sql(path: str, params: Optional[Dict[str, str]] = None) -> str:
    contents = _read_sql(path)
    if params:
        return contents.format(**params)

    return contents


def connect_dw():
    con = duckdb.connect(config.DB_PATH)
    return con


@prefect.task
def get_most_recent_day(con: duckdb.DuckDBPyConnection) -> datetime.date:
    logger = prefect.context.get("logger")
    rel = con.execute(hydrate_sql("scripts/most_recent_day.sql"))
    most_recent_day = rel.fetchone()[0]
    logger.info(f"Most recent day was {most_recent_day}")
    return most_recent_day


@prefect.task
def increment_date(d: datetime.date) -> datetime.date:
    return d + datetime.timedelta(days=1)


@prefect.task
def create_meta_table(con: duckdb.DuckDBPyConnection):
    con.execute(hydrate_sql("scripts/meta.sql"))


@prefect.task
def create_review_table(con: duckdb.DuckDBPyConnection):
    con.execute(hydrate_sql("scripts/reviews.sql"))


@prefect.task
def create_lookups(con: duckdb.DuckDBPyConnection):
    con.execute(hydrate_sql("scripts/lookups.sql"))


@prefect.task
def training_data(con: duckdb.DuckDBPyConnection):
    res = con.execute(hydrate_sql("scripts/training_data.sql"))

    return res.fetchnumpy()


@prefect.task
def get_max_ids(con: duckdb.DuckDBPyConnection) -> Tuple[int, int]:
    item_id = con.execute(hydrate_sql("scripts/get_max_item_id.sql")).fetchone()
    user_id = con.execute(hydrate_sql("scripts/get_max_user_id.sql")).fetchone()

    return item_id[0], user_id[0]
