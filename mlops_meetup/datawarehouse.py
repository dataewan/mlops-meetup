import datetime
import prefect
from mlops_meetup import config
import duckdb


def connect_dw():
    con = duckdb.connect(config.DB_PATH)
    return con


@prefect.task
def get_most_recent_day(con: duckdb.DuckDBPyConnection) -> datetime.date:
    logger = prefect.context.get("logger")
    rel = con.execute(
        """
    SELECT MAX(reviewDate) FROM 'data/reviews/*.parquet'
    """
    )
    most_recent_day = rel.fetchone()[0]
    logger.info(f"Most recent day was {most_recent_day}")
    return most_recent_day


@prefect.task
def increment_date(d: datetime.date) -> datetime.date:
    return d + datetime.timedelta(days=1)
