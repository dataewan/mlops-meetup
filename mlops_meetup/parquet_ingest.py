from prefect import task
import pyarrow
import pyarrow.json
import pyarrow.parquet
import pyarrow.compute
import pyarrow.fs
from datetime import timedelta, date
import os
import time
from typing import Tuple, List
import shutil
from mlops_meetup import config


def filterTimestamp(table):
    return table.filter(
        pyarrow.compute.greater_equal(table["unixReviewTime"], 1451606400)
    )


def make_two_weeks(start_date: date = date(2016, 1, 1), number_days=14) -> List[date]:
    return [start_date + timedelta(i) for i in range(number_days)]


def date_to_int(d: date) -> Tuple[int, int]:
    start_epoch = int(time.mktime(d.timetuple()))
    end_epoch = start_epoch + (24 * 60 * 60)
    return start_epoch, end_epoch


@task
def reviews_to_parquet(inputfile: str, outputdir: str):
    table = pyarrow.json.read_json(inputfile)
    table = filterTimestamp(table)
    pyarrow.parquet.write_to_dataset(table, root_path=outputdir)


@task
def meta_to_parquet(inputfile: str, outputdir: str):
    table = pyarrow.json.read_json(
        inputfile,
        parse_options=pyarrow.json.ParseOptions(
            # need explicit schema because inferring it fails on this dataset.
            explicit_schema=pyarrow.schema(
                [
                    pyarrow.field("asin", pyarrow.string()),
                    pyarrow.field("title", pyarrow.string()),
                    pyarrow.field("imageURLHighRes", pyarrow.list_(pyarrow.string())),
                    pyarrow.field("category", pyarrow.list_(pyarrow.string())),
                ]
            ),
            unexpected_field_behavior="ignore",
        ),
    )
    pyarrow.parquet.write_to_dataset(table, root_path=outputdir)


def filter_table_on_date(d: date, table: pyarrow.Table) -> pyarrow.Table:
    start_epoch, end_epoch = date_to_int(d)

    table = table.filter(
        pyarrow.compute.greater_equal(table["unixReviewTime"], start_epoch)
    )
    table = table.filter(pyarrow.compute.less_equal(table["unixReviewTime"], end_epoch))
    return table


def add_date_column(d: date, table: pyarrow.Table) -> pyarrow.Table:
    table = table.append_column(
        "reviewDate", pyarrow.array([d] * len(table), pyarrow.date32())
    )
    return table


@task
def daily_extract(d: date):
    table = pyarrow.parquet.read_table(config.RAW_REVIEWS_PARQUET_PATH)
    table = filter_table_on_date(d, table)
    table = add_date_column(d, table)
    pyarrow.parquet.write_to_dataset(
        table,
        root_path=config.DAILY_REVIEWS_PARQUET_PATH,
    )


@task
def clean_path(path: str) -> str:
    if os.path.exists(path):
        shutil.rmtree(path)

    return path
