import json
import dask.distributed
import dask.bag
from prefect import task, Flow


def process_review_line(line: str):
    record = json.loads(line)
    return {
        "reviewerID": record.get("reviewerID"),
        "asin": record.get("asin"),
        "unixReviewTime": record.get("unixReviewTime"),
    }


def process_meta_line(line: str):
    record = json.loads(line)
    return {
        "asin": record.get("asin"),
        "title": record.get("title"),
    }


@task
def process_review_file(filename: str):
    lines = dask.bag.read_text(filename)
    records = lines.map(process_review_line)
    df = records.to_dataframe()
    df.to_parquet("data/reviews/")


@task
def process_meta_file(filename: str):
    lines = dask.bag.read_text(filename)
    records = lines.map(process_meta_line)
    df = records.to_dataframe()
    df.to_parquet("data/meta/")


def rawdata():
    client = dask.distributed.Client(n_workers=4)
    with Flow("Raw data processing flow") as flow:
        process_review_file("data/Musical_Instruments.json.gz")
        process_meta_file("data/meta_Musical_Instruments.json.gz")
        flow.run()
