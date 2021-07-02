from prefect import Flow
from mlops_meetup import parquet_ingest, config, datawarehouse


def convert_to_parquet():
    with Flow("Convert to parquet") as flow:
        reviews_path = parquet_ingest.clean_path(config.RAW_REVIEWS_PARQUET_PATH)
        meta_path = parquet_ingest.clean_path(config.META_PARQUET_PATH)
        parquet_ingest.reviews_to_parquet(
            "data/Musical_Instruments.json.gz", reviews_path
        )
        parquet_ingest.meta_to_parquet(
            "data/meta_Musical_Instruments.json.gz", meta_path
        )

    flow.run()


def create_baseline_dataset():
    dates = parquet_ingest.make_two_weeks()
    with Flow("Create baseline dataset") as flow:
        _ = parquet_ingest.clean_path(config.DAILY_REVIEWS_PARQUET_PATH)
        parquet_ingest.daily_extract.map(dates)
    flow.run()


def get_next_day():
    with Flow("Get next day of data") as flow:
        con = datawarehouse.connect_dw()
        most_recent_day = datawarehouse.get_most_recent_day(con)
        next_day = datawarehouse.increment_date(most_recent_day)
        parquet_ingest.daily_extract(next_day)

    flow.run()
