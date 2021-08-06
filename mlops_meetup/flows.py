from prefect import Flow
from mlops_meetup import (
    parquet_ingest,
    config,
    datawarehouse,
    modelling,
    indexes,
)


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


def training_flow():
    with Flow("Training model") as flow:
        con = datawarehouse.connect_dw()
        meta_table = datawarehouse.create_meta_table(con)
        review_table = datawarehouse.create_review_table(con)
        lookups = datawarehouse.create_lookups(
            con, upstream_tasks=[meta_table, review_table]
        )
        max_item_id, max_user_id = datawarehouse.get_max_ids(con)
        training_data = datawarehouse.training_data(con, upstream_tasks=[lookups])

        model = modelling.train_model(training_data, max_item_id, max_user_id)

        modelling.save_model(model, config.MODEL_PATH)

    flow.run()


def make_indexes():
    with Flow("Create nearest neighbours index") as flow:
        con = datawarehouse.connect_dw()
        reverse_item_lookup = datawarehouse.get_reverse_item_lookup(con)
        item_lookup = indexes.flip_reverse_lookup(reverse_item_lookup)
        user_metadata = datawarehouse.get_user_metadata(con)
        model = modelling.load_model(config.MODEL_PATH)
        item_embeddings = indexes.extract_item_embeddings(model)
        item_index = indexes.make_nn_index(item_embeddings, reverse_item_lookup)
        user_index = indexes.make_user_index(
            user_metadata, item_lookup, item_embeddings
        )

        indexes.persist_nn_index(item_index, config.ITEM_INDEX_PATH)
        indexes.persist_user_index(user_index, config.USER_INDEX_PATH)

    flow.run()
