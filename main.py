from mlops_meetup import config
from mlops_meetup import flows, indexes, api, datawarehouse
from fastapi import FastAPI
import click

api_indexes = {}

app = FastAPI()


@app.on_event("startup")
def startup_event():
    con = datawarehouse.connect_dw()
    api_indexes["nn_index"] = indexes.load_nn_index(config.ITEM_INDEX_PATH)
    api_indexes["user_index"] = indexes.load_user_index(config.USER_INDEX_PATH)
    api_indexes["reverse_item_lookup"] = datawarehouse.get_reverse_item_lookup.run(con)


@app.get("/user/")
def read_user(user_id: str):
    return api.get_user_recs(
        user_id,
        api_indexes["nn_index"],
        api_indexes["user_index"],
        api_indexes["reverse_item_lookup"],
    )


@click.group()
def cli():
    pass


@cli.command()
def initiate():
    flows.convert_to_parquet()
    flows.create_baseline_dataset()


@cli.command()
def date_increment():
    flows.get_next_day()


@cli.command()
def training():
    flows.training_flow()


@cli.command()
def make_indexes():
    flows.make_indexes()


if __name__ == "__main__":
    cli()
