from mlops_meetup import flows
import click


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


if __name__ == "__main__":
    cli()
