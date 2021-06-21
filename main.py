from mlops_meetup import dataflows
import click


@click.group()
def cli():
    pass


@cli.command()
def rawdata():
    dataflows.rawdata()
    

if __name__ == "__main__":
    cli()
