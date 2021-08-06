import pytest
import datetime
from mlops_meetup import datawarehouse


def test_increment_date():
    in_date = datetime.date(2016, 1, 1)
    expected_date = datetime.date(2016, 1, 2)

    assert datawarehouse.increment_date.run(in_date) == expected_date


def test_hydrate_sql(mocker):
    mocker.patch(
        "mlops_meetup.datawarehouse._read_sql",
        return_value="{a} putting parameters {b}",
    )

    assert (
        datawarehouse.hydrate_sql("any path", {"a": "Testing", "b": "works"})
        == "Testing putting parameters works"
    )


def test_hydrate_sql_without_params(mocker):
    mocker.patch(
        "mlops_meetup.datawarehouse._read_sql",
        return_value="{a} putting parameters {b}",
    )

    assert datawarehouse.hydrate_sql("any path") == "{a} putting parameters {b}"


def test_lookup_to_dict():
    test_data = [
        (1, "a"),
        (2, "b"),
    ]

    assert datawarehouse.lookup_to_dict(test_data) == {
        1: "a",
        2: "b",
    }
