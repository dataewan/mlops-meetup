import datetime
from mlops_meetup import datawarehouse


def test_increment_date():
    in_date = datetime.date(2016, 1, 1)
    expected_date = datetime.date(2016, 1, 2)

    assert datawarehouse.increment_date.run(in_date) == expected_date
