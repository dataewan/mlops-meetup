from datetime import date

import pyarrow
from mlops_meetup import parquet_ingest


def test_make_two_weeks():
    assert parquet_ingest.make_two_weeks(date(2016, 1, 1)) == [
        date(2016, 1, 1),
        date(2016, 1, 2),
        date(2016, 1, 3),
        date(2016, 1, 4),
        date(2016, 1, 5),
        date(2016, 1, 6),
        date(2016, 1, 7),
        date(2016, 1, 8),
        date(2016, 1, 9),
        date(2016, 1, 10),
        date(2016, 1, 11),
        date(2016, 1, 12),
        date(2016, 1, 13),
        date(2016, 1, 14),
    ]


def test_date_to_int():
    assert parquet_ingest.date_to_int(date(2016, 1, 1)) == (1451606400, 1451692800)


def test_add_date_column():
    d = date(2016, 1, 2)

    col_a = pyarrow.array([1, 2, 3], pyarrow.int32())
    col_b = pyarrow.array([1, 2, 3], pyarrow.int32())

    date_col = pyarrow.array(
        [
            d,
            d,
            d,
        ]
    )

    in_table = pyarrow.Table.from_arrays(
        [col_a, col_b],
        schema=pyarrow.schema(
            [pyarrow.field("a", col_a.type), pyarrow.field("b", col_b.type)]
        ),
    )

    out_table = pyarrow.Table.from_arrays(
        [col_a, col_b, date_col],
        schema=pyarrow.schema(
            [
                pyarrow.field("a", col_a.type),
                pyarrow.field("b", col_b.type),
                pyarrow.field("reviewDate", date_col.type),
            ]
        ),
    )

    test_table = parquet_ingest.add_date_column(d, in_table)
    assert test_table == out_table
