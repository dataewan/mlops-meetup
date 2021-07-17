from mlops_meetup import modelling
import numpy as np


def test_make_empty_batch():
    result = modelling.make_empty_batch(5, 10)

    assert result.shape == (55, 3)
    assert np.sum(result) == 0


def test_negative_sampler():
    data = {
        "user_id": np.arange(200),
        "item_id": np.arange(200),
    }

    positive_indexes = np.array([0, 1, 2, 3, 4, 5])
    negative_ratio = 2

    negative_users, negative_items = modelling.negative_sampler(
        data, positive_indexes, negative_ratio
    )

    assert len(negative_users) == len(negative_items)
