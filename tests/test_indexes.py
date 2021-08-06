from mlops_meetup import indexes


def test_group_users():
    test_data = [
        ("a", "1"),
        ("a", "1"),
        ("b", "1"),
        ("b", "2"),
        ("b", "2"),
    ]

    result = [("a", ["1", "1"]), ("b", ["1", "2", "2"])]

    assert indexes.group_users(test_data) == result


def test_flip_reverse_lookup():
    test_data = {
        1: "a",
        2: "b",
    }

    result = {
        "a": 1,
        "b": 2,
    }

    assert indexes.flip_reverse_lookup.run(test_data) == result
