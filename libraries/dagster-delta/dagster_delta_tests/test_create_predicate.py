from dagster_delta._handler.utils.predicates import create_predicate


def test_create_predicate_string_list_proper_escaping():
    filters = [("country", "IN", ["foo", "bar", "ba'z", "ba''z"])]

    pred = create_predicate(filters)

    assert pred == "`country` IN ('foo', 'bar', 'ba''z', 'ba''''z')"


def test_create_predicate_string_additional_filters():
    filters = [("country", "IN", ["foo", "bar", "ba'z", "ba''z"])]
    add_pred_str = "flag in ('red','white','blue')"
    pred = create_predicate(filters, add_pred_str)

    assert (
        pred == "`country` IN ('foo', 'bar', 'ba''z', 'ba''''z') AND flag in ('red','white','blue')"
    )
