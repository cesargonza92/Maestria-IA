from rules import (
    MIN_AMOUNT_SUM_HIGH_AMOUNT,
    MIN_COUNTRIES_MULTI_COUNTRY,
    MIN_MERCHANTS_MULTI_MERCHANT,
    MIN_TRANSACTIONS_HIGH_FREQUENCY,
    RULE_HIGH_AMOUNT,
    RULE_HIGH_FREQUENCY,
    RULE_MULTI_COUNTRY,
    RULE_MULTI_MERCHANT,
    CardWindowStatsFn,
    build_alert_id,
    evaluate_rules,
)


def _stats(count=1, amount_sum=0, countries=None, merchants=None):
    return {
        "count": count,
        "amount_sum": amount_sum,
        "countries": set(countries or ["PY"]),
        "merchants": set(merchants or ["merch-1"]),
    }


def test_no_rules_triggered_for_quiet_card():
    stats = _stats(count=1, amount_sum=10_000)
    assert evaluate_rules(stats) == []


def test_high_frequency_threshold_is_inclusive():
    below = _stats(count=MIN_TRANSACTIONS_HIGH_FREQUENCY - 1)
    at = _stats(count=MIN_TRANSACTIONS_HIGH_FREQUENCY)
    assert RULE_HIGH_FREQUENCY not in evaluate_rules(below)
    assert RULE_HIGH_FREQUENCY in evaluate_rules(at)


def test_high_amount_is_strictly_greater_than_threshold():
    at_limit = _stats(amount_sum=MIN_AMOUNT_SUM_HIGH_AMOUNT)
    above_limit = _stats(amount_sum=MIN_AMOUNT_SUM_HIGH_AMOUNT + 1)
    assert RULE_HIGH_AMOUNT not in evaluate_rules(at_limit)
    assert RULE_HIGH_AMOUNT in evaluate_rules(above_limit)


def test_multi_country_threshold():
    one_country = _stats(countries=["PY"])
    two_countries = _stats(countries=["PY", "AR"][:MIN_COUNTRIES_MULTI_COUNTRY])
    assert RULE_MULTI_COUNTRY not in evaluate_rules(one_country)
    assert RULE_MULTI_COUNTRY in evaluate_rules(two_countries)


def test_multi_merchant_threshold():
    few_merchants = _stats(merchants=[f"m{i}" for i in range(MIN_MERCHANTS_MULTI_MERCHANT - 1)])
    enough_merchants = _stats(merchants=[f"m{i}" for i in range(MIN_MERCHANTS_MULTI_MERCHANT)])
    assert RULE_MULTI_MERCHANT not in evaluate_rules(few_merchants)
    assert RULE_MULTI_MERCHANT in evaluate_rules(enough_merchants)


def test_a_card_can_trigger_more_than_one_rule_at_once():
    stats = _stats(
        count=MIN_TRANSACTIONS_HIGH_FREQUENCY,
        amount_sum=MIN_AMOUNT_SUM_HIGH_AMOUNT + 1,
        countries=["PY", "AR"],
        merchants=[f"m{i}" for i in range(MIN_MERCHANTS_MULTI_MERCHANT)],
    )
    triggered = evaluate_rules(stats)
    assert set(triggered) == {
        RULE_HIGH_FREQUENCY,
        RULE_HIGH_AMOUNT,
        RULE_MULTI_COUNTRY,
        RULE_MULTI_MERCHANT,
    }


def test_card_window_stats_fn_is_incremental_and_commutative():
    combine_fn = CardWindowStatsFn()
    events = [
        {"amount": 100, "country": "PY", "merchant_id": "m1"},
        {"amount": 200, "country": "AR", "merchant_id": "m2"},
        {"amount": 300, "country": "PY", "merchant_id": "m1"},
    ]

    acc = combine_fn.create_accumulator()
    for event in events:
        acc = combine_fn.add_input(acc, event)
    sequential = combine_fn.extract_output(acc)

    acc_a = combine_fn.create_accumulator()
    acc_a = combine_fn.add_input(acc_a, events[0])
    acc_b = combine_fn.create_accumulator()
    for event in events[1:]:
        acc_b = combine_fn.add_input(acc_b, event)
    merged = combine_fn.extract_output(combine_fn.merge_accumulators([acc_a, acc_b]))

    assert sequential == merged
    assert sequential["count"] == 3
    assert sequential["amount_sum"] == 600
    assert sequential["countries"] == {"PY", "AR"}
    assert sequential["merchants"] == {"m1", "m2"}


def test_build_alert_id_is_stable_for_retries_and_late_panes():
    alert_id_a = build_alert_id("card-1", "2026-08-22T14:00:00Z", "2026-08-22T14:01:00Z", "HIGH_AMOUNT")
    alert_id_b = build_alert_id("card-1", "2026-08-22T14:00:00Z", "2026-08-22T14:01:00Z", "HIGH_AMOUNT")
    assert alert_id_a == alert_id_b


def test_build_alert_id_differs_by_type_or_window():
    base = build_alert_id("card-1", "2026-08-22T14:00:00Z", "2026-08-22T14:01:00Z", "HIGH_AMOUNT")
    other_type = build_alert_id("card-1", "2026-08-22T14:00:00Z", "2026-08-22T14:01:00Z", "HIGH_FREQUENCY")
    other_window = build_alert_id("card-1", "2026-08-22T14:01:00Z", "2026-08-22T14:02:00Z", "HIGH_AMOUNT")
    assert base != other_type
    assert base != other_window
