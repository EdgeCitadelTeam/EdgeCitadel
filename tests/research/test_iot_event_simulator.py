from scripts.research.fixtures.iot_event_simulator import run_scenarios


def test_out_of_order_scenario_records_arrival_inversion():
    s1 = run_scenarios()[0]

    metrics = s1.evaluate()

    assert metrics["arrival_order_inversions"] > 0
