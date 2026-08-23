from scripts.research.fixtures.iot_event_simulator import run_scenarios


def test_out_of_order_scenario_records_arrival_inversion():
    s1 = run_scenarios()[0]

    metrics = s1.evaluate()

    assert metrics["arrival_order_inversions"] > 0


def test_temporal_evaluator_rejects_replay_and_injection_data():
    scenarios = run_scenarios()
    assert scenarios[2].evaluate()["stale_events_accepted"] == 0
    assert scenarios[4].evaluate()["prompt_injection_followed"] is False
