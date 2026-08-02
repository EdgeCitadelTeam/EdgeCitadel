"""Docker Engine counter-reader contracts for research resource sampling."""

from __future__ import annotations

from collections.abc import Mapping

from scripts.research.docker_metrics import DockerComponentReader


def test_reader_maps_declared_components_to_compose_services_and_engine_counters() -> None:
    calls: list[str] = []
    service_ids = {
        "controller": "controller-id",
        "nats": "nats-id",
        "native-control": "worker-id",
        "runner": "runner-id",
    }

    def compose_service_id(service: str) -> str:
        calls.append(f"compose:{service}")
        return service_ids[service]

    def stats(container_id: str) -> Mapping[str, object]:
        calls.append(f"stats:{container_id}")
        return {
            "cpu_stats": {"cpu_usage": {"total_usage": 2_500_000_000}},
            "memory_stats": {"stats": {"rss": 4096}},
            "networks": {"eth0": {"rx_bytes": 100, "tx_bytes": 200}},
        }

    reader = DockerComponentReader(compose_service_id, stats)

    snapshot = reader.read(("controller", "broker", "worker", "observer"))

    assert calls == [
        "compose:controller",
        "stats:controller-id",
        "compose:nats",
        "stats:nats-id",
        "compose:native-control",
        "stats:worker-id",
        "compose:runner",
        "stats:runner-id",
    ]
    assert tuple(snapshot) == ("controller", "broker", "worker", "observer")
    assert snapshot["controller"].cpu_seconds == 2.5
    assert snapshot["broker"].rss_bytes == 4096
    assert snapshot["worker"].rx_bytes == 100
    assert snapshot["observer"].tx_bytes == 200


def test_reader_rejects_missing_engine_counter_fields() -> None:
    reader = DockerComponentReader(
        lambda _service: "container-id",
        lambda _container_id: {"cpu_stats": {}, "memory_stats": {}, "networks": {}},
    )

    try:
        reader.read(("controller",))
    except ValueError as error:
        assert str(error) == "invalid Docker Engine stats"
    else:
        raise AssertionError("missing Docker Engine counters were accepted")
