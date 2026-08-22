from scripts.research.fixtures.mqtt_gateway import normalize_mqtt_payload


def test_telemetry_normalizes_to_log_envelope():
    env = normalize_mqtt_payload(
        topic="devices/pi-1/telemetry",
        payload=b'{"temperature_c": 22.5}',
        sender_id="bench-mqtt-gateway",
    )
    assert env["type"] == "log"
    assert env["payload"]["mqtt_topic"] == "devices/pi-1/telemetry"
    assert env["payload"]["temperature_c"] == 22.5


def test_command_normalizes_to_command_envelope():
    env = normalize_mqtt_payload(
        topic="devices/pi-1/command/shell-1",
        payload=b'{"body": "printf mqtt"}',
        sender_id="bench-mqtt-gateway",
    )
    assert env["type"] == "command"
    assert env["recipient_id"] == "shell-1"
    assert env["payload"]["body"] == "printf mqtt"
