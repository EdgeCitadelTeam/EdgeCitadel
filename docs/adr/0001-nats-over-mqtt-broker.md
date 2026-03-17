# ADR-0001: Use NATS with Built-in MQTT Adapter Instead of Standalone MQTT Broker

## Status

Accepted

## Date

2025-12-01

## Context and Problem Statement

EdgeCitadel originally used a standalone Mosquitto MQTT broker for all agent communication. As the system grew, several limitations became apparent: MQTT's pub/sub model lacked native request-reply patterns, there was no built-in message persistence, the central broker was a single point of failure, and there was no support for streaming large payloads (like LLM token streams) across agent boundaries.

## Decision Drivers

- Need persistent message history for conversation replay
- Need request-reply pattern for command-response workflows
- Need streaming support for LLM token delivery
- Must maintain backward compatibility with IoT agents using MQTT
- Must work on constrained edge hardware (Raspberry Pi, ESP32)

## Considered Options

1. Keep Mosquitto MQTT broker, add persistence layer on top
2. Replace with NATS 2.10+ with JetStream and built-in MQTT adapter
3. Replace with Apache Kafka for streaming + separate MQTT broker

## Decision Outcome

Chosen option: "NATS 2.10+ with JetStream and built-in MQTT adapter", because it provides native JetStream persistence, request-reply patterns, and streaming support while maintaining MQTT compatibility through the built-in adapter — all in a single binary with minimal resource usage suitable for edge deployment.

### Consequences

#### Positive

- Single server handles both NATS native and MQTT protocols
- JetStream provides persistent message streams with replay capability
- Native request-reply pattern for command-response workflows
- K/V store for agent state management
- Low resource footprint suitable for edge hardware

#### Negative

- MQTT adapter has limitations vs full MQTT broker (QoS 2 not supported)
- Team must learn NATS concepts (subjects, streams, consumers)
- MQTT topic translation (slashes to dots) adds cognitive overhead

#### Neutral

- Auth model changes from per-user MQTT credentials to single NATS token
- Aggregator migrates from MQTT client to native NATS client for full feature access

## Links

- [NATS MQTT Adapter Docs](https://docs.nats.io/running-a-nats-service/configuration/mqtt)
- [JetStream Documentation](https://docs.nats.io/nats-concepts/jetstream)
