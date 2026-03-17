# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- P2P agent-to-agent delegation via LLM tool pattern
- Comprehensive documentation for all EdgeCitadel features (docs/ directory)
- Future potential documentation with concrete examples and references
- Architecture Decision Records (ADR) framework
- Claude Code agents for code review, security review, test review, and document standards
- Path-specific rules for Python, React, NATS, E2E, and Docker
- Contributing guide with quality gates and conventions

### Changed
- Migrated from standalone Mosquitto to hybrid NATS+MQTT architecture
- Aggregator now uses native nats-py instead of MQTT client
- CLAUDE.md restructured for conciseness (<120 lines) with progressive disclosure via rules/

### Fixed
- join.sh MQTT connection test: force exit on connect, use grep for robust OK check
- Nested payload parsing in nats-listener inbox handler
- Reply routing: deliver responses to sender's inbox
