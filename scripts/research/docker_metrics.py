"""Read reproducible component counters from Docker Engine statistics."""

from __future__ import annotations

import http.client
import json
import os
import socket
import subprocess
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import cast
from urllib.parse import urlparse

from scripts.research.metrics import ComponentCounters

_COMPONENT_SERVICES = {
    "controller": "controller",
    "broker": "nats",
    "worker": "native-control",
    "observer": "runner",
}


class DockerComponentReader:
    """Map declared logical components to raw Docker Engine container counters."""

    def __init__(
        self,
        compose_service_id: Callable[[str], str],
        engine_stats: Callable[[str], Mapping[str, object]],
    ) -> None:
        self._compose_service_id = compose_service_id
        self._engine_stats = engine_stats

    def read(self, components: tuple[str, ...]) -> Mapping[str, ComponentCounters]:
        result: dict[str, ComponentCounters] = {}
        for component in components:
            try:
                service = _COMPONENT_SERVICES[component]
            except KeyError as error:
                raise ValueError("unknown Docker component") from error
            result[component] = _component_counters(
                self._engine_stats(self._compose_service_id(service))
            )
        return result


def build_docker_component_reader(
    *,
    project: str,
    compose_file: Path,
    environment: Mapping[str, str],
) -> DockerComponentReader:
    """Build the Linux Docker Engine reader for one owned compose topology."""
    if not project or not compose_file.is_file():
        raise ValueError("invalid Docker component reader configuration")

    def compose_service_id(service: str) -> str:
        completed = subprocess.run(
            [
                "docker",
                "compose",
                "--project-name",
                project,
                "--file",
                str(compose_file),
                "ps",
                "--quiet",
                service,
            ],
            check=True,
            capture_output=True,
            text=True,
            env=dict(environment),
        )
        ids = tuple(line for line in completed.stdout.splitlines() if line)
        if len(ids) != 1:
            raise ValueError("component container is not uniquely running")
        return ids[0]

    socket_path = _docker_socket_path(environment)
    return DockerComponentReader(
        compose_service_id,
        lambda container_id: _engine_stats(socket_path, container_id),
    )


def _component_counters(stats: Mapping[str, object]) -> ComponentCounters:
    cpu_stats = stats.get("cpu_stats")
    memory_stats = stats.get("memory_stats")
    networks = stats.get("networks")
    if (
        not isinstance(cpu_stats, Mapping)
        or not isinstance(memory_stats, Mapping)
        or not isinstance(networks, Mapping)
    ):
        raise TypeError("invalid Docker Engine stats")
    cpu_usage = cpu_stats.get("cpu_usage")
    memory_detail = memory_stats.get("stats")
    if cpu_usage is None or memory_detail is None:
        raise ValueError("invalid Docker Engine stats")
    if not isinstance(cpu_usage, Mapping) or not isinstance(memory_detail, Mapping):
        raise TypeError("invalid Docker Engine stats")
    total_usage = cpu_usage.get("total_usage")
    rss = memory_detail.get("rss", memory_detail.get("anon"))
    if (
        type(total_usage) is not int
        or total_usage < 0
        or type(rss) is not int
        or rss < 0
    ):
        raise ValueError("invalid Docker Engine stats")
    rx_bytes = 0
    tx_bytes = 0
    for interface in networks.values():
        if not isinstance(interface, Mapping):
            raise TypeError("invalid Docker Engine stats")
        rx = interface.get("rx_bytes")
        tx = interface.get("tx_bytes")
        if type(rx) is not int or rx < 0 or type(tx) is not int or tx < 0:
            raise ValueError("invalid Docker Engine stats")
        rx_bytes += rx
        tx_bytes += tx
    return ComponentCounters(
        cpu_seconds=total_usage / 1_000_000_000,
        rss_bytes=rss,
        rx_bytes=rx_bytes,
        tx_bytes=tx_bytes,
        application_bytes=0,
        nats_connection_bytes=0,
        http_bytes=0,
        storage_bytes=0,
        message_count=0,
    )


class _UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, socket_path: str) -> None:
        super().__init__("localhost")
        self._socket_path = socket_path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(self._socket_path)


def _engine_stats(socket_path: str, container_id: str) -> Mapping[str, object]:
    connection = _UnixHTTPConnection(socket_path)
    try:
        connection.request("GET", f"/containers/{container_id}/stats?stream=false")
        response = connection.getresponse()
        body = response.read()
    finally:
        connection.close()
    if response.status != 200:
        raise ValueError("Docker Engine stats request failed")
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("invalid Docker Engine stats") from error
    if not isinstance(value, Mapping):
        raise TypeError("invalid Docker Engine stats")
    return cast(Mapping[str, object], value)


def _docker_socket_path(environment: Mapping[str, str]) -> str:
    host = environment.get("DOCKER_HOST", os.environ.get("DOCKER_HOST", ""))
    if not host:
        return "/var/run/docker.sock"
    parsed = urlparse(host)
    if parsed.scheme != "unix" or not parsed.path:
        raise ValueError("Docker metrics require a Unix Docker Engine socket")
    return parsed.path


__all__ = ["DockerComponentReader", "build_docker_component_reader"]
