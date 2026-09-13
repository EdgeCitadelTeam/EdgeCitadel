"""Download the pinned broker into this validation unit, without installing it globally."""
from __future__ import annotations

import hashlib
import io
import platform
import tarfile
import urllib.request
from pathlib import Path

VERSION = "2.14.6"
# Official release SHA256SUMS, also pinned by EdgeCitadel scripts/nats_leaf.py
CHECKSUMS = {
    ("darwin", "amd64"): "239d4f3314334cc86cb12d7c252cf9b3461364451cc8ec5a39fb4915acd717c1",
    ("darwin", "arm64"): "291b9aa8c342b3cdcc53d872585bd467de4b5c9acf375066d882eb5412a09d55",
    ("linux", "amd64"): "61c3d55f69f61ec616b75782250936445f2819e9e5f2ae6159b10a31abd2200c",
    ("linux", "arm64"): "3ff6e463762db64186a36cf0276dae8320509e995151ad0153ba9c9f67eee3f9",
}
DEFAULT_BINARY = Path(__file__).resolve().parent / ".cache" / "nats-server"


def main() -> None:
    system = platform.system().lower()
    machine = {"x86_64": "amd64", "aarch64": "arm64"}.get(platform.machine(), platform.machine())
    checksum = CHECKSUMS.get((system, machine))
    if checksum is None:
        raise SystemExit("Supported platforms: macOS/Linux, amd64/arm64")
    name = f"nats-server-v{VERSION}-{system}-{machine}"
    url = f"https://github.com/nats-io/nats-server/releases/download/v{VERSION}/{name}.tar.gz"
    limit = 64 * 1024 * 1024
    with urllib.request.urlopen(url, timeout=60) as response:
        archive = response.read(limit + 1)
    if len(archive) > limit or hashlib.sha256(archive).hexdigest() != checksum:
        raise RuntimeError("Archive size/checksum mismatch; no binary installed")
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        member = tar.getmember(f"{name}/nats-server")
        if not member.isfile() or member.size > limit:
            raise RuntimeError("Expected a bounded regular binary file")
        with tar.extractfile(member) as source:
            binary = source.read(limit + 1)
    DEFAULT_BINARY.parent.mkdir(parents=True, exist_ok=True)
    temporary = DEFAULT_BINARY.with_suffix(".download")
    temporary.write_bytes(binary)
    temporary.chmod(0o755)
    temporary.replace(DEFAULT_BINARY)
    print(DEFAULT_BINARY)


if __name__ == "__main__":
    main()
