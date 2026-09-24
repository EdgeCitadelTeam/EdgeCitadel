# Native macOS trace storage

The native service uses the existing macOS account and a private, fixed-size
512 MiB UDRW image containing journaled HFS+. This is user-managed bounded
storage: the image imposes a physical ceiling, while the existing 256 MiB
application admission thresholds remain unchanged. It does not provide Linux
inode quotas or protection against deliberate changes by the same user.
Linux retains its existing dedicated-UID ext4 quota verifier unchanged.

## Layout and startup

Within the node's `agentd` directory:

- `trace-volume.dmg`: fully allocated 512 MiB image, mode 0600.
- `trace-volume.json`: durable provisioning phase, image device/inode identity,
  volume UUID, label and volume size.
- `trace/agentd.sqlite3`: trace database, on the mounted HFS+ volume.
- `agentd-tasks.sqlite3` and `payload.key`: task database and existing encryption
  key, outside the trace volume.

Every production database open verifies the mount, backing-image association,
format, size, private ownership and task/key separation. The service holds its
state-directory writer lock while mounting. A verified existing mount is reused;
an unmounted image is attached before database admission. Missing, replaced,
resized, sparse, incorrectly mounted or publicly accessible storage is refused.
Startup never writes into the underlying unmounted directory or force-detaches a
busy volume. Rollback journals and the attached task/trace transaction remain
required.

The bound is 536870912 image bytes. The journaled volume is slightly smaller
because of partition metadata; health reports both numbers independently.
Filesystem maintenance entries created by macOS are not an inode quota.

## Explicit offline upgrade

Install the matching CLI/runtime distribution, then run:

```sh
edgecitadel service storage-setup --state-dir "$HOME/.edgecitadel"
EDGECITADEL_TRACE_SYNC=1 edgecitadel service start --state-dir "$HOME/.edgecitadel"
edgecitadel service status --state-dir "$HOME/.edgecitadel" --json
```

`storage-setup` stops the service, refreshes the CLI-managed runtime and schemas,
provisions or reuses native storage, and performs an offline migration when a
legacy source is present. The service remains stopped if provisioning or
migration fails. For an externally managed runtime, install matching code and
schemas there first. Provisioning refuses unrelated images and mount contents.
For launchd, an explicit `EDGECITADEL_TRACE_SYNC=0|1` is persisted in the service
plist and retained across ordinary service restarts. Core collection must also
be enabled on the existing Core deployment.

Schema 6 is upgraded using `AgentdStore`'s existing schema transformations.
Schema 29 pairs retain the existing byte-preserving layout migration. No
additional backup is created. Shared-schema conversion uses these durable phases:

1. `staging`: install the startup barrier, recover the closed SQLite source into
   DELETE journal mode, record source/key hashes, and build the upgraded pair in
   private transaction working storage.
2. `ready`: validate both databases, pair identity and cross-database references;
   fsync the pair and record output hashes.
3. While still fenced, copy and fsync the trace database to the bounded volume,
   atomically move the task database into place, validate the activated pair,
   and retire the source and staging files before removing the barrier.

Retries rebuild interrupted staging from the unchanged source or finish a
recorded handoff. Changed source/key/output hashes, mismatched pairs and malformed
barriers fail closed. Node/connector identities, credentials, ciphertext, results,
sessions, pending delivery and existing trace/export identities are preserved;
source epochs are not rotated. Temporary migration copies are offline working
state outside the trace volume and are removed after successful cutover.

## Diagnostics and qualification

Health exposes `schema_version` and `storage_backend`, including backend,
`mount_verified`, trust boundary, physical/admission limits, actual volume size,
available bytes, volume UUID and migration status. Unverified component fixtures
remain explicitly unverified. Existing unsupported model/tool observations stay
unavailable; upgrading storage does not reconstruct historical observations.

Native fixture qualification:

```sh
RUN_AGENTD_MACOS_STORAGE=1 python -m pytest -q \
  tests/agentd/test_storage_macos_native.py
```

The tests exercise actual provisioning, mount reuse/remount, daemon restart,
wrong backing image, altered UUID/permissions/size, schema-6 migration, physical
exhaustion/retry, and abrupt process exit with an attached transaction. Other
migration tests cover populated encrypted results, sessions, pending messages,
interrupted durable phases and mismatched output rejection. On HFS+ exhaustion,
SQLite may report `SQLITE_CANTOPEN` when journal creation fails; verification
asserts rollback and durable retry, not only a particular SQLite error number.

These checks do not replace the live two-agent acceptance gate. That gate needs
a reloaded native Codex MCP process, a real Codex-to-Hermes task, Core settlement,
connected execution and messaging evidence for both identities, browser inspection
and retained history readable with the source service stopped. A historical
Hermes-only graph does not pass.
