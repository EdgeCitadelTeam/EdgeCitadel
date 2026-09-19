# Linux trace storage provisioning

Production Agentd requires a dedicated unprivileged UID and an administrator-owned
ext4 filesystem with enforced user quotas. The UID has a **256 MiB hard allocation
limit and 128-inode limit**. Trace files, directories, SQLite journals and open
unlinked allocations count toward its user quota. Shared ext4 infrastructure is
accounted separately. The daemon verifies enforcement, UID/capability restrictions,
private ownership and separation from the task/key filesystem before admission.

`provision.py` creates a new service account, private state directory, preallocated
512 MiB filesystem image and two persistent systemd mount units. The image is
root-owned under `/var/lib/edgecitadel-trace`, including its shared filesystem
infrastructure. Only the private service-owned `trace/` directory is bind-mounted
into the account's state directory. The image size is not the trace limit: the
filesystem's UID quota independently enforces 256 MiB. The service UID cannot
modify the backing image, mount units or quota settings.

## Prepare a readable runtime

Use matching release assets and runtime dependencies installed outside `/root`.
For example, as administrator, with a verified wheel at an absolute local path:

```sh
/usr/bin/python3 -m venv /opt/edgecitadel/release
/opt/edgecitadel/release/bin/python -m pip install /absolute/path/edgecitadel.whl
/opt/edgecitadel/release/bin/python -m pip install -e /opt/edgecitadel/release/share/edgecitadel/agent-runtime
```

Keep this installation root-owned and readable/executable by the service account.
The provisioning probe executes its interpreter and imports the matching runtime
as the new UID. A root-only virtual environment or missing runtime dependencies
cannot qualify the account. Python 3.12+, systemd, `useradd`, `loginctl`,
`systemd-escape`, and ext4 tools are required. Native admission is qualified on
x86-64 jim-eq; the implemented aarch64 ABI still needs native qualification.

## Provision a new account and volume

Run the repository helper as administrator:

```sh
/opt/edgecitadel/release/bin/python /absolute/checkout/deploy/trace-storage/provision.py --name agent
```

This creates:

- Account `edgecitadel-agent`, a system UID with a private home and no login shell.
- State `/var/lib/edgecitadel-agent/state`, with `agentd/trace` on its quota mount.
- Root-owned image `/var/lib/edgecitadel-trace/agent.ext4` and its private base mount.
- System mount units enabled for `local-fs.target`; the bind unit requires and
  follows the volume unit.
- A lingering user manager for the service account. No daemon is started.

The helper validates native enforcement using the actual UID with no_new_privs,
no supplementary groups and no capabilities. It prints only paths, UID/GID and
quota measurements. It refuses existing accounts, images, target paths or units;
it does not overwrite or repair them. Interrupted provisioning retains resources
for administrator investigation. Do not rerun it with a different name to conceal
a failed preparation, or remove a mounted backing image.

The administrator must retain enforcement and keep the UID exclusive to this
service. The helper does not manage filesystem resizing, host backup capacity,
service credentials, package installation or migration of existing deployments.

## Move an existing service

Stop and fence the old writer before copying authoritative state. Preserve the
node configuration, connector/admin credentials, database pair and encryption key.
Relocate managed-agent records, launch files and Python environment references
explicitly; copied absolute `/root` paths will not work under the new UID. Keep
Core Docker storage and separately operated provider/broker services under their
own administrator ownership. Provider tokens needed by the Agent must be private
files readable by its UID, with their launch configuration updated accordingly.
No source epoch rotation or task retry is needed for a storage/account move.

Once the clean schema-24 pair and key belong to the service UID in the new state
directory, run the fenced migration with no_new_privs:

```sh
runuser -u edgecitadel-agent -- setpriv --no-new-privs \
  /opt/edgecitadel/release/bin/python -m edgecitadel_agentd.storage_migration \
  --state-dir /var/lib/edgecitadel-agent/state/agentd
```

See [the runtime migration contract](../../agent-runtime/README.md) for recovery
rules. Do not start a replacement until old authority is durably fenced. Do not
start an old daemon version against the completed new layout. Migration and
completion capacity reservations remain separate requirements.

Operate the CLI in the service account's user manager. Set **both** bus variables
when switching from an administrator shell, which may carry the root bus address:

```sh
service_uid=$(id -u edgecitadel-agent)
runuser -u edgecitadel-agent -- env \
  XDG_RUNTIME_DIR="/run/user/$service_uid" \
  DBUS_SESSION_BUS_ADDRESS="unix:path=/run/user/$service_uid/bus" \
  EDGECITADEL_STATE_DIR=/var/lib/edgecitadel-agent/state \
  /opt/edgecitadel/release/bin/edgecitadel service status --json
```

After reviewing the migrated state, use the same invocation with `service start`.
The generated service unit sets `NoNewPrivileges=yes`. Preserve required service
environment, including telemetry enablement, in that account's persistent unit
drop-ins. An absent bind mount leaves the ordinary state directory visible; native
startup refuses that same-filesystem fallback. Validate mount persistence and this
refusal before cutover. A remount test verifies quota persistence, not a complete
host reboot or full application recovery.
