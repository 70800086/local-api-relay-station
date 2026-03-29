# Local API Relay Hot Reload Design

Date: 2026-03-29
Status: Approved for planning
Target: `local_api_relay.py`

## Goal

Make the local API relay automatically reload `local_api_relay.json` after file changes without requiring a manual service restart.

The design must satisfy these constraints:

- Reload is triggered automatically from config file changes.
- Invalid config changes must not take the running relay down.
- Runtime-safe config changes should switch over without interrupting in-flight requests.
- Full config changes, including `listen_host`, `listen_port`, and `database_path`, should also be supported.
- A brief interruption during listener rebinding is acceptable when host or port changes.

## Current State

The relay currently loads config exactly once during process startup:

- `main()` parses `--config`
- `RelayConfig.load()` reads and validates the JSON file
- `RelayHTTPServer` is created with a fixed `relay_config` and `usage_store`
- `serve_forever()` runs until the process exits

Request handling reads configuration directly from `self.server.relay_config`, so the process has no way to adopt new config without restart.

## Recommended Approach

Implement in-process automatic hot reload with a background config watcher and switchable runtime objects.

This approach is preferred over service restart or self-`exec` because it preserves the current deployment shape, avoids unnecessary restarts for routine upstream changes, and allows invalid config updates to fail safely while the old config keeps serving traffic.

## Architecture

### 1. Relay Runtime

Introduce a runtime object that represents one coherent serving state:

- `RelayConfig`
- `UsageStore`
- reload metadata for that runtime version
- in-flight request tracking for safe draining

Each incoming request captures a runtime snapshot at request start and uses that snapshot for the entire request lifecycle. This prevents a request from seeing mixed config during a concurrent reload.

### 2. Runtime Manager

Introduce a manager responsible for:

- storing the active runtime
- returning per-request runtime snapshots
- replacing the active runtime after a successful reload
- tracking retired runtimes that are draining existing requests
- cleaning up drained runtimes after their in-flight requests reach zero

The existing server/request handler flow should depend on the manager rather than directly on a single mutable `relay_config` reference.

### 3. Config Watcher

Add a background watcher thread that monitors the configured JSON file.

Watcher behavior:

- poll every 1 second
- track both `mtime` and content hash
- only attempt reload after the same changed content is observed twice consecutively

This double-observation rule reduces false reload attempts while editors are still writing the file.

### 4. Reload Controller

Add reload logic that:

1. reads the config file
2. fully validates it using the existing `RelayConfig.load()` path
3. determines whether the change is runtime-only or requires infrastructure replacement
4. prepares new runtime resources before making them active
5. atomically swaps the active runtime on success
6. records reload success or failure metadata

## Reload Categories

### Runtime-Only Changes

These can switch without rebinding the listener:

- `local.clients`
- `upstreams`
- `order`
- `idle_window_seconds`
- `request_timeout_seconds`

For these changes:

- construct a new `RelayConfig`
- reuse the existing listener
- reuse the existing `UsageStore` when `database_path` is unchanged
- swap the active runtime atomically

### Database Changes

When `database_path` changes:

- create and validate a new `UsageStore`
- keep the old runtime active until the new store is ready
- swap active runtime only after the new store is usable
- let the old runtime drain and then close its database connection

This keeps old in-flight requests writing to the store they started with while new requests use the new database.

### Listener Changes

When `listen_host` or `listen_port` changes:

- create a new listening server bound to the new address
- prepare the new runtime first
- start the new listener
- atomically mark the new runtime/server as active
- stop accepting new requests on the old listener
- let old in-flight requests drain
- close the old listener once draining completes

Because a brief interruption is acceptable, the implementation may use a short cutover window if needed to avoid excessive socket complexity.

If the new listener cannot bind, the reload fails and the old listener stays active.

## Request Handling Semantics

Request behavior during reload:

- each request acquires a runtime snapshot before admin or proxy handling
- all config reads for that request use that snapshot
- request start/finish accounting is attached to the runtime snapshot, not a global mutable object

This preserves consistency for:

- local key validation
- upstream order selection
- timeout behavior
- admin responses such as `/_relay/health` and `/_relay/idle`

Long-lived upgrade responses continue to use the runtime they started with and are allowed to finish naturally.

## Failure Handling

Failure rule: reload errors must never replace a healthy active runtime.

Failure cases:

- invalid JSON
- schema validation failure
- unknown upstream in `order`
- invalid primitive values
- database open/init failure
- listener bind failure
- any unexpected exception during preparation

Behavior on failure:

- keep the current active runtime serving traffic
- record the error in reload status
- do not partially swap resources
- close any newly created resources that were prepared before the failure

## Health and Observability

Extend `/_relay/health` with reload status fields:

- `config_path`
- `config_version`
- `reload_enabled`
- `reload_poll_interval_seconds`
- `last_reload_at`
- `last_reload_status`
- `last_reload_error`
- `last_successful_reload_at`
- `draining_runtimes`

Definitions:

- `config_version` is a monotonic version or hash tied to the active config content
- `last_reload_status` is one of `never`, `success`, `error`
- `draining_runtimes` reports how many retired runtimes still have active requests

This endpoint should allow operators to tell whether a file change has been applied or rejected.

## Concurrency Model

Use a small synchronization boundary around active runtime replacement.

Requirements:

- request snapshot acquisition must be thread-safe
- runtime swaps must be atomic
- draining cleanup must not race request completion
- background watcher must not block normal request handling for long periods

Use a lock around runtime-manager state transitions. Keep lock-held sections short by preparing config, stores, and replacement listeners before the swap.

## Testing Strategy

Add automated tests covering:

### Runtime-Only Reload

- change `order` and verify new requests follow the new upstream order
- change local clients and verify new local keys work without restart
- change timeout values and verify subsequent requests use new settings
- ensure in-flight requests continue using the old runtime snapshot

### Failure and Rollback

- malformed JSON does not break serving
- schema-invalid config does not break serving
- listener bind failure does not break serving
- database open failure does not break serving
- health endpoint reports the failure correctly

### Infrastructure Replacement

- `database_path` change moves new traffic to a new store while old requests finish on the old store
- `listen_port` change brings up the new port and retires the old listener
- health endpoint reflects the active config version after cutover

### Admin Surface

- `/_relay/health` includes reload metadata
- reload metadata updates after success and failure
- draining runtime count drops back to zero after old requests finish

## Rollout Notes

No config schema change is required for the initial implementation. Hot reload should be enabled by default for the configured `--config` file path.

The existing `systemd --user` unit can remain unchanged. Manual service restart remains a valid fallback for recovery, but normal config edits should no longer require it.

## Non-Goals

- no external dependency on inotify or watchdog libraries
- no change to upstream selection policy beyond using the newly active runtime
- no attempt to preserve active TCP connections across listener rebind beyond normal request draining
- no cross-process shared reload coordination

## Open Decisions Resolved

- Trigger mode: automatic file-based reload
- Invalid config handling: reject new config, keep old runtime
- Reload scope: include runtime config and infrastructure config
- Availability target: no interruption for runtime-only changes, brief interruption acceptable for listener changes
