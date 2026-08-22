# Home Assistant adapter

`homeassistant-1` is a native EdgeCitadel worker for controlled smart-home experiments. It receives typed `command` envelopes on `agents.homeassistant-1.inbox`, calls a local Home Assistant REST API, and returns structured `result` envelopes through the shared JetStream adapter contract.

Supported operations are passed in `payload.args`:

- `get_state`: `{operation, entity_id}`
- `set_light`: `{operation, entity_id, state: "on"|"off", brightness?}`
- `wait_state`: `{operation, entity_id, state, timeout_sec?, poll_sec?}`
- `read_camera`: `{operation, entity_id, roi?: [x, y, width, height]}`
- `run_sequence`: `{operation, steps: [{operation, args}], restore?: true}`

All entities are configured by allowlists in `HA_ALLOWED_*`. `run_sequence` is limited to `HA_MAX_SEQUENCE_STEPS` and restores the original state of every touched light by default. Camera responses are reduced to luminance statistics and raw JPEG bytes are not returned or retained.

## Host configuration

The systemd unit reads `/etc/edgecitadel/homeassistant.env`:

```dotenv
HA_BASE_URL=http://localhost:8123
HA_TOKEN_FILE=/etc/edgecitadel/homeassistant/token
HA_ALLOWED_LIGHTS=light.example
HA_ALLOWED_ENTITIES=sensor.example
HA_ALLOWED_CAMERAS=camera.example
HA_CAMERA_ROIS=camera.example:0:0:640:480
```

The token file must be readable by the `edgecitadel` service user and must not be committed. The unit uses `ProtectHome=true`, so the token must not remain under `/root`.

## Example command payload

```json
{
  "args": {
    "operation": "run_sequence",
    "steps": [
      {"operation": "set_light", "args": {"entity_id": "light.example", "state": "on"}},
      {"operation": "wait_state", "args": {"entity_id": "light.example", "state": "on"}},
      {"operation": "read_camera", "args": {"entity_id": "camera.example"}}
    ]
  }
}
```
