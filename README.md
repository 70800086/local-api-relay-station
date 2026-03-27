# local-api-relay-station

A lightweight local API relay / reverse proxy for OpenAI-compatible upstreams.

## Features
- local key -> app_id / upstream mapping
- transparent-ish request/response forwarding
- SQLite usage logging
- `/_relay/stats` admin endpoint
- `/_relay/idle` admin endpoint
- minimal idle detection rule

## Files
- `local_api_relay.py` - main service
- `local_api_relay.example.json` - example config
- `test_local_api_relay.py` - black-box tests
- `docs/` - design and plan notes

## Run
```bash
python3 local_api_relay.py --config local_api_relay.example.json
```
