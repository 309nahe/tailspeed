# tailspeed

A tiny self-hosted speed test for a [Tailscale](https://tailscale.com) node. It measures latency, jitter,
download and upload between your browser and the server, and shows **how your device reaches it**:
a direct WireGuard path or a DERP relay (which caps speed and adds latency).

- Live gauge, ping → download → upload, 5/8/15 s runs, warm-up excluded from the result
- Device card from `tailscale status`: hostname, OS, user, direct or relayed, relay region
- **Check path** runs `tailscale ping` to your device, shows each hop, and nudges Tailscale to try a direct route
- History of the last 100 tests from all devices, kept in memory only
- One small Rust binary, one embedded HTML page, no external assets

## Requirements

Runs on a Tailscale node behind `tailscale serve`, which supplies the client address (`X-Forwarded-For`) and the
`Tailscale-User-*` identity headers. The service user must be able to run `tailscale status` and `tailscale ping`
(any user can on a default install).

## Run

```sh
cargo build --release
BIND=127.0.0.1:8453 ./target/release/speed
sudo tailscale serve --bg --set-path=/speed http://127.0.0.1:8453
```

## Deploy

`deploy/speed.service` is a hardened systemd unit. Open it as `/speed/` with the trailing slash.

## API

| Method | Path | Notes |
|---|---|---|
| `GET` | `/api/ping` | empty response, for latency |
| `GET` | `/api/down?bytes=N` | streams N incompressible bytes (max 512 MiB) |
| `POST` | `/api/up` | discards the body, returns `{"bytes"}` |
| `GET` | `/api/whoami` | client device and path from `tailscale status` |
| `POST` | `/api/path` | `tailscale ping` to the client |
| `GET`/`POST` | `/api/results` | test history |

Note: the numbers measure the path between the browser and the server, not the device's internet connection.
