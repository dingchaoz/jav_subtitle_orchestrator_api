# Cloudflare Tunnel Setup

Expose the Mac orchestrator dashboard and Swagger UI through Cloudflare Tunnel at:

```text
https://orchestrator.javsubtitle.com
```

This is for human operator access only. Keep SMB, `/Users/ytt/MissAVJobs`, and the
Windows `M:\` share LAN-only.

## Target Architecture

```text
Browser
  -> Cloudflare Access email allowlist
  -> Cloudflare Tunnel: orchestrator.javsubtitle.com
  -> Mac localhost: http://127.0.0.1:8010
  -> python -m orchestrator api
```

The Windows worker should continue to use the LAN API URL because it also needs SMB:

```text
MAC_API_BASE_URL=http://<mac-lan-ip>:8010
```

## Cloudflare Access First

Create the Access application before routing DNS to the tunnel.

1. Open Cloudflare Zero Trust.
2. Go to **Access** -> **Applications**.
3. Create a **Self-hosted** application.
4. Use:

```text
Application name: JAV Subtitle Orchestrator
Subdomain: orchestrator
Domain: javsubtitle.com
Session Duration: 24 hours
```

5. Add an allow policy:

```text
Action: Allow
Include: Emails -> <your approved email>
```

Do not create a bypass policy. Do not make this application public.

## Mac Tunnel Setup

Install `cloudflared`:

```bash
brew install cloudflared
cloudflared --version
```

Authenticate with Cloudflare:

```bash
cloudflared tunnel login
```

Create the named tunnel:

```bash
cloudflared tunnel create jav-orchestrator-mac
cloudflared tunnel list
```

Create `/Users/ytt/.cloudflared/config.yml`:

```yaml
tunnel: <tunnel-id>
credentials-file: /Users/ytt/.cloudflared/<tunnel-id>.json

ingress:
  - hostname: orchestrator.javsubtitle.com
    service: http://127.0.0.1:8010
  - service: http_status:404
```

Route DNS to the tunnel only after the Access app exists:

```bash
cloudflared tunnel route dns jav-orchestrator-mac orchestrator.javsubtitle.com
```

Run once in the foreground to verify:

```bash
cloudflared tunnel run jav-orchestrator-mac
```

Then install it as the user login service:

```bash
cloudflared service install
launchctl print gui/$(id -u)/com.cloudflare.cloudflared
```

On this Mac, Homebrew `cloudflared service install` may generate a LaunchAgent with only
`/opt/homebrew/bin/cloudflared` as the command. If the service exits with:

```text
use `cloudflared tunnel run` to start tunnel <tunnel-id>
```

unload it:

```bash
launchctl bootout gui/$(id -u) /Users/ytt/Library/LaunchAgents/com.cloudflare.cloudflared.plist
```

Then update `ProgramArguments` in
`/Users/ytt/Library/LaunchAgents/com.cloudflare.cloudflared.plist` to:

```xml
<array>
  <string>/opt/homebrew/bin/cloudflared</string>
  <string>tunnel</string>
  <string>--config</string>
  <string>/Users/ytt/.cloudflared/config.yml</string>
  <string>--no-autoupdate</string>
  <string>run</string>
  <string>jav-orchestrator-mac</string>
</array>
```

Reload and verify:

```bash
launchctl bootstrap gui/$(id -u) /Users/ytt/Library/LaunchAgents/com.cloudflare.cloudflared.plist
cloudflared tunnel info jav-orchestrator-mac
```

## Orchestrator API Service

The existing Mac API LaunchAgent remains the owner of the app process:

```bash
launchctl print gui/$(id -u)/com.javsubtitle.orchestrator-api
curl -I http://127.0.0.1:8010/dashboard
curl -I http://127.0.0.1:8010/docs
```

Keep the API bound to `0.0.0.0:8010` while the Windows worker uses the LAN API.
Cloudflare should target `127.0.0.1:8010`, not the LAN IP.

## Verification

From the Mac:

```bash
curl -I http://127.0.0.1:8010/dashboard
curl -I http://127.0.0.1:8010/docs
curl -I https://orchestrator.javsubtitle.com/dashboard
```

From a non-LAN machine:

1. Open `https://orchestrator.javsubtitle.com/dashboard`.
2. Confirm Cloudflare Access prompts for login.
3. Confirm the approved email can reach the dashboard.
4. Confirm `/docs` and `/openapi.json` load after login.
5. Confirm an unapproved email cannot reach the app.

## Security Rules

- Do not expose SMB over Cloudflare.
- Do not expose `/Users/ytt/MissAVJobs`.
- Do not put Cloudflare Access service tokens or admin tokens in browser code.
- Do not route `*.javsubtitle.com` to this tunnel; route only `orchestrator.javsubtitle.com`.
- Leave the Windows worker LAN-only for v1.

