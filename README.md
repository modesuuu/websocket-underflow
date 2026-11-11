# underflow-realtime

Self-hosted WebSocket push server for Underflow (free, no third-party service).
It makes notifications + like/comment counts arrive **instantly** — the PHP
backend has no realtime server, so this small Node process is the push path.
The frontend also polls every 5s as a fallback, so if this server is down
nothing is lost (delivery just gets slower).

## What it does

- **WebSocket server** on port **8080** (localhost only). A browser connects to
  `ws://127.0.0.1:8080` and authenticates with its JWT as the **first message**:
  ```json
  { "type": "auth", "token": "<JWT from POST /api/auth/login>" }
  ```
  The JWT is verified (HS256) against `JWT_SECRET` read from
  `../bk-underflow/.env` at startup — never hardcoded, never logged.
  Missing/invalid/expired token → connection closed (4401); no auth within
  5s → closed (4408). On success the server replies
  `{ "type": "authed", "userId": "<uuid>" }`.
- **HTTP `POST /publish`** on the same port — how the PHP API pushes events.
  Localhost only (any other source → 403). Body:
  ```json
  { "type": "notification", "userId": "<recipient>",
    "payload": { "notification": { "...same shape as GET /api/notifications..." } } }
  { "type": "notification-removed", "userId": "<recipient>",
    "payload": { "notificationId": "<uuid>" } }
  { "type": "post-stats", "postId": "<uuid>",
    "payload": { "likes": 3, "commentCount": 2 } }
  { "type": "comment-stats", "commentId": "<uuid>",
    "postId": "<uuid>", "payload": { "likes": 1 } }
  ```
  `notification` / `notification-removed` go only to that user's sockets;
  `post-stats` and `comment-stats` are broadcast to all connected sockets.
  Always responds
  `200 { "ok": true }` (even when the user has no sockets open).
- `GET /health` → `{ "ok": true, "connections": <n> }`.

## Pushed to browsers

```json
{ "type": "notification", "payload": { "notification": { "..." } } }
{ "type": "notification-removed", "payload": { "notificationId": "<uuid>" } }
{ "type": "post-stats", "payload": { "postId": "<uuid>", "likes": 3, "commentCount": 2 } }
{ "type": "comment-stats", "payload": { "commentId": "<uuid>", "postId": "<uuid>", "likes": 1 } }
```

## Run it

```bash
cd C:\Users\Raka Alfarezi\Documents\dev_learn\underflow-realtime
npm install          # once (installs the `ws` package)
nohup node server.js > realtime.log 2>&1 &
```

Check it answers:

```bash
curl http://127.0.0.1:8080/health
# {"ok":true,"connections":0}
curl -X POST http://127.0.0.1:8080/publish \
  -H "Content-Type: application/json" \
  -d '{"type":"post-stats","postId":"x","payload":{"likes":1,"commentCount":0}}'
# {"ok":true}
```

Logs go to `realtime.log` (connections + publishes; no tokens or secrets).

## Stop it

```bash
netstat -ano | findstr :8080   # find the PID of node server.js
taskkill /F /PID <pid>
```

## Notes

- The server binds `127.0.0.1` only — it is not reachable from other machines.
  The frontend connects to `ws://127.0.0.1:8080` via the
  `NEXT_PUBLIC_REALTIME_URL` env var (see the frontend `.env.local`).
- If `JWT_SECRET` is missing from `../bk-underflow/.env`, the server refuses
  to start (it exits with an error — that is intentional).
- Port can be changed with the `REALTIME_PORT` env var.
