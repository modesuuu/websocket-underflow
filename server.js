/**
 * underflow-realtime — WebSocket push server for Underflow.
 * ─────────────────────────────────────────────────────────────────────────────
 * One process, two jobs, same port (8080, localhost only):
 *
 *  1. WebSocket server — browsers connect to ws://127.0.0.1:8080 and
 *     authenticate with their JWT as the FIRST message:
 *       { "type": "auth", "token": "<JWT from /api/auth/login>" }
 *     The JWT is verified (HS256, same algorithm + secret as the PHP
 *     backend utils/JWT.php) against JWT_SECRET read from
 *     ../bk-underflow/.env at startup. Missing/invalid/expired token
 *     -> connection closed (4401); no auth within 5s -> closed (4408).
 *     After a successful auth the server replies:
 *       { "type": "authed", "userId": "<uuid>" }
 *     and routes pushes to that user's sockets. Connections are tracked
 *     as userId -> Set of sockets (one user may have several tabs).
 *
 *  2. HTTP POST /publish — how the PHP API pushes events into the
 *     sockets. Localhost only (any non-127.0.0.1 request -> 403).
 *     Body (JSON):
 *       { "type": "notification", "userId": "<recipient>",
 *         "payload": { "notification": { ...mapped row, same shape as
 *           GET /api/notifications... } } }
 *       { "type": "notification-removed", "userId": "<recipient>",
 *         "payload": { "notificationId": "<uuid>" } }
 *       { "type": "post-stats", "postId": "<uuid>",
 *         "payload": { "likes": <n>, "commentCount": <n> } }
 *     "notification" / "notification-removed" go only to that user's
 *     sockets; "post-stats" is broadcast to ALL connected sockets.
 *     Responds 200 { ok: true } even when the user has no sockets.
 *
 *  Pushed to browsers (JSON):
 *    { "type": "notification", "payload": { "notification": {...} } }
 *    { "type": "notification-removed", "payload": { "notificationId": "..." } }
 *    { "type": "post-stats", "payload": { "postId": "...", "likes": n, "commentCount": n } }
 *
 *  The frontend ALSO polls every 5s (LivePoller) as a fallback, so if
 *  this server is down nothing is lost — delivery just gets slower.
 *
 *  Start:  nohup node server.js > realtime.log 2>&1 &
 *  Stop:   find the PID (netstat -ano | findstr :8080) then taskkill /F /PID <pid>
 *  Logs:   connections + publishes to stdout (never tokens or secrets).
 * ─────────────────────────────────────────────────────────────────────────────
 */
"use strict";

const http = require("node:http");
const path = require("node:path");
const fs = require("node:fs");
const crypto = require("node:crypto");
const WebSocket = require("ws");

const PORT = Number(process.env.REALTIME_PORT || 8080);
const BACKEND_ENV_PATH = path.join(__dirname, "..", "bk-underflow", ".env");
const AUTH_TIMEOUT_MS = 5000;
const HEARTBEAT_MS = 30000;

// ── JWT_SECRET: read from the backend .env at startup (never logged) ──
function readJwtSecret() {
  let raw;
  try {
    raw = fs.readFileSync(BACKEND_ENV_PATH, "utf8");
  } catch {
    console.error("[realtime] FATAL: cannot read backend .env at " + BACKEND_ENV_PATH);
    process.exit(1);
  }
  for (const line of raw.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const eq = trimmed.indexOf("=");
    if (eq === -1) continue;
    if (trimmed.slice(0, eq).trim() !== "JWT_SECRET") continue;
    let value = trimmed.slice(eq + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    if (value) return value;
  }
  console.error("[realtime] FATAL: JWT_SECRET not found in backend .env");
  process.exit(1);
}
const JWT_SECRET = readJwtSecret();

// ── JWT HS256 verification (mirrors bk-underflow utils/JWT.php) ──
function base64urlDecode(str) {
  str = str.replace(/-/g, "+").replace(/_/g, "/");
  while (str.length % 4) str += "=";
  return Buffer.from(str, "base64");
}

function verifyJwt(token) {
  if (typeof token !== "string" || token === "") throw new Error("token missing");
  const parts = token.split(".");
  if (parts.length !== 3) throw new Error("bad format");
  const [headerB64, payloadB64, sigB64] = parts;
  let header;
  try {
    header = JSON.parse(base64urlDecode(headerB64).toString("utf8"));
  } catch {
    throw new Error("bad header");
  }
  if (!header || header.alg !== "HS256") throw new Error("bad alg");
  const expected = crypto
    .createHmac("sha256", JWT_SECRET)
    .update(headerB64 + "." + payloadB64)
    .digest();
  const actual = base64urlDecode(sigB64);
  if (actual.length !== expected.length || !crypto.timingSafeEqual(expected, actual)) {
    throw new Error("bad signature");
  }
  let payload;
  try {
    payload = JSON.parse(base64urlDecode(payloadB64).toString("utf8"));
  } catch {
    throw new Error("bad payload");
  }
  if (typeof payload.exp === "number" && Math.floor(Date.now() / 1000) >= payload.exp) {
    throw new Error("expired");
  }
  if (!payload.id || typeof payload.id !== "string") throw new Error("no user id");
  return payload;
}

// ── Connection registry: userId -> Set of sockets ──
/** @type {Map<string, Set<import("ws")>>} */
const userSockets = new Map();

function addSocket(userId, ws) {
  let set = userSockets.get(userId);
  if (!set) {
    set = new Set();
    userSockets.set(userId, set);
  }
  set.add(ws);
}

function removeSocket(userId, ws) {
  const set = userSockets.get(userId);
  if (!set) return;
  set.delete(ws);
  if (set.size === 0) userSockets.delete(userId);
}

function totalSockets() {
  let n = 0;
  for (const set of userSockets.values()) n += set.size;
  return n;
}

function sendToUser(userId, message) {
  const set = userSockets.get(userId);
  if (!set) return 0;
  const data = JSON.stringify(message);
  let n = 0;
  for (const ws of set) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(data);
      n++;
    }
  }
  return n;
}

function broadcast(message) {
  const data = JSON.stringify(message);
  let n = 0;
  for (const set of userSockets.values()) {
    for (const ws of set) {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(data);
        n++;
      }
    }
  }
  return n;
}

// ── HTTP server: GET /health + POST /publish (localhost only) ──
function isLoopback(req) {
  const addr = req.socket.remoteAddress || "";
  return addr === "127.0.0.1" || addr === "::1" || addr === "::ffff:127.0.0.1";
}

const server = http.createServer((req, res) => {
  if (req.method === "GET" && req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, connections: totalSockets() }));
    return;
  }

  if (req.method !== "POST" || req.url !== "/publish") {
    res.writeHead(404, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: false, error: "not found" }));
    return;
  }

  if (!isLoopback(req)) {
    res.writeHead(403, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: false, error: "forbidden" }));
    return;
  }

  let body = "";
  req.on("data", (chunk) => {
    body += chunk;
    if (body.length > 65536) req.destroy();
  });
  req.on("end", () => {
    let event;
    try {
      event = JSON.parse(body);
    } catch {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: false, error: "bad json" }));
      return;
    }

    const type = event && event.type;
    const payload = (event && event.payload) || {};
    if (type === "notification") {
      const userId = event.userId || payload.userId;
      const n = userId
        ? sendToUser(userId, { type: "notification", payload: { notification: payload.notification } })
        : 0;
      console.log(`[publish] notification -> user ${userId} (${n} socket${n === 1 ? "" : "s"})`);
    } else if (type === "notification-removed") {
      const userId = event.userId || payload.userId;
      const n = userId
        ? sendToUser(userId, {
            type: "notification-removed",
            payload: { notificationId: payload.notificationId },
          })
        : 0;
      console.log(
        `[publish] notification-removed ${payload.notificationId} -> user ${userId} (${n} socket${n === 1 ? "" : "s"})`
      );
    } else if (type === "post-stats") {
      const postId = event.postId || payload.postId;
      const n = broadcast({
        type: "post-stats",
        payload: { postId, likes: payload.likes, commentCount: payload.commentCount },
      });
      console.log(
        `[publish] post-stats ${postId} likes=${payload.likes} comments=${payload.commentCount} (${n} socket${n === 1 ? "" : "s"})`
      );
    } else {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: false, error: "unknown type" }));
      return;
    }

    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true }));
  });
});

// ── WebSocket server (shares the HTTP server / port) ──
const wss = new WebSocket.Server({ server });

wss.on("connection", (ws) => {
  let userId = null;
  let authed = false;
  ws.isAlive = true;

  const authTimer = setTimeout(() => {
    if (!authed) {
      try {
        ws.close(4408, "auth timeout");
      } catch {}
    }
  }, AUTH_TIMEOUT_MS);

  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString("utf8"));
    } catch {
      return; // ignore non-JSON
    }

    if (!authed) {
      if (msg && msg.type === "auth" && typeof msg.token === "string") {
        let payload;
        try {
          payload = verifyJwt(msg.token);
        } catch (err) {
          console.log("[ws] auth rejected: " + err.message);
          try {
            ws.close(4401, "unauthorized");
          } catch {}
          return;
        }
        authed = true;
        userId = payload.id;
        clearTimeout(authTimer);
        addSocket(userId, ws);
        console.log(`[ws] user ${userId} connected (${userSockets.get(userId).size} socket(s))`);
        ws.send(JSON.stringify({ type: "authed", userId }));
      }
      // Any other pre-auth message is ignored; the auth timeout closes it.
      return;
    }

    // Authed: only ping is handled.
    if (msg && msg.type === "ping") {
      ws.send(JSON.stringify({ type: "pong" }));
    }
  });

  ws.on("close", () => {
    clearTimeout(authTimer);
    if (userId) {
      removeSocket(userId, ws);
      console.log(`[ws] user ${userId} disconnected`);
    }
  });
  ws.on("error", () => {
    // close follows; handled above.
  });
});

// Heartbeat: drop dead sockets.
const heartbeat = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      ws.terminate();
      return;
    }
    ws.isAlive = false;
    try {
      ws.ping();
    } catch {}
  });
}, HEARTBEAT_MS);

server.listen(PORT, "127.0.0.1", () => {
  console.log(`[realtime] listening on 127.0.0.1:${PORT} (ws + POST /publish)`);
});

function shutdown() {
  clearInterval(heartbeat);
  server.close(() => process.exit(0));
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
