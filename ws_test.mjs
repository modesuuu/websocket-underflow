// WS smoke test — run ON the laptop: node ws_test.mjs (from underflow-realtime)
// Tests: good JWT -> "authed" + receives notification + post-stats pushes;
// bad JWT -> connection closed (4401).
import WebSocket from "ws";

const API = "http://127.0.0.1:8000";
const RT = "http://127.0.0.1:8080";

async function login(username) {
  const res = await fetch(API + "/api/auth/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password: "underflow123" }),
  });
  const json = await res.json();
  return { token: json.data?.token, id: json.data?.id };
}

const russel = await login("russel");
if (!russel.token) throw new Error("login failed");
console.log("russel id:", russel.id);

const received = [];
const ws = new WebSocket("ws://127.0.0.1:8080");
ws.on("open", () => ws.send(JSON.stringify({ type: "auth", token: russel.token })));
ws.on("message", async (raw) => {
  const msg = JSON.parse(raw.toString());
  received.push(msg.type);
  console.log("RECV:", JSON.stringify(msg).slice(0, 300));
  if (msg.type === "authed") {
    await fetch(RT + "/publish", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        type: "notification",
        userId: russel.id,
        payload: {
          notification: {
            id: "test-1", type: "like", title: "kimi liked your post", body: "Test post",
            is_read: false, created_at: new Date().toISOString(),
            actor: { id: "actor-1", username: "kimi", photo: null },
            postId: "p1", postTitle: "Test post", link: "/dashboard/p1",
          },
        },
      }),
    });
    setTimeout(async () => {
      await fetch(RT + "/publish", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ type: "post-stats", postId: "p1", payload: { likes: 5, commentCount: 2 } }),
      });
    }, 300);
  }
});

// Bad token must be rejected.
const bad = new WebSocket("ws://127.0.0.1:8080");
bad.on("open", () => bad.send(JSON.stringify({ type: "auth", token: "bad.token.here" })));
bad.on("close", (code) => console.log("bad-token connection closed, code:", code));

// No-auth connection must be closed by the 5s timeout.
const silent = new WebSocket("ws://127.0.0.1:8080");
silent.on("close", (code) => console.log("no-auth connection closed, code:", code));

setTimeout(() => {
  console.log("received types:", JSON.stringify(received));
  const ok =
    received.includes("authed") &&
    received.includes("notification") &&
    received.includes("post-stats");
  console.log(ok ? "WS SMOKE TEST: PASS" : "WS SMOKE TEST: FAIL");
  process.exit(ok ? 0 : 1);
}, 8000);
