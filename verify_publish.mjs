// Backend publish verification — run ON the laptop: node verify_publish.mjs
// (from underflow-realtime). Drives the PHP API + a WS client as russel:
// kimi likes -> WS gets notification + post-stats; unlike -> notification-removed;
// comment -> notification + post-stats; delete comment -> notification-removed.
// Cleans up the test post afterwards.
import WebSocket from "ws";

const API = "http://127.0.0.1:8000";
const STAMP = Date.now().toString(36);

let passed = 0, failed = 0;
function check(name, ok, extra = "") {
  if (ok) { passed++; console.log("  PASS " + name); }
  else { failed++; console.log("  FAIL " + name + (extra ? " — " + extra : "")); }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function login(username) {
  const res = await fetch(API + "/api/auth/login", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password: "underflow123" }),
  });
  const json = await res.json();
  return { token: json.data?.token, id: json.data?.id };
}

const russel = await login("russel");
const kimi = await login("kimi");

// Russel creates a post.
const postRes = await fetch(API + "/api/posts", {
  method: "POST",
  headers: { "Content-Type": "application/json", Authorization: "Bearer " + russel.token },
  body: JSON.stringify({ title: "Publish probe " + STAMP, description: "Publish probe " + STAMP, portfolio_type: "link", media_urls: [] }),
});
const postJson = await postRes.json();
const postId = postJson.data?.id;
if (!postId) throw new Error("post create failed: " + JSON.stringify(postJson).slice(0, 200));
console.log("  post:", postId);

// WS client as russel.
const inbox = [];
const ws = new WebSocket("ws://127.0.0.1:8080");
await new Promise((resolve, reject) => {
  ws.on("open", () => ws.send(JSON.stringify({ type: "auth", token: russel.token })));
  ws.on("message", (raw) => {
    const msg = JSON.parse(raw.toString());
    inbox.push(msg);
    if (msg.type === "authed") resolve();
  });
  ws.on("error", reject);
  setTimeout(() => reject(new Error("authed timeout")), 5000);
});
check("WS authed as russel", true);

// Wait for messages of given types (since a mark), up to timeoutMs.
async function waitForTypes(types, since, timeoutMs = 5000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const got = inbox.slice(since).map((m) => m.type);
    if (types.every((t) => got.includes(t))) return inbox.slice(since);
    await sleep(100);
  }
  return inbox.slice(since);
}

let mark = inbox.length;

// 1. Kimi likes.
await fetch(API + "/api/posts/" + postId + "/like", { method: "POST", headers: { Authorization: "Bearer " + kimi.token } });
let msgs = await waitForTypes(["notification", "post-stats"], mark);
const likeNotif = msgs.find((m) => m.type === "notification");
check("like -> WS notification", !!likeNotif && likeNotif.payload?.notification?.type === "like" && likeNotif.payload?.notification?.actor?.username === "kimi", JSON.stringify(likeNotif?.payload?.notification?.actor));
const likeStats = msgs.find((m) => m.type === "post-stats");
check("like -> WS post-stats likes=1", !!likeStats && likeStats.payload?.likes === 1 && likeStats.payload?.postId === postId, JSON.stringify(likeStats?.payload));
mark = inbox.length;

// 2. Kimi unlikes.
await fetch(API + "/api/posts/" + postId + "/unlike", { method: "DELETE", headers: { Authorization: "Bearer " + kimi.token } });
msgs = await waitForTypes(["notification-removed", "post-stats"], mark);
const unlikeRemoved = msgs.find((m) => m.type === "notification-removed");
check("unlike -> WS notification-removed", !!unlikeRemoved && unlikeRemoved.payload?.notificationId === likeNotif?.payload?.notification?.id, JSON.stringify(unlikeRemoved?.payload));
const unlikeStats = msgs.find((m) => m.type === "post-stats");
check("unlike -> WS post-stats likes=0", !!unlikeStats && unlikeStats.payload?.likes === 0, JSON.stringify(unlikeStats?.payload));
mark = inbox.length;

// 3. Kimi comments.
const commentRes = await fetch(API + "/api/posts/" + postId + "/comments", {
  method: "POST",
  headers: { "Content-Type": "application/json", Authorization: "Bearer " + kimi.token },
  body: JSON.stringify({ content: "Publish probe comment " + STAMP }),
});
const commentJson = await commentRes.json();
const commentId = commentJson.data?.id;
check("comment API 200", commentRes.status === 200 && !!commentId);
msgs = await waitForTypes(["notification", "post-stats"], mark);
const commentNotif = msgs.find((m) => m.type === "notification");
check("comment -> WS notification", !!commentNotif && commentNotif.payload?.notification?.type === "comment", JSON.stringify(commentNotif?.payload?.notification?.type));
const commentStats = msgs.find((m) => m.type === "post-stats");
check("comment -> WS post-stats commentCount=1", !!commentStats && commentStats.payload?.commentCount === 1, JSON.stringify(commentStats?.payload));
mark = inbox.length;

// 4. Delete the comment.
await fetch(API + "/api/posts/" + postId + "/comments/" + commentId, { method: "DELETE", headers: { Authorization: "Bearer " + kimi.token } });
msgs = await waitForTypes(["notification-removed", "post-stats"], mark);
const delRemoved = msgs.find((m) => m.type === "notification-removed");
check("delete comment -> WS notification-removed", !!delRemoved && delRemoved.payload?.notificationId === commentNotif?.payload?.notification?.id, JSON.stringify(delRemoved?.payload));
const delStats = msgs.find((m) => m.type === "post-stats");
check("delete comment -> WS post-stats commentCount=0", !!delStats && delStats.payload?.commentCount === 0, JSON.stringify(delStats?.payload));

// 5. Notifications API list should be empty now (like removed + comment removed).
const listRes = await fetch(API + "/api/notifications", { headers: { Authorization: "Bearer " + russel.token } });
const listJson = await listRes.json();
check("notifications list empty after cleanup", (listJson.data || []).length === 0, JSON.stringify((listJson.data || []).length));

// Cleanup: delete the post.
await fetch(API + "/api/posts/" + postId, { method: "DELETE", headers: { Authorization: "Bearer " + russel.token } });

ws.close();
console.log(`\n${passed}/${passed + failed} PASS`);
process.exit(failed ? 1 : 0);
